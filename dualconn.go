package dualconn

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/segmentio/ksuid"
	"go.uber.org/multierr"
)

type Manager struct {
	// 一个管理器内部，暂时只用一把锁
	*sync.Mutex `json:"-"`

	Timeout time.Duration `json:"timeout"`
	Dialer  Dialer        `json:"-"`
	Targets []*Target     `json:"targets"`

	// ProtagonistHalo 开启主角光环，一旦主角复活，其它副本自动退位（Close)
	ProtagonistHalo bool `json:"protagonistHalo"`
	stop            chan struct{}
}

func NewManager(addresses []string, dailTimeout time.Duration) *Manager {
	m := &Manager{
		Mutex:   &sync.Mutex{},
		Timeout: dailTimeout,
		Dialer:  &net.Dialer{Timeout: dailTimeout},
	}
	m.Targets = make([]*Target, len(addresses))
	for i, addr := range addresses {
		m.Targets[i] = &Target{
			Addr:  addr,
			Conns: make(map[string]*DualConn),
		}
	}
	go m.recycle(3 * time.Second)

	return m
}

func (d *Manager) Close() error {
	close(d.stop)

	d.Lock()
	defer d.Unlock()

	var errs error
	for _, t := range d.Targets {
		errs = multierr.Append(errs, t.Close())
	}

	return errs
}

func (d *Manager) WithProtagonistHalo() *Manager {
	d.ProtagonistHalo = true
	return d
}

func (d *Manager) MarshalJSON() ([]byte, error) {
	d.Lock()
	defer d.Unlock()

	type alias Manager
	return json.Marshal(alias(*d))
}

func (d *Manager) Enable(target string, disabled bool) bool {
	for _, t := range d.Targets {
		if t.Addr == target {
			t.SetDisabled(disabled)
			return true
		}
	}

	return false
}

func (d *Manager) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	for i, target := range d.Targets {
		if target.Disabled {
			continue
		}

		dialTime := Now()
		conn, err := d.Dialer.DialContext(ctx, network, target.Addr)
		if err != nil {
			d.Lock()
			target.LastErr = err.Error()
			target.DialTime = dialTime
			d.Unlock()
			continue
		}

		dc := &DualConn{
			ID:   ksuid.New().String(),
			conn: conn,
		}

		d.Lock()
		target.Conns[dc.ID] = dc
		target.LastErr = ""
		target.DialTime = dialTime

		if i == 0 && d.ProtagonistHalo {
			for i := 1; i < len(d.Targets); i++ {
				_ = d.Targets[i].Close()
			}
		}
		d.Unlock()

		return dc, nil
	}

	return nil, ErrNotAvailable
}

func (d *Manager) recycle(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.runRecycle()
			d.healthCheck()

		case <-d.stop:
			return
		}
	}
}

func (d *Manager) runRecycle() {
	d.Lock()
	defer d.Unlock()

	for _, target := range d.Targets {
		if target.Disabled {
			target.Close()
			target.Conns = make(map[string]*DualConn)
			continue
		}

		for _, conn := range target.Conns {
			if conn.HasError() {
				_ = conn.Close()
				delete(target.Conns, conn.ID)
			}
		}
	}
}

func (d *Manager) healthCheck() {
	d.Lock()
	defer d.Unlock()

	target := d.Targets[0]
	if target.Disabled {
		return
	}

	conn, err := d.Dialer.DialContext(context.Background(), "tcp", target.Addr)
	if err != nil {
		return
	}
	defer conn.Close()

	if d.ProtagonistHalo {
		for i := 1; i < len(d.Targets); i++ {
			_ = d.Targets[i].Close()
		}
	}
}

type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type ContextDialerFunc func(ctx context.Context, network, address string) (net.Conn, error)

func (f ContextDialerFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

type Target struct {
	Addr     string               `json:"addr"`
	Disabled bool                 `json:"disabled,omitempty"`
	LastErr  string               `json:"lastErr,omitempty"`
	DialTime *time.Time           `json:"dialTime,omitempty"`
	Conns    map[string]*DualConn `json:"conns,omitempty"`
}

func (t *Target) SetDisabled(disabled bool) {
	t.Disabled = disabled
	if disabled {
		_ = t.Close()
	}
}

func (t *Target) Close() error {
	var err error
	for _, conn := range t.Conns {
		if !conn.Closed {
			err = multierr.Append(err, conn.Close())
		}
	}

	return err
}

type DualConn struct {
	conn net.Conn

	ID string `json:"-"`

	ReadN  int `json:"readN,omitempty"`
	WriteN int `json:"writeN,omitempty"`

	ReadLast  *time.Time `json:"readLast,omitempty"`
	WriteLast *time.Time `json:"writeLast,omitempty"`
	CloseTime *time.Time `json:"closeTime,omitempty"`

	ReadErr  string `json:"readErr,omitempty"`
	WriteErr string `json:"writeErr,omitempty"`
	CloseErr string `json:"closeErr,omitempty"`

	Closed bool `json:"closed"`
}

func Now() *time.Time {
	t := time.Now()
	return &t
}

func (d *DualConn) Unwrap() net.Conn {
	return d.conn
}

func (d *DualConn) Read(b []byte) (n int, err error) {
	n, err = d.conn.Read(b)
	d.ReadLast = Now()
	d.ReadN += n
	if err != nil {
		d.ReadErr = err.Error()
	}

	return
}

func (d *DualConn) Write(b []byte) (n int, err error) {
	n, err = d.conn.Write(b)
	d.WriteLast = Now()
	d.WriteN += n
	if err != nil {
		d.WriteErr = err.Error()
	}

	return
}

func (d *DualConn) Close() (err error) {
	err = d.conn.Close()
	d.Closed = true
	d.CloseTime = Now()
	if err != nil {
		d.CloseErr = err.Error()
	}

	return
}

func (d *DualConn) LocalAddr() net.Addr                { return d.conn.LocalAddr() }
func (d *DualConn) RemoteAddr() net.Addr               { return d.conn.RemoteAddr() }
func (d *DualConn) SetDeadline(t time.Time) error      { return d.conn.SetDeadline(t) }
func (d *DualConn) SetReadDeadline(t time.Time) error  { return d.conn.SetReadDeadline(t) }
func (d *DualConn) SetWriteDeadline(t time.Time) error { return d.conn.SetWriteDeadline(t) }

func (d *DualConn) HasError() bool {
	return d.Closed || d.CloseErr != "" || d.ReadErr != "" || d.WriteErr != ""
}

var ErrNotAvailable = errors.New("not available")
