package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/bingoohuang/dualconn"
	"github.com/bingoohuang/dualconn/db"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"
	"github.com/xo/dburl"
)

var (
	targets = pflag.StringArrayP("target", "t", []string{"127.0.0.1:3301", "127.0.0.1:3302"},
		"target address (IP:PORT)")
	listen = pflag.StringP("listen", "l", ":8080", "Listen address")
	dsn    = pflag.StringP("dsn", "d", "mysql://root:root@127.0.0.1:3306/db", "DSN")

	sdb *sql.DB
	mgr *dualconn.Manager
)

func main() {
	pflag.Parse()

	mgr = dualconn.NewManager(*targets, 3*time.Second).WithProtagonistHalo()

	mysql.RegisterDialContext("tcp", func(ctx context.Context, addr string) (net.Conn, error) {
		return mgr.DialContext(ctx, "tcp", addr)
	})

	var err error
	sdb, err = dburl.Open(*dsn)
	if err != nil {
		log.Fatalf("open db error: %v", err)
	}
	defer sdb.Close()

	// See "Important settings" section.
	sdb.SetConnMaxLifetime(3 * time.Minute)
	sdb.SetMaxOpenConns(10)
	sdb.SetMaxIdleConns(10)

	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		queryResult := db.RunSQL(r.Context(), sdb, r.URL.Query().Get("q"))
		if err := json.NewEncoder(w).Encode(queryResult); err != nil {
			log.Printf("encode queryResult error: %v", err)
		}
	})
	http.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(mgr); err != nil {
			log.Printf("encode manager info error: %v", err)
		}
	})
	http.HandleFunc("/enable", func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Query().Get("target")
		disabled := r.URL.Query().Get("disable") == "1"
		if !mgr.Enable(target, disabled) {
			w.WriteHeader(http.StatusNotFound)
		}
	})

	if err := http.ListenAndServe(*listen, nil); err != nil {
		log.Printf("listen on %s error: %v", *listen, err)
	}
}
