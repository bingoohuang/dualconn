package db

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/xwb1989/sqlparser"
	"go.uber.org/multierr"
)

type QueryResult struct {
	Error string           `json:"error,omitempty"`
	Cost  string           `json:"cost,omitempty"`
	Rows  []map[string]any `json:"rows,omitempty"`
}

type DB interface {
	ExecAware
	Queryer
}

func RunSQL(ctx context.Context, dba DB, query string) *QueryResult {
	scanner := NewJsonRowsScanner(0, 30)
	firstWord := strings.ToLower(strings.Fields(query)[0])
	switch firstWord {
	default:
		return Exec(ctx, dba, query, nil, scanner)
	case "select", "show", "desc", "describe":
		return Query(ctx, dba, query, nil, scanner)
	case "insert":
		if strings.Contains(strings.ToLower(query), "returning") {
			return Query(ctx, dba, query, nil, scanner)
		}

		return Exec(ctx, dba, query, nil, scanner)
	}
}

func Query(ctx context.Context, db Queryer, q string, args []any, scanner *JsonRowsScanner) *QueryResult {
	_ = PingDB(ctx, db, 3*time.Second)

	scanner.StartExecute()

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return &QueryResult{Error: err.Error()}
	}

	defer rows.Close()

	if err := scanner.Scan(rows); err != nil {
		return &QueryResult{Error: err.Error()}
	}

	qr := &QueryResult{}
	scanner.Complete(qr)
	return qr
}

type ExecAware interface {
	// ExecContext executes a query without returning any rows.
	// The args are for any placeholder parameters in the query.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type RowsScanner interface {
	StartExecute()
	StartRows(header []string)
	// AddRow add a row cells value
	// If return true, the scanner will continue, or return false to stop scanning
	AddRow(rowIndex int, columns []any) bool
	Complete(result *QueryResult)
}

func Exec(ctx context.Context, db DB, q string, args []any, rowsScanner RowsScanner) *QueryResult {
	_ = PingDB(ctx, db, 3*time.Second)

	rowsScanner.StartExecute()
	result, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return &QueryResult{Error: err.Error()}
	}

	id, err1 := result.LastInsertId()
	affected, err2 := result.RowsAffected()

	if err := multierr.Append(err1, err2); err != nil {
		rowsScanner.StartRows([]string{"lastInsertId", "rowsAffected", "error"})
		rowsScanner.AddRow(0, []any{id, affected, err})
	} else {
		rowsScanner.StartRows([]string{"lastInsertId", "rowsAffected"})
		rowsScanner.AddRow(0, []any{id, affected})
	}

	qr := &QueryResult{}
	rowsScanner.Complete(qr)

	return qr
}

func PingDB(ctx context.Context, db Queryer, timeout time.Duration) error {

	timeoutCtx, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()

	for i := 0; i < 3; i++ {
		rows, err := db.QueryContext(timeoutCtx, "select 1")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		_ = rows.Close()
		break
	}

	return nil
}

type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type JsonRowsScanner struct {
	start         time.Time
	Header        []string
	Limit, Offset int

	Rows []map[string]any
}

func NewJsonRowsScanner(offset, limit int) *JsonRowsScanner {
	return &JsonRowsScanner{Limit: limit, Offset: offset}
}

func (j *JsonRowsScanner) StartExecute() {
	j.start = time.Now()
}

func (j *JsonRowsScanner) StartRows(header []string) {
	j.Header = header
}

func (j *JsonRowsScanner) AddRow(rowIndex int, columns []any) bool {
	if j.Offset > 0 && rowIndex < j.Offset {
		return true
	}

	if rowIndex+1 > j.Limit+j.Offset {
		return false
	}

	row := map[string]any{}
	for i, h := range j.Header {
		row[h] = columns[i]
	}
	j.Rows = append(j.Rows, row)

	return j.Limit <= 0 || rowIndex+1 <= j.Limit+j.Offset
}

func (j *JsonRowsScanner) Complete(result *QueryResult) {
	result.Cost = time.Since(j.start).String()
	result.Rows = j.Rows
}

func (j *JsonRowsScanner) Scan(rows *sql.Rows) error {
	scan, err := NewRowScanner(rows)
	if err != nil {
		return err
	}

	rowNum := 0
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	j.StartRows(columns)

	for ; scan.Next(); rowNum++ {
		row, err := scan.Scan()
		if err != nil {
			return err
		}

		if !j.AddRow(rowNum, row) {
			break
		}
	}

	return rows.Err()
}

type ValueType int

const (
	_ ValueType = iota
	ValueTypeBool
	ValueTypeInt64
	ValueTypeFloat64
	ValueTypeString
	ValueTypeBytes
	ValueTypeOther
)

type RowScanner struct {
	Rows *sql.Rows

	Types        []ValueType
	LowerColumns []string
	Columns      []string
}

func NewRowScanner(rows *sql.Rows) (*RowScanner, error) {
	scanner := &RowScanner{
		Rows: rows,
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	scanner.Columns = columns

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	scanner.Types = lo.Map(columnTypes, func(columnType *sql.ColumnType, index int) ValueType {
		switch typeName := strings.ToUpper(columnType.DatabaseTypeName()); {
		case Contains(typeName, "CHAR", "TEXT", "NVARCHAR"):
			return ValueTypeString
		case Contains(typeName, "BOOL"):
			return ValueTypeBool
		case Contains(typeName, "BOOL", "INT", "NUMBER"):
			return ValueTypeInt64
		case Contains(typeName, "DECIMAL"):
			return ValueTypeFloat64
		case Contains(typeName, "LOB"):
			return ValueTypeBytes
		default:
			return ValueTypeOther
		}
	})

	return scanner, nil
}

func Contains(s string, ss ...string) bool {
	for _, of := range ss {
		if strings.Contains(s, of) {
			return true
		}
	}
	return false
}

func (s RowScanner) Next() bool {
	return s.Rows.Next()
}

func (s RowScanner) Scan() ([]any, error) {
	values := lo.Map(s.Types, func(t ValueType, index int) any {
		return &NullAny{ValueType: t}
	})
	if err := s.Rows.Scan(values...); err != nil {
		return nil, err
	}

	row := lo.Map(values, func(val any, idx int) any {
		colValue := s.getColValue(*val.(*NullAny))
		return colValue
	})
	return row, nil
}

func (s RowScanner) getColValue(n NullAny) any {
	if !n.Valid {
		return nil
	}

	switch n.ValueType {
	case ValueTypeInt64, ValueTypeFloat64:
		return n.Value
	case ValueTypeBytes:
		if data, ok := n.Value.([]byte); ok {
			return string(data)
		}
	}

	return Quote(fmt.Sprintf("%v", n.Value))
}

const (
	quote  = '\''
	escape = '\\'
)

// Quote returns a single-quoted Go string literal representing s. But, nothing else escapes.
func Quote(s string) string {
	out := []rune{quote}
	for _, r := range s {
		switch r {
		case quote:
			out = append(out, escape, r)
		default:
			out = append(out, r)
		}
	}
	out = append(out, quote)
	return string(out)
}

func GetSingleTableName(query string) string {
	result, err := sqlparser.Parse(query)
	if err != nil {
		return ""
	}

	sel, _ := result.(*sqlparser.Select)
	if sel == nil || len(sel.From) != 1 {
		return ""
	}

	expr, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return ""
	}

	return sqlparser.GetTableName(expr.Expr).String()
}

type NullAny struct {
	Value     any
	ValueType ValueType
	Valid     bool
}

// Scan implements the Scanner interface.
func (ns *NullAny) Scan(value any) error {
	ns.Valid = value != nil
	if !ns.Valid {
		return nil
	}

	switch ns.ValueType {
	case ValueTypeBool:
		var v0 sql.NullBool
		err := v0.Scan(value)
		ns.Value = v0.Bool
		return err
	case ValueTypeInt64:
		var v0 sql.NullInt64
		err := v0.Scan(value)
		ns.Value = v0.Int64
		return err
	case ValueTypeFloat64:
		var v1 sql.NullFloat64
		err := v1.Scan(value)
		ns.Value = v1.Float64
		return err
	case ValueTypeString:
		var v2 sql.NullString
		err := v2.Scan(value)
		ns.Value = v2.String
		return err
	case ValueTypeBytes:
		if _, ok := value.([]byte); ok {
			ns.Value = value
			return nil
		}
		fallthrough
	default:
		switch nv := value.(type) {
		case int8, int16, int32, int, int64, uint8, uint16, uint32, uint, uint64:
			ns.ValueType = ValueTypeInt64
			ns.Value = nv
		case float32, float64:
			ns.ValueType = ValueTypeFloat64
			ns.Value = nv
		case string:
			ns.ValueType = ValueTypeString
			ns.Value = nv
		case []byte:
			if len(nv) < 1024 {
				if ns.ValueType == ValueTypeOther {
					ns.ValueType = ValueTypeString
					ns.Value = string(nv)
				}
			} else {
				ns.ValueType = ValueTypeBytes
				ns.Value = nv
			}
		default:
			ns.convertAlias(value)
		}

		return nil
	}
}

func (ns *NullAny) convertAlias(value any) {
	rv := reflect.ValueOf(value)

	for _, alias := range aliasConverters {
		if rv.CanConvert(alias.Type) {
			alias.Converter(alias.Type, rv, ns)
			return
		}
	}

	ns.Value = value
}

type AliasConvert struct {
	Type      reflect.Type
	Converter func(typ reflect.Type, value reflect.Value, ns *NullAny)
}

var aliasConverters = []AliasConvert{
	{
		Type: reflect.TypeOf((*time.Time)(nil)).Elem(),
		Converter: func(typ reflect.Type, rv reflect.Value, ns *NullAny) {
			t := rv.Convert(typ).Interface().(time.Time)

			// refer: https://github.com/hexon/mysqltsv/blob/main/mysqltsv.go
			hour, minute, sec := t.Clock()

			switch nsec := t.Nanosecond(); {
			case hour == 0 && minute == 0 && sec == 0 && nsec == 0:
				ns.Value = t.Format("2006-01-02")
			case nsec == 0:
				ns.Value = t.Format("2006-01-02 15:04:05")
			default:
				ns.Value = t.Format("2006-01-02 15:04:05.999999999")
			}
			ns.ValueType = ValueTypeString
		},
	},

	{
		Type: reflect.TypeOf(""),
		Converter: func(typ reflect.Type, rv reflect.Value, ns *NullAny) {
			ns.ValueType = ValueTypeString
			ns.Value = rv.Convert(typ).Interface().(string)
		},
	},
}
