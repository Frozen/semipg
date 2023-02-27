package semipg

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

var (
	_ driver.Driver = Driver{}
)

type Driver struct {
}

func (d Driver) Open(name string) (driver.Conn, error) {
	db := NewDB()
	return Conn{
		db: db,
	}, nil
}

type Conn struct {
	db *DB
}

type Prepare struct {
	stmt parser.Statement
	db   *DB
}

func (p Prepare) Close() error {
	return nil
}

func (p Prepare) NumInput() int {
	v := NewPlaceholderVisitor()
	switch t := p.stmt.AST.(type) {
	case *tree.Select:
		_ = t.Select
		_ = t.Select.(*tree.SelectClause)
		_ = t.Select.(*tree.SelectClause).Where
		//_ = t.Select.(*tree.SelectClause).Where.Expr
		if t.Select.(*tree.SelectClause).Where != nil {
			t.Select.(*tree.SelectClause).Where.Expr.Walk(v)
		}
		return v.count
		//return calculatePlaceholders(t.Select.(*tree.SelectClause).Where.Expr)
	case *tree.CreateTable:
		return 0
	case *tree.Insert:
		if len(t.Rows.Select.(*tree.ValuesClause).Rows) != 1 {
			panic("only one row is supported")
		}
		v := NewPlaceholderVisitor()
		for _, expr := range t.Rows.Select.(*tree.ValuesClause).Rows[0] {
			expr.Walk(v)
		}
		return v.count
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}

func (p Prepare) Exec(args []driver.Value) (driver.Result, error) {
	if placeholders := p.NumInput(); len(args) != placeholders {
		return nil, errors.Errorf("expected %d arguments, got %d", placeholders, len(args))
	}
	switch t := p.stmt.AST.(type) {
	case *tree.Select:
		return nil, errors.New("select is not supported")
	case *tree.CreateTable:
		return p.db.CreateTable(t)
	case *tree.Insert:
		return p.db.Insert(t, args)
	}
	panic(fmt.Sprintf("unexpected type %T, %+v", p.stmt.AST, args))
}

func (p Prepare) Query(args []driver.Value) (driver.Rows, error) {
	return p.db.Query(p.stmt, args)
}

func (c Conn) Prepare(query string) (driver.Stmt, error) {
	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.New("only one statement is supported")
	}
	return Prepare{
		stmt: stmts[0],
		db:   c.db,
	}, nil
}

func (c Conn) Close() error {
	c.db = nil
	return nil
}

func (c Conn) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func init() {
	//sql.DB{}
	sql.Register("semipg", &Driver{})
}

// Rows implements the driver.Rows interface.
type Rows struct {
	db      *DB
	columns []int
	table   *Table
	index   int
}

func (r *Rows) Columns() []string {
	columns := make([]string, 0, len(r.columns))
	for _, c := range r.columns {
		//_ = r.Table.Meta.Columns[c].Name
		columns = append(columns, r.table.Meta.Columns[c].Name.String())
	}
	return columns
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	r.index++
	if r.index >= len(r.table.Data.rows) {
		return io.EOF
	}
	row := r.table.Data.rows[r.index]
	for i, c := range r.columns {
		dest[i] = row[c]
	}
	return nil
}
