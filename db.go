package semipg

import (
	"database/sql/driver"
	"fmt"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/pkg/errors"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

type DB struct {
	tables map[string]*Table
}

func NewDB() *DB {
	return &DB{
		tables: make(map[string]*Table),
	}
}

func (db *DB) CreateTable(table *tree.CreateTable) (*Result, error) {
	return db.createTable(table)
}

func (db *DB) AddTable(table *Table) error {
	return db.addTable(table)
}

func (db *DB) Insert(table *tree.Insert, values []driver.Value) (*Result, error) {
	return db.insert(table, values)
}

func (db *DB) addTable(table *Table) error {
	if _, ok := db.tables[table.Name.String()]; ok {
		return errors.New("table already exists")
	}
	db.tables[table.Name.String()] = table
	return nil
}

type Result struct {
	lastInsertId int64
	rowsAffected int64
}

func newResult(lastInsertId, rowsAffected int64) *Result {
	return &Result{
		lastInsertId: lastInsertId,
		rowsAffected: rowsAffected,
	}
}

func (r Result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (db *DB) Exec(query string) *Result {
	return &Result{}
}

func (db *DB) insert(insert *tree.Insert, values []driver.Value) (*Result, error) {

	for _, currentTable := range db.tables {
		if currentTable.Name.Equals(insert.Table.(*tree.TableName)) {
			valuesClause := insert.Rows.Select.(*tree.ValuesClause)
			if len(valuesClause.Rows) == 0 {
				return nil, errors.Errorf("no values provided")
			}
			for _, row := range valuesClause.Rows {
				if len(currentTable.Meta.Columns) != len(row) {
					return nil, errors.Errorf("column count mismatch, expected %d, got %d", len(valuesClause.Rows), len(row))
				}
				for _, e := range valuesClause.Rows[0] {
					switch v := e.(type) {
					case *tree.NumVal:
						fmt.Printf(" %T %+v %+v", v, v, currentTable.Meta.Columns[0])
					case *tree.StrVal:
						fmt.Printf(" %T %+v", v, v)
					}
				}
			}
		}
	}
	return nil, nil
}

func (db *DB) evaluateSelect(statement *tree.SelectStatement) error {
	return nil
}

func (db *DB) createTable(table *tree.CreateTable) (*Result, error) {
	t := NewTable(table.Table)
	for _, c := range table.Defs {
		switch c := c.(type) {
		case *tree.ColumnTableDef:
			t.Meta.Columns = append(t.Meta.Columns, c)
		}
	}

	if len(t.Meta.Columns) == 0 {
		return nil, errors.New("no columns defined")
	}

	return newResult(0, 0), db.addTable(t)
}

func (db *DB) Query(stmt parser.Statement, values []driver.Value) (*Rows, error) {
	/* Assert 	*/
	_ = stmt.AST
	_ = stmt.AST.(*tree.Select)
	_ = stmt.AST.(*tree.Select).Select
	_ = stmt.AST.(*tree.Select).Select.(*tree.SelectClause)
	/* 			*/

	selectClause := stmt.AST.(*tree.Select).Select.(*tree.SelectClause)
	if len(selectClause.Exprs) == 0 {
		return nil, errors.New("no columns defined")
	}
	if len(selectClause.From.Tables) == 0 {
		return nil, errors.New("no queries like `select 1` are supported")
	}

	/* Assert 	*/
	_ = selectClause.From
	_ = selectClause.From.Tables
	_ = selectClause.From.Tables[0]
	_ = selectClause.From.Tables[0].(*tree.AliasedTableExpr)
	_ = selectClause.From.Tables[0].(*tree.AliasedTableExpr).Expr
	_ = selectClause.From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.TableName)
	/* 			*/

	tableName := selectClause.From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.TableName)
	table, ok := db.tables[tableName.String()]
	if !ok {
		return nil, errors.Errorf("table %s not found", tableName.String())
	}

	var columns []int
	//table.Meta.Columns[0].Type
	fmt.Printf("tables: %T %+v\n", selectClause.From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.TableName), selectClause.From.Tables[0].(*tree.AliasedTableExpr))

	for _, v := range stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs {
		fmt.Printf("expr: %T\n", v.Expr)
		switch t := v.Expr.(type) {
		case tree.UnqualifiedStar:
			fmt.Printf("unqualified star: %T\n", t)
		case *tree.UnqualifiedStar:
			fmt.Printf("unqualified star: %T\n", t)
		case *tree.UnresolvedName:
			found := false
			for idx, col := range table.Meta.Columns {
				if col.Name.String() == t.String() {
					found = true
					columns = append(columns, idx)
				}
			}
			if !found {
				return nil, errors.Errorf("column %s not found", t.String())
			}
			fmt.Printf("unresolved name: %T, %s\n", t, t.String())
		}
	}

	temporaryStorage := make([][]driver.Value, 0, 1)

	for _, v := range table.Data.rows {
		temporaryRow := make([]driver.Value, 0, len(columns))
		for _, c := range columns {
			temporaryRow = append(temporaryRow, v[c])
		}
		// filter row
		if true {
			temporaryStorage = append(temporaryStorage, temporaryRow)
		}
	}
	return &Rows{
		columns: columns,
		table:   table,
		db:      db,
		index:   -1,
	}, nil
}
