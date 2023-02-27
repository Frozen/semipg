package semipg

import (
	"database/sql/driver"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

type Column struct {
}

type Meta struct {
	Columns []*tree.ColumnTableDef
}
type Data struct {
	rows [][]driver.Value
}

type Table struct {
	Name tree.TableName
	Meta Meta
	Data Data
}

func NewTable(name tree.TableName) *Table {
	return &Table{
		Name: name,
	}
}
