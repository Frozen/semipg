package semipg

import (
	"log"
	"testing"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

func TestMainf(t *testing.T) {
	sql := `create table item (id int, name varchar(255), price int, primary key (id));`
	stmts, err := parser.Parse(sql)
	if err != nil {
		return
	}

	db := NewDB()

	for _, s := range stmts {
		log.Printf("stmt type %T %+v", s.AST, s.AST)
		v := s.AST.(*tree.CreateTable)

		//table := NewTable(v.Table)
		_, err := db.CreateTable(v)
		if err != nil {
			log.Fatalf("add table error %v", err)
		}

		//v.Table

		//v.Select.(*tree.SelectClause)
		//log.Printf("stmt type %T %+v", v, v)
	}

	sql = `insert into item values(1, 'item1', 100);`

	stmts, err = parser.Parse(sql)
	if err != nil {
		return
	}

	for _, s := range stmts {
		log.Printf("stmt type %T %+v", s.AST, s.AST)
		v := s.AST.(*tree.Insert)

		//table := NewTable(v.Table)
		_, err := db.Insert(v, nil)
		if err != nil {
			log.Fatalf("add table error %v", err)
		}

		//db.Insert(v.Table, v.Select)

		//v.Table

		//v.Select.(*tree.SelectClause)
		//log.Printf("stmt type %T %+v", v, v)
	}

	sql = `select * from item where id = $1;`

	stmts, err = parser.Parse(sql)

	if err != nil {
		log.Fatal(err)
	}

	for _, v := range stmts {
		log.Printf("stmt type %T %+v", v.AST, v.AST)
		log.Printf("stmt type %T %+v", v.AST.(*tree.Select).Select.(*tree.SelectClause).Where.Expr.(*tree.ComparisonExpr).Right, v.AST.(*tree.Select).Select.(*tree.SelectClause).Where.Expr.(*tree.ComparisonExpr).Right)
	}

	return
}
