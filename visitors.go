package semipg

import (
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

type PlaceholderVisitor struct {
	count int
}

func (v *PlaceholderVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch expr.(type) {
	case *tree.Placeholder:
		v.count++
	}
	return true, expr
}

func (v *PlaceholderVisitor) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	return expr
}

func NewPlaceholderVisitor() *PlaceholderVisitor {
	return &PlaceholderVisitor{}
}
