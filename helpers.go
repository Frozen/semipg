package semipg

import (
	"fmt"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

func calculatePlaceholders(expr tree.Expr) int {
	switch t := expr.(type) {
	case *tree.Placeholder:
		return 1
	case *tree.ComparisonExpr:
		return calculatePlaceholders(t.Left) + calculatePlaceholders(t.Right)
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}
