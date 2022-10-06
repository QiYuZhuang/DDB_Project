package database_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func TestParseDebug(t *testing.T) {
	my_parser := parser.New()
	// stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.id = b.id", "", "")

	stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.id = b.id", "", "")
	// Otherwise do something with stmt
	switch stmt.(type) {
	case *ast.SelectStmt:
		fmt.Println("Select")
	case *ast.InsertStmt:
		fmt.Println("Insert")
	default:
		// createdb, dropdb, create table, drop table, all broadcast
	}

}
