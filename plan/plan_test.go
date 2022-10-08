package plan_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"

	plan "project/plan"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

/******************* partition json *************************/
// test, this meta should be in etcd
type Partitions struct {
	Partitions []Partition `json:"patitions"`
}
type Partition struct {
	TableName  string      `json:"table_name"`
	SiteIP     []string    `json:"site_ip"`
	Conditions []Condition `json:"condition"`
}
type Condition struct {
	Key    string           `json:"key"`
	Ranges []ConditionRange `json:"range"`
}
type ConditionRange struct {
	GreaterThan string `json:"gt"`
	LessThan    string `json:"lt"`
}

/************************************************************/

/******************* tableMeta json *************************/
// test, this meta should be in etcd
type TableMetas struct {
	TableMetas []TableMeta `json:"tables"`
}
type TableMeta struct {
	TableName string   `json:"table_name"`
	Columns   []Column `json:"columns"`
}
type Column struct {
	ColumnName string `json:"col_name"`
	Type       string `json:"type"`
}

/************************************************************/

type InsertRequest struct {
	SQL    string
	SiteIP string
}

func GetMidStr(s string, sep1 string, sep2 string) string {
	//
	c1 := strings.Index(s, sep1) + 1
	c2 := strings.LastIndex(s, sep2)
	s = string([]byte(s)[c1:c2])
	return s
}

func HandleInsert(ctx Context, stmt ast.StmtNode) InsertRequest {
	// router the insert sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition

	var ret InsertRequest

	sel := stmt.(*ast.InsertStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	ret.SQL = sql

	tablename := sel.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	values := strings.Split(GetMidStr(sel.Text(), "(", ")"), ",")

	var table_meta TableMeta
	var partition_meta Partition
	// find metaData
	for _, element := range ctx.table_metas.TableMetas {
		if element.TableName == tablename {
			table_meta = element
			break
		}
	}

	// find PartitionMetaData
	for _, element := range ctx.partitions.Partitions {
		if element.TableName == tablename {
			partition_meta = element
			break
		}
	}

	// check current value is in Partition or not
	for col_index_, col_ := range table_meta.Columns {
		val_ := values[col_index_]
		for _, cond_ := range partition_meta.Conditions {
			if cond_.Key == col_.ColumnName {
				// range
				for range_index_, range_ := range cond_.Ranges {
					if col_.Type == "string" {
						if range_.GreaterThan <= val_ &&
							val_ <= range_.LessThan {
							//! TODO: multipule partition condition on one table
							ret.SiteIP = partition_meta.SiteIP[range_index_]
						}
					} else if col_.Type == "int" {
						gt, _ := strconv.Atoi(range_.GreaterThan)
						lt, _ := strconv.Atoi(range_.LessThan)
						val, _ := strconv.Atoi(val_)

						if gt <= val &&
							val <= lt {
							//! TODO: multipule partition condition on one table
							ret.SiteIP = partition_meta.SiteIP[range_index_]
						}
					}

				}
			}
		}
	}

	fmt.Println(tablename)
	return ret
}

type Context struct {
	table_metas TableMetas
	partitions  Partitions
}

func HandleSelect(ctx Context, sel *ast.SelectStmt) (p plan.BasePlan, err error) {
	if sel.From != nil {
		p, err = buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Can't parse sql without From.")
	}
	// TODO unfold wildstar in `projection`
	// originalFields := sel.Fields.Fields
	// sel.Fields.Fields, err = unfoldWildStar(p, sel.Fields.Fields)
	// if err != nil {
	// 	return nil, err
	// }

	if sel.Where != nil {
		p, err = buildSelection(ctx, p, sel.Where)
		if err != nil {
			return nil, err
		}
	}

	p, err = buildProjection(ctx, p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}

	return p, err
}

// buildProjection returns a Projection plan and non-aux columns length.
func buildProjection(ctx Context, p plan.BasePlan, fields []*ast.SelectField) (plan.BasePlan, error) {
	proj := plan.PlanTreeNode{
		Type: plan.ProjectionType,
	}.Init()

	// schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)

	proj.SetChildren(p)
	return proj, nil
}

// splitWhere split a where expression to a list of AND conditions.
// func splitWhere(where ast.ExprNode) []ast.ExprNode {
// 	var conditions []ast.ExprNode
// 	switch x := where.(type) {
// 	case nil:
// 	case *ast.BinaryOperationExpr:
// 		if x.Op == opcode.LogicAnd {
// 			conditions = append(conditions, splitWhere(x.L)...)
// 			conditions = append(conditions, splitWhere(x.R)...)
// 		} else {
// 			conditions = append(conditions, x)
// 		}
// 	case *ast.ParenthesesExpr:
// 		conditions = append(conditions, splitWhere(x.Expr)...)
// 	default:
// 		conditions = append(conditions, where)
// 	}
// 	return conditions
// }

func buildSelection(ctx Context, p plan.BasePlan, where ast.ExprNode) (plan.BasePlan, error) {

	// conditions := splitWhere(where)
	// expressions := make([]expression.Expression, 0, len(conditions))
	selection := plan.PlanTreeNode{
		Type: plan.SelectType,
	}.Init()
	// TODO rewrite
	// selection.Conditions = expressions
	selection.SetChildren(p)
	return selection, nil
}

func buildJoin(ctx Context, joinNode *ast.Join) (plan.BasePlan, error) {
	if joinNode.Right == nil {
		return buildResultSetNode(ctx, joinNode.Left)
	}

	leftPlan, err := buildResultSetNode(ctx, joinNode.Left)
	if err != nil {
		return nil, err
	}

	rightPlan, err := buildResultSetNode(ctx, joinNode.Right)
	if err != nil {
		return nil, err
	}

	joinPlan := plan.PlanTreeNode{Type: plan.JoinType}.Init()
	joinPlan.SetChildren(leftPlan, rightPlan)
	// TODO set schema and join node name
	// joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	// joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	// copy(joinPlan.names, leftPlan.OutputNames())
	// copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())
	return joinPlan, nil
}

func buildDataSource(ctx Context, tn *ast.TableName) (plan.BasePlan, error) {
	// dbName := tn.Schema
	// TODO check table in this db
	// tbl, err := b.is.TableByName(dbName, tn.Name)
	// if err != nil {
	// 	return nil, err
	// }

	result := plan.PlanTreeNode{
		Type:          plan.DataSourceType,
		FromTableName: tn.Name.String(),
	}.Init()

	return result, nil
}

func buildResultSetNode(ctx Context, node ast.ResultSetNode) (p plan.BasePlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return buildJoin(ctx, x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.TableName:
			p, err = buildDataSource(ctx, v)
		default:
			err = errors.New("hello,error")
		}
		if err != nil {
			return nil, err
		}
		return p, nil
	default:
		return nil, errors.New("hello,error")
	}
}

func TestParseDebug(t *testing.T) {
	// read partion meta info
	jsonFileDir := "/home/bigdata/Course3-DDB/DDB_Project/config/partition.json"
	jsonFile, err := os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")
	byteValue, _ := ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var partitions Partitions
	json.Unmarshal([]byte(byteValue), &partitions)
	fmt.Println(partitions)
	////

	// read table meta info
	jsonFileDir = "/home/bigdata/Course3-DDB/DDB_Project/config/table_meta.json"
	jsonFile, err = os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")

	byteValue, _ = ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var table_metas TableMetas
	json.Unmarshal([]byte(byteValue), &table_metas)
	fmt.Println(table_metas)
	////

	ctx := Context{
		partitions:  partitions,
		table_metas: table_metas,
	}

	// parser and hanlder insert and select
	my_parser := parser.New()
	// stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.id = b.id", "", "")
	stmt, _ := my_parser.ParseOneStmt("select test.a, test2.b from test, test2 where test.a >= 2 && test2.b < 30;", "", "")
	// stmt, _ := my_parser.ParseOneStmt("insert into book values(20000, 'hello world');", "", "")
	// Otherwise do something with stmt
	switch x := stmt.(type) {
	case *ast.SelectStmt:
		fmt.Println("Select")
		_, _ = HandleSelect(ctx, x)
	case *ast.InsertStmt:
		fmt.Println("Insert") // same as delete
		HandleInsert(ctx, x)
	default:
		// createdb, dropdb, create table, drop table, all broadcast
	}

}
