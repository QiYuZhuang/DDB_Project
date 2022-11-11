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

	cfg "project/config"
	coordinator "project/core"
	core "project/core"
	mysql "project/mysql"
	plan "project/plan"
	utils "project/utils"
	logger "project/utils/log"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

/******************* partition json *************************/
// test, this meta should be in etcd
type Partitions struct {
	Partitions []Partition `json:"patitions"`
}
type Partition struct {
	TableName  string      `json:"table_name"`
	SiteInfos  []SiteInfo  `json:"site_info"`
	FragType   string      `json:"fragmentation_type"`
	HFragInfos []HFragInfo `json:"horizontal_fragmentation"`
	VFragInfos []VFragInfo `json:"vertical_fragmentation"`
}

type SiteInfo struct {
	Name string `json:"frag_name"`
	IP   string `json:"ip"`
}
type HFragInfo struct {
	FragName   string           `json:"frag_name"`
	Conditions []ConditionRange `json:"range"`
}

type ConditionRange struct {
	ColName          string `json:"col_name"`
	GreaterEqualThan string `json:"get"`
	LessThan         string `json:"lt"`
	Equal            string `json:"eq"`
}

type VFragInfo struct {
	SiteName   string   `json:"frag_name"`
	ColumnName []string `json:"col_names"`
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
	Siteinfo     SiteInfo
	InsertValues []InsertValue
}

type InsertValue struct {
	ColName string
	Val     string
}

func GetMidStr(s string, sep1 string, sep2 string) string {
	//
	c1 := strings.Index(s, sep1) + 1
	c2 := strings.LastIndex(s, sep2)
	s = string([]byte(s)[c1:c2])
	return s
}

func FindMetaInfo(ctx Context, tablename string) (TableMeta, Partition, error) {
	var table_meta TableMeta
	var partition_meta Partition
	var err error
	// find metaData
	is_find_ := false
	for _, element := range ctx.table_metas.TableMetas {
		if strings.EqualFold(element.TableName, tablename) {
			table_meta = element
			is_find_ = true
			break
		}
	}
	if is_find_ == false {
		err = errors.New("fail to find " + tablename + " in current database")
		return table_meta, partition_meta, err
	}
	// find PartitionMetaData
	for _, element := range ctx.partitions.Partitions {
		if strings.EqualFold(element.TableName, tablename) {
			partition_meta = element
			break
		}
	}
	if is_find_ == false {
		err = errors.New("fail to find partition info about " + tablename + " in current database")
		return table_meta, partition_meta, err
	}

	return table_meta, partition_meta, err
}

func GenInsertRequest(table_meta TableMeta, partition_meta Partition, values []string) ([]InsertRequest, error) {
	//
	var returns []InsertRequest
	var err error

	if strings.EqualFold(partition_meta.FragType, "HORIZONTAL") {
		// insert to one table only
		var ret InsertRequest

		for frag_index_, frag_ := range partition_meta.HFragInfos {
			is_satisfied_ := true

			for _, cond_ := range frag_.Conditions {
				// cond_ are joined with AND only

				// get index and value for cur cond_
				cur_col_index_ := -1
				var cur_col Column

				for col_index_, col_ := range table_meta.Columns {
					if strings.EqualFold(col_.ColumnName, cond_.ColName) {
						cur_col_index_ = col_index_
						cur_col = col_
						break
					}
				}
				if cur_col_index_ == -1 {
					err = errors.New("Fail to find cond " + cond_.ColName + " in table " + table_meta.TableName)
					return returns, err
				}

				val_ := values[cur_col_index_]
				val_ = strings.Trim(val_, " ")
				val_ = strings.Trim(val_, "'")

				// value get compared with condition
				if len(cond_.Equal) != 0 {
					// EQ
					if cond_.Equal != val_ {
						is_satisfied_ = false
						break
					}
				} else {
					// GT LT
					if strings.EqualFold(cur_col.Type, "string") {
						if !(cond_.GreaterEqualThan <= val_ &&
							val_ < cond_.LessThan) {
							is_satisfied_ = false
							break
						}
					} else if strings.EqualFold(cur_col.Type, "int") {
						var get int
						var lt int
						if len(cond_.GreaterEqualThan) > 0 {
							get, _ = strconv.Atoi(cond_.GreaterEqualThan)
						} else {
							get = MinInt
						}

						if len(cond_.LessThan) > 0 {
							lt, _ = strconv.Atoi(cond_.LessThan)
						} else {
							lt = MaxInt
						}
						// get, _ := strconv.Atoi(cond_.GreaterEqualThan)
						// lt, _ := strconv.Atoi(cond_.LessThan)
						val, _ := strconv.Atoi(val_)

						if !(get <= val &&
							val < lt) {
							is_satisfied_ = false
							break
						}
					}
				}
			}
			if is_satisfied_ {
				ret.Siteinfo = partition_meta.SiteInfos[frag_index_]
				for col_index_, col_ := range table_meta.Columns {
					var insert_val InsertValue
					val_ := values[col_index_]
					insert_val.ColName = col_.ColumnName
					insert_val.Val = val_
					ret.InsertValues = append(ret.InsertValues, insert_val)
				}
				break
			}
		}
		returns = append(returns, ret)
	} else if strings.EqualFold(partition_meta.FragType, "VERTICAL") {
		for frag_index, fag_ := range partition_meta.VFragInfos {
			var ret InsertRequest
			ret.Siteinfo = partition_meta.SiteInfos[frag_index]

			for _, col_name := range fag_.ColumnName {
				// per site
				var insert_val InsertValue

				for col_index_, col_ := range table_meta.Columns {
					if strings.EqualFold(col_.ColumnName, col_name) {
						val_ := values[col_index_]
						insert_val.ColName = col_name
						insert_val.Val = val_
					}
				}
				ret.InsertValues = append(ret.InsertValues, insert_val)
			}
			returns = append(returns, ret)
		}
	} else {
		err = errors.New("todo none frag table")
	}
	return returns, err
}

func GenInsertSQL(insert_requests []InsertRequest) ([]SqlRouter, error) {
	var err error
	var ret []SqlRouter

	for _, insert_req := range insert_requests {
		// insert into ...(col1, col2) values();
		var table_cols string
		var table_vals string
		for _, col_ := range insert_req.InsertValues {
			if len(table_cols) > 0 {
				table_cols += ", "
				table_vals += ", "
			}
			table_cols += col_.ColName
			table_vals += col_.Val
		}
		cur_sql := "insert into " + insert_req.Siteinfo.Name + "(" + table_cols + ") values " + "(" + table_vals + ");"
		var cur_insert SqlRouter
		cur_insert.site_ip = insert_req.Siteinfo.IP
		cur_insert.sql = cur_sql
		ret = append(ret, cur_insert)
	}
	return ret, err
}

type SqlRouter struct {
	sql     string
	site_ip string
}

func HandleCreateTable(ctx Context, stmt ast.StmtNode) ([]SqlRouter, error) {
	// router the create table sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []SqlRouter
	sel := stmt.(*ast.CreateTableStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	// ret.SQL = sql

	tablename := sel.Table.Name.String()

	create_cols := strings.Split(GetMidStr(sel.Text(), "(", ")"), ",")

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}
	if strings.EqualFold(partition_meta.FragType, "HORIZONTAL") {
		// directly replace table NAME
		for frag_index, _ := range partition_meta.HFragInfos {
			site_name_ := partition_meta.SiteInfos[frag_index].Name
			var sql_router_ SqlRouter
			sql_router_.site_ip = partition_meta.SiteInfos[frag_index].IP
			sql_router_.sql = sql
			sql_router_.sql = strings.Replace(sql_router_.sql, table_meta.TableName, site_name_, 1)
			ret = append(ret, sql_router_)
		}
	} else {
		// vertical fragment
		for frag_index_, frag_ := range partition_meta.VFragInfos {
			var per SqlRouter
			var col_sql_str string

			covered_frag_col := 0
			for _, create_col := range create_cols {
				// find col_ in current fragment
				is_find_ := false
				for _, col_ := range frag_.ColumnName {
					if strings.Contains(create_col, col_) {
						is_find_ = true
						break
					}
				}
				//
				if is_find_ {
					covered_frag_col += 1
					if len(col_sql_str) > 0 {
						col_sql_str += ", "
					}
					col_sql_str += create_col
				}
			}
			if covered_frag_col != len(frag_.ColumnName) {
				err = errors.New("Dont cover all cols in frag " + frag_.SiteName + " in table " + table_meta.TableName)
				break
			}
			per.site_ip = partition_meta.SiteInfos[frag_index_].IP
			per.sql = "create table " + partition_meta.SiteInfos[frag_index_].Name + " ( " + col_sql_str + " )"
			ret = append(ret, per)
		}
	}

	fmt.Println(tablename)
	return ret, err
}

func HandleDropTable(ctx Context, stmt ast.StmtNode) ([]SqlRouter, error) {
	// router the create table sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []SqlRouter
	sel := stmt.(*ast.DropTableStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	// ret.SQL = sql

	tablename := sel.Tables[0].Name.String()

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}
	//
	for _, site_info := range partition_meta.SiteInfos {
		site_name_ := site_info.Name
		var sql_router_ SqlRouter
		sql_router_.site_ip = site_info.IP
		sql_router_.sql = sql
		sql_router_.sql = strings.Replace(sql_router_.sql, table_meta.TableName, site_name_, 1)
		ret = append(ret, sql_router_)
	}

	fmt.Println(tablename)
	return ret, err
}

func HandleInsert(ctx Context, stmt ast.StmtNode) ([]SqlRouter, error) {
	// router the insert sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []SqlRouter
	sel := stmt.(*ast.InsertStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	// ret.SQL = sql

	tablename := sel.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	values := strings.Split(GetMidStr(sel.Text(), "(", ")"), ",")

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}

	// check current value is in Partition or not
	insert_requests, err := GenInsertRequest(table_meta, partition_meta, values)
	if err != nil {
		return ret, err
	}
	// generate sql with router ip
	ret, err = GenInsertSQL(insert_requests)
	fmt.Println(tablename)
	return ret, err
}

func HandleDelete(ctx Context, stmt ast.StmtNode) ([]SqlRouter, error) {
	// router the delete sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []SqlRouter
	sel := stmt.(*ast.DeleteStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	// ret.SQL = sql

	tablename := sel.Tables.Tables[0].Name.String()

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}
	if strings.EqualFold(partition_meta.FragType, "HORIZONTAL") {
		for frag_index, _ := range partition_meta.HFragInfos {
			site_name_ := partition_meta.SiteInfos[frag_index].Name
			var sql_router_ SqlRouter
			sql_router_.site_ip = partition_meta.SiteInfos[frag_index].IP
			sql_router_.sql = sql
			sql_router_.sql = strings.Replace(sql_router_.sql, table_meta.TableName, site_name_, 1)
		}
	} else {
		// vertical fragment
		// ask certain table for primary key
		// and broadcast delete to all frags
		err = errors.New("todo not implemented vertical delete")
		//
	}

	fmt.Println(tablename)
	return ret, err
}

func BroadcastSQL(ctx Context, coord *coordinator.Coordinator, stmt ast.StmtNode) ([]SqlRouter, error) {
	var ret []SqlRouter
	sql := stmt.Text()
	fmt.Println(sql)

	for _, peer := range coord.Peers {
		var per SqlRouter
		per.site_ip = peer.Ip
		per.sql = sql
		ret = append(ret, per)
	}
	return ret, nil
}

type Context struct {
	table_metas TableMetas
	partitions  Partitions
}

// func unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
// 	join, isJoin := p.(*LogicalJoin)
// 	for i, field := range selectFields {
// 		if field.WildCard == nil {
// 			resultList = append(resultList, field)
// 			continue
// 		}
// 		if field.WildCard.Table.L == "" && i > 0 {
// 			return nil, ErrInvalidWildCard
// 		}
// 		list := unfoldWildStar(field, p.OutputNames(), p.Schema().Columns)
// 		// For sql like `select t1.*, t2.* from t1 join t2 using(a)` or `select t1.*, t2.* from t1 natual join t2`,
// 		// the schema of the Join doesn't contain enough columns because the join keys are coalesced in this schema.
// 		// We should collect the columns from the fullSchema.
// 		if isJoin && join.fullSchema != nil && field.WildCard.Table.L != "" {
// 			list = unfoldWildStar(field, join.fullNames, join.fullSchema.Columns)
// 		}
// 		// if len(list) == 0 {
// 		// 	return nil, ErrBadTable.GenWithStackByArgs(field.WildCard.Table)
// 		// }
// 		resultList = append(resultList, list...)
// 	}
// 	return resultList, nil
// }

func PrintPlanTree(p *plan.PlanTreeNode) string {
	var result string
	//新建一个队列
	queue := []*plan.PlanTreeNode{p}

	i := 0
	for len(queue) > 0 {
		//新建临时队列，用于重新给queue赋值
		temp := []*plan.PlanTreeNode{}
		//新建每一行的一维数组

		for _, v := range queue {
			//
			if v.Type == plan.DataSourceType {
				result += v.Type.String() + "[" + v.FromTableName + "]" + " "
			} else if v.Type == plan.JoinType || v.Type == plan.UnionType {
				result += v.Type.String() + "[" + v.FromTableName + "]" + " "
			} else {
				result += v.Type.String() + " "
			}

			nums := v.GetChildrenNum()
			if nums == 0 {
				continue
			}
			for i := 0; i < nums; i++ {
				cur_node := v.GetChild(i)
				temp = append(temp, cur_node)
			}
		}
		result += "\n"
		i++
		//二叉树新的一行的节点放入队列中
		queue = temp
	}
	return result
}

func InitFragWithCondition(frag_info Partition, frag_name string) ([]ast.ExprNode, []string) {
	var conds_ []ast.ExprNode
	var cols_ []string

	if strings.EqualFold(frag_info.FragType, "HORIZONTAL") {
		for _, frag_ := range frag_info.HFragInfos {
			if strings.EqualFold(frag_.FragName, frag_name) {
				for _, cond_ := range frag_.Conditions {
					var new_expr_node ast.BinaryOperationExpr
					var col_attr_node ast.ColumnNameExpr
					var name ast.ColumnName
					name.Table.O = frag_.FragName
					name.Name.O = cond_.ColName

					col_attr_node.Name = &name
					new_expr_node.L = &col_attr_node

					if cond_.Equal == "" {
						// <  >=
						if cond_.LessThan != "" {
							new_expr_node_ := new_expr_node
							new_expr_node_.Op = opcode.LT

							var val_attr_node test_driver.ValueExpr
							val_attr_node.Datum.SetString(cond_.LessThan)
							val_attr_node.SetText(cond_.LessThan)
							new_expr_node_.R = &val_attr_node

							conds_ = append(conds_, &new_expr_node_)
						}
						if cond_.GreaterEqualThan != "" {
							new_expr_node_ := new_expr_node
							new_expr_node_.Op = opcode.GE

							var val_attr_node test_driver.ValueExpr
							val_attr_node.Datum.SetString(cond_.GreaterEqualThan)
							val_attr_node.SetText(cond_.GreaterEqualThan)
							new_expr_node_.R = &val_attr_node

							conds_ = append(conds_, &new_expr_node_)
						}
					} else {
						// ==
						new_expr_node_ := new_expr_node
						new_expr_node_.Op = opcode.EQ

						var val_attr_node test_driver.ValueExpr
						val_attr_node.Datum.SetString(cond_.Equal)
						val_attr_node.SetText(cond_.Equal)
						new_expr_node_.R = &val_attr_node

						conds_ = append(conds_, &new_expr_node_)
					}
				}
			}
		}
	} else {
		for _, frag_ := range frag_info.VFragInfos {
			if strings.EqualFold(frag_.SiteName, frag_name) {
				cols_ = append(cols_, frag_.ColumnName...)
			}
		}
	}
	return conds_, cols_
}

func SplitFragTable_(ctx Context, p *plan.PlanTreeNode) error {
	var frags_ Partition
	var err error
	is_find_ := false

	for _, partition := range ctx.partitions.Partitions {
		if strings.EqualFold(partition.TableName, p.FromTableName) {
			frags_ = partition
			is_find_ = true
			break
		}
	}
	if is_find_ == false {
		err = errors.New("fail to find table " + p.FromTableName + " in any frags")
		return err
	}

	// find and expand the frag tables
	for _, site_info := range frags_.SiteInfos {
		if strings.EqualFold(frags_.FragType, "HORIZONTAL") {
			p.Type = plan.UnionType
			node := plan.PlanTreeNode{
				Type:          plan.DataSourceType,
				FromTableName: site_info.Name,
			}.Init()
			//
			conds_, _ := InitFragWithCondition(frags_, site_info.Name)
			node.Conditions = append(node.Conditions, conds_...)
			p.AddChild(node)

		} else {
			p.Type = plan.JoinType
			node := plan.PlanTreeNode{
				Type:          plan.DataSourceType,
				FromTableName: site_info.Name,
			}.Init()
			_, cols_ := InitFragWithCondition(frags_, site_info.Name)
			node.ColsName = append(node.ColsName, cols_...)
			p.AddChild(node)
		}
	}
	return nil
}

func SplitFragTable(ctx Context, p *plan.PlanTreeNode) {
	nums := p.GetChildrenNum()
	for i := 0; i < nums; i++ {
		child_ := p.GetChild(i)
		if child_.Type == plan.DataSourceType {
			//
			SplitFragTable_(ctx, child_)
		} else {
			SplitFragTable(ctx, child_)
		}
	}
}

func tryPushDown(subWhere string, beginNode int64) {
	// pos := find2ChildNode(beginNode)
	// if pos == -1 { //若为-1则说明没有两个孩子的节点，只能加在curPos上
	// 	addWhereNodeOnTop(iparser.CreateSelectionNode(iparser.GetTmpTableName(), subWhere), beginNode)
	// } else {
	// 	flag1 := checkCols(subWhere, pt.Nodes[pt.Nodes[pos].Left].Rel_cols)
	// 	flag2 := checkCols(subWhere, pt.Nodes[pt.Nodes[pos].Right].Rel_cols)
	// 	if flag1 == false && flag2 == false {
	// 		addWhereNodeOnTop(iparser.CreateSelectionNode(iparser.GetTmpTableName(), subWhere), pos)
	// 	}
	// 	if flag1 == true {
	// 		tryPushDown(subWhere, pt.Nodes[pos].Left)
	// 	}
	// 	if flag2 == true {
	// 		tryPushDown(subWhere, pt.Nodes[pos].Right)
	// 	}
	// }
}

type ColType int

const (
	//1 for table, 2 for select, 3 for projuection, 4 for join, 5 for union
	AttrType  ColType = 1
	ValueType ColType = 2
)

func GetCondition(expr *ast.BinaryOperationExpr) (ColType, ColType,
	ast.ColumnNameExpr, ast.ColumnNameExpr,
	test_driver.ValueExpr, test_driver.ValueExpr,
	error) {
	var left ColType
	var right ColType
	var left_attr ast.ColumnNameExpr
	var right_attr ast.ColumnNameExpr
	var left_val test_driver.ValueExpr
	var right_val test_driver.ValueExpr

	switch x := expr.R.(type) {
	case *ast.ColumnNameExpr:
		right = AttrType
		right_attr = *x
	case *test_driver.ValueExpr:
		right = ValueType
		right_val = *x
	default:
		return left, right, left_attr, right_attr, left_val, right_val, errors.New("fail to type cast into BinaryOperationExpr")
	}

	switch x := expr.L.(type) {
	case *ast.ColumnNameExpr:
		left = AttrType
		left_attr = *x
	case *test_driver.ValueExpr:
		left = ValueType
		left_val = *x
	default:
		return left, right, left_attr, right_attr, left_val, right_val, errors.New("fail to type cast into BinaryOperationExpr")
	}
	return left, right, left_attr, right_attr, left_val, right_val, nil
}

func PushDownPredicates(ctx Context, frag_node *plan.PlanTreeNode, where *plan.PlanTreeNode) (err error) {
	for index_, cond_ := range where.Conditions {
		expr, ok := cond_.(*ast.BinaryOperationExpr)
		if !ok {
			return errors.New("fail to type cast into BinaryOperationExpr")
		}
		left, right, left_attr, _, _, _, err := GetCondition(expr)
		if err != nil {
			return err
		}
		if left == AttrType && right == AttrType {
			continue
		} else if left == AttrType && right == ValueType {
			cur_table_name := strings.ToUpper(left_attr.Name.Table.String())
			if strings.Contains(frag_node.FromTableName, cur_table_name) {
				frag_node.Conditions = append(frag_node.Conditions, expr)
				fmt.Println("Condition [" + strconv.FormatInt(int64(index_), 10) + "] adds to " + frag_node.FromTableName)
			}

		} else {
			return errors.New("not support")
		}
	}
	return nil
}

type DataRange struct {
	FieldType string
	LValueStr string
	RValueStr string
	//
	LValueInt int
	RValueInt int
}

func RetureType(table_metas TableMetas, table_name string, col_name string) (string, error) {
	for _, table := range table_metas.TableMetas {
		if strings.Contains(table_name, table.TableName) {
			for _, col := range table.Columns {
				if strings.EqualFold(col.ColumnName, col_name) {
					return col.Type, nil
				}
			}
		}
	}
	return "", errors.New("not find this col" + col_name + table_name)
}

func GetPredicatePruning(ctx Context, frag_node *plan.PlanTreeNode) error {
	// get single table pruned
	condition_range := make(map[string]DataRange)

	// len := len(frag_node.Conditions)
	// for i := 0; i < len; i++ {
	for _, cond_ := range frag_node.Conditions {
		expr, ok := cond_.(*ast.BinaryOperationExpr)
		if !ok {
			return errors.New("fail to type cast into BinaryOperationExpr")
		}
		left, right, left_attr, _, _, right_val, err := GetCondition(expr)
		if err != nil {
			return err
		}

		if left == AttrType && right == AttrType {
			continue
		} else if left == AttrType && right == ValueType {
			attr_num := left_attr.Name.Name.String()
			var new_range DataRange
			type_, err := RetureType(ctx.table_metas, left_attr.Name.Table.String(), left_attr.Name.Name.String())
			if err != nil {
				return nil
			}
			new_range.FieldType = type_

			switch expr.Op {
			case opcode.EQ:
				if type_ == "string" {
					new_range.LValueStr = right_val.GetDatumString()
					new_range.LValueStr = right_val.GetDatumString()
				} else {
					new_range.LValueInt, _ = strconv.Atoi(right_val.Text())
					new_range.LValueInt, _ = strconv.Atoi(right_val.Text())
				}
			case opcode.LT:
				if type_ == "string" {
					new_range.LValueStr = ""
					new_range.RValueStr = right_val.GetDatumString()
				} else {
					new_range.LValueInt = MinInt
					new_range.RValueInt, _ = strconv.Atoi(right_val.Text())
				}
			case opcode.GE, opcode.GT:
				if type_ == "string" {
					new_range.LValueStr = right_val.GetDatumString()
					new_range.RValueStr = ""
				} else {
					new_range.LValueInt, _ = strconv.Atoi(right_val.Text())
					new_range.RValueInt = MaxInt
				}
			default:
				return errors.New("fail to support cur expr op")
			}
			_, ok = condition_range[attr_num]
			if !ok {
				//
				condition_range[attr_num] = new_range
			} else {
				// exists, check if overlap
				cur_range := condition_range[attr_num]
				if expr.Op == opcode.EQ {
					if type_ == "string" {
						if cur_range.LValueStr != new_range.LValueStr {
							frag_node.IsPruned = true
							fmt.Println("frag_node " + frag_node.FromTableName + " IS PRUNED")
							break
						}
					} else {
						if cur_range.LValueInt != new_range.LValueInt {
							frag_node.IsPruned = true
							fmt.Println("frag_node " + frag_node.FromTableName + " IS PRUNED")
							break
						}
					}
				} else {
					// [L,    R]
					//    [L,R]
					if type_ == "string" {
						if condition_range[attr_num].LValueStr == "" {
							cur_range.LValueStr = new_range.LValueStr
						} else {
							if cur_range.LValueStr < new_range.LValueStr {
								cur_range.LValueStr = new_range.LValueStr
							}
						}
						if condition_range[attr_num].RValueStr == "" {
							cur_range.RValueStr = new_range.RValueStr
						} else {
							if new_range.RValueStr < cur_range.RValueStr {
								cur_range.RValueStr = new_range.RValueStr
							}
						}
						if cur_range.LValueStr != "" && cur_range.RValueStr != "" && cur_range.LValueStr > cur_range.RValueStr {
							frag_node.IsPruned = true
							fmt.Println("frag_node " + frag_node.FromTableName + " IS PRUNED")
							break
						}
					} else {
						if cur_range.LValueInt < new_range.LValueInt {
							cur_range.LValueInt = new_range.LValueInt
						}
						if new_range.RValueInt < cur_range.RValueInt {
							cur_range.RValueInt = new_range.RValueInt
						}
						if cur_range.LValueInt > cur_range.RValueInt {
							frag_node.IsPruned = true
							fmt.Println("frag_node " + frag_node.FromTableName + " IS PRUNED")
							break
						}
					}
				}
			}

		} else {
			return errors.New("not support")
		}

	}
	return nil
}

func SelectionPushDown(ctx Context, from *plan.PlanTreeNode, where *plan.PlanTreeNode) error {

	cur_ptr := from
	for {
		child_num := cur_ptr.GetChildrenNum()
		if child_num == 1 && cur_ptr.FromTableName == "" {
			cur_ptr = cur_ptr.GetChild(0)
			continue
		}

		if cur_ptr.FromTableName == "" {
			//
			// JionType[] <- cur_ptr
			// UnionType[ORDERS] UnionType[PUBLISHER] <- cur_table_ptr
			// DataSourceType[ORDERS.1] DataSourceType[ORDERS.2] DataSourceType[ORDERS.3] DataSourceType[ORDERS.4] DataSourceType[PUBLISHER.1] DataSourceType[PUBLISHER.2] DataSourceType[PUBLISHER.3] DataSourceType[PUBLISHER.4]
			child_num = cur_ptr.GetChildrenNum()
			for i := 0; i < child_num; i++ {
				cur_table_ptr := cur_ptr.GetChild(i)
				frag_num := cur_table_ptr.GetChildrenNum()
				for j := 0; j < frag_num; j++ {
					frag_ptr := cur_table_ptr.GetChild(j)
					err := PushDownPredicates(ctx, frag_ptr, where)
					if err != nil {
						return err
					}

					err = GetPredicatePruning(ctx, frag_ptr)
					if err != nil {
						return err
					}
				}
			}

			break
		} else {
			// UnionType[ORDERS]
			// DataSourceType[ORDERS.1] DataSourceType[ORDERS.2] DataSourceType[ORDERS.3]
			child_num = cur_ptr.GetChildrenNum()

			break
		}

	}
	return nil
	// for i := 0; i < nums; i++ {
	// 	child_ := from.GetChild(i)
	// 	if child_.Type == plan.UnionType || child_.Type == plan.JoinType || child_.Type == plan.DataSourceType {
	// 		//
	// 		SplitFragTable_(ctx, child_)
	// 	} else {
	// 		// SelectionPushDown(ctx, child_)
	// 	}
	// }
	// nums := p.GetChildrenNum()
	// for i := 0; i < nums; i++ {
	// 	child_ := p.GetChild(i)
	// 	if child_.Type == plan.SelectType {
	// 		//按照and分割where子句
	// 		//方法：先按照空格分割，然后检测and来组合
	// 		wheres := p.Conditions
	// 		for _, subWhere := range wheres {
	// 			sub_where_string = "where " + subWhere.Text()
	// 			tryPushDown(subWhere, node.Nodeid)
	// 		}
	// 		deleteWhereNode(node.Nodeid)
	// 	}
	// }
	// for _, node := range pt.Nodes {
	// 	if node.NodeType == 2 {
	// 		//按照and分割where子句
	// 		//方法：先按照空格分割，然后检测and来组合
	// 		wheres := strings.Split(node.Where, "and")
	// 		for _, subWhere := range wheres {
	// 			subWhere = "where " + subWhere
	// 			tryPushDown(subWhere, node.Nodeid)
	// 		}
	// 		deleteWhereNode(node.Nodeid)
	// 	}

	// }

	// for i, node := range pt.Nodes {
	// 	if node.NodeType == 1 && node.TransferFlag == false {
	// 		pt.Nodes[i].Status = 1
	// 	} else {
	// 		pt.Nodes[i].Status = 0
	// 	}
	// }
}

func HandleSelect(ctx Context, sel *ast.SelectStmt) (p *plan.PlanTreeNode, err error) {
	// generate Logical Plan tree
	//            	   Proj.
	//                  |
	//                 Sel.
	//                  |
	//                 Join
	//                 /  \
	//              Join   Dat.
	//              /  \
	//            Dat.  Dat.

	var from *plan.PlanTreeNode
	var where *plan.PlanTreeNode
	var proj *plan.PlanTreeNode

	if sel.From != nil {
		from, err = buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Can't parse sql without From.")
	}
	// TODO unfold wildstar in `projection`
	// originalFields := sel.Fields.Fields
	// sel.Fields.Fields, err = unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}

	if sel.Where != nil {
		where, err = buildSelection(ctx, sel.Where)
		if err != nil {
			return nil, err
		}
	}

	proj, err = buildProjection(ctx, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}

	fmt.Println(PrintPlanTree(from))
	fmt.Println(PrintPlanTree(where))
	fmt.Println(PrintPlanTree(proj))

	SplitFragTable(ctx, from)

	fmt.Println(PrintPlanTree(from))

	SelectionPushDown(ctx, from, where)

	fmt.Println(PrintPlanTree(from))

	return p, err
}

// buildProjection returns a Projection plan and non-aux columns length.
func buildProjection(ctx Context, fields []*ast.SelectField) (*plan.PlanTreeNode, error) {
	proj := plan.PlanTreeNode{
		Type: plan.ProjectionType,
	}.Init()
	for _, field := range fields {
		proj.ColsName = append(proj.ColsName, field.Expr.(*ast.ColumnNameExpr).Name.String())
	}
	// schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)

	// proj.SetChildren(p)
	return proj, nil
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func buildSelection(ctx Context, where ast.ExprNode) (*plan.PlanTreeNode, error) {

	conditions := splitWhere(where)
	// expressions := make([]expression.Expression, 0, len(conditions))
	selection := plan.PlanTreeNode{
		Type: plan.SelectType,
	}.Init()

	selection.Conditions = conditions
	// selection.SetChildren(p)
	return selection, nil
}

func buildJoin(ctx Context, joinNode *ast.Join) (*plan.PlanTreeNode, error) {

	//新建一个队列
	queue := []*ast.Join{joinNode}

	joinPlan := plan.PlanTreeNode{Type: plan.JoinType}.Init()

	i := 0
	for len(queue) > 0 {
		//新建临时队列，用于重新给queue赋值
		temp := []*ast.Join{}
		//新建每一行的一维数组

		for _, v := range queue {
			//
			if v.Right == nil {
				p, err := buildResultSetNode(ctx, v.Left)
				if err != nil {
					return nil, err
				}
				joinPlan.AddChild(p)
				continue
			}
			switch x := v.Right.(type) {
			case *ast.Join:
				temp = append(temp, x)
			case *ast.TableSource:
				p, err := buildResultSetNode(ctx, x)
				if err != nil {
					return nil, err
				}
				joinPlan.AddChild(p)
			default:
				return nil, errors.New("hello,error")
			}

			switch x := v.Left.(type) {
			case *ast.Join:
				temp = append(temp, x)
			case *ast.TableSource:
				p, err := buildResultSetNode(ctx, x)
				if err != nil {
					return nil, err
				}
				joinPlan.AddChild(p)
			default:
				return nil, errors.New("hello,error")
			}
		}
		i++
		//二叉树新的一行的节点放入队列中
		queue = temp
	}
	// return result

	// if joinNode.Right == nil {
	// 	return buildResultSetNode(ctx, joinNode.Left)
	// }

	// leftPlan, err := buildResultSetNode(ctx, joinNode.Left)
	// if err != nil {
	// 	return nil, err
	// }

	// rightPlan, err := buildResultSetNode(ctx, joinNode.Right)
	// if err != nil {
	// 	return nil, err
	// }

	// joinPlan.SetChildren(leftPlan, rightPlan)
	// TODO set schema and join node NAME
	// joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	// joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	// copy(joinPlan.names, leftPlan.OutputNames())
	// copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())
	return joinPlan, nil
}

func buildDataSource(ctx Context, tn *ast.TableName) (*plan.PlanTreeNode, error) {
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

func buildResultSetNode(ctx Context, node ast.ResultSetNode) (p *plan.PlanTreeNode, err error) {
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

func HandlePartitionSQL(sql_str string) (Partition, error) {
	// type Partition struct {
	// 	TableName  string      `json:"table_name"`
	// 	SiteInfos  []SiteInfo  `json:"site_info"`
	// 	HFragInfos []HFragInfo `json:"horizontal_fragmentation"`
	// 	VFragInfos []VFragInfo `json:"vertical_fragmentation"`
	// }
	var partition Partition
	var err error

	partition.TableName = GetMidStr(sql_str, "|", "|")

	frag_type := GetMidStr(sql_str, "[", "]")
	site_ips := strings.Split(GetMidStr(sql_str, "(", ")"), ",")
	site_details := strings.Split(GetMidStr(sql_str, "{", "}"), ";")
	var site_names []string
	var site_conds []string

	if len(site_ips) != len(site_details) {
		err = errors.New("len(site_ips) != len(site_details) ")
		return partition, err
	}

	for index_, _ := range site_ips {
		var site_info SiteInfo
		site_info.IP = site_ips[index_]

		site_detail := strings.Split(site_details[index_], ":")
		site_info.Name = strings.Trim(strings.Trim(site_detail[0], "\""), "'")

		site_names = append(site_names, site_info.Name)
		site_conds = append(site_conds, site_detail[1])

		partition.SiteInfos = append(partition.SiteInfos, site_info)
	}

	partition.FragType = frag_type

	if strings.EqualFold(frag_type, "HORIZONTAL") {
		// create partition on |PUBLISHER| [horizontal]
		// at (10.77.110.145, 10.77.110.146, 10.77.110.145, 10.77.110.146)
		// where {
		//  "PUBLISHER.1" : ID < 104000 and NATION = 'PRC';
		//  "PUBLISHER.2" : ID < 104000 and NATION = 'USA';
		//  "PUBLISHER.3" : ID >= 104000 and NATION = 'PRC';
		//  "PUBLISHER.4" : ID >= 104000 and NATION = 'USA';
		// };
		//
		for index_, _ := range site_ips {
			var cur_frag HFragInfo
			cur_frag.FragName = site_names[index_]
			// for _, cond_ := range site_conds {
			cond_ := site_conds[index_]
			// type ConditionRange struct {
			// 	ColName          string `json:"col_name"`
			// 	GreaterEqualThan string `json:"get"`
			// 	LessThan         string `json:"lt"`
			// 	Equal            string `json:"eq"`
			// }

			logis_ := strings.Split(cond_, "AND")
			for _, logi_ := range logis_ {
				var new_cond ConditionRange
				var attr_and_value []string
				// attr_and_value[0] attr
				// attr_and_value[1] value
				// only allow left be attr and right be value

				if strings.Contains(logi_, "<=") {
					attr_and_value = strings.Split(logi_, "<=")
					err = errors.New("not support")
					break
				} else if strings.Contains(logi_, ">=") {
					attr_and_value = strings.Split(logi_, ">=")
					new_cond.GreaterEqualThan = strings.Trim(attr_and_value[1], " ")
				} else if strings.Contains(logi_, "=") {
					attr_and_value = strings.Split(logi_, "=")
					new_cond.Equal = strings.Trim(attr_and_value[1], " ")
				} else if strings.Contains(logi_, ">") {
					attr_and_value = strings.Split(logi_, ">")
					err = errors.New("not support")
					break
				} else if strings.Contains(logi_, "<") {
					attr_and_value = strings.Split(logi_, "<")
					new_cond.LessThan = strings.Trim(attr_and_value[1], " ")
				} else {
					err = errors.New("Dont exist operation in condition " + logi_)
					break
				}
				new_cond.ColName = strings.Trim(attr_and_value[0], " ")
				cur_frag.Conditions = append(cur_frag.Conditions, new_cond)
			}
			partition.HFragInfos = append(partition.HFragInfos, cur_frag)
			// }

		}
	} else {
		// create partition on |CUSTOMER| [vertical]
		// at (10.77.110.145, 10.77.110.146)
		// where {
		//  "CUSTOMER.1" : ID, NAME;
		//  "CUSTOMER.2" : ID, rank;
		// };
		//
		for index_, _ := range site_ips {
			var cur_frag VFragInfo
			cond_ := site_conds[index_]
			cur_frag.SiteName = site_names[index_]
			//
			cols_ := strings.Split(cond_, ",")
			for _, col_ := range cols_ {
				col_ = strings.Trim(col_, " ")
				cur_frag.ColumnName = append(cur_frag.ColumnName, col_)
			}
			partition.VFragInfos = append(partition.VFragInfos, cur_frag)
		}
	}
	fmt.Println(partition)
	return partition, err
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
	///////

	var g_ctx cfg.Context
	utils.ParseArgs(&g_ctx)
	// init log level, log file...
	logger.LoggerInit(&g_ctx)
	// a test connection to db engine
	mysql.SQLDriverInit(&g_ctx)
	// start coordinator <worker, socket_input, socket_dispatcher>
	c := core.NewCoordinator(&g_ctx)

	ctx := Context{
		partitions:  partitions,
		table_metas: table_metas,
	}

	// parser and hanlder insert and select
	my_parser := parser.New()
	// sql_str := "insert into publisher values(200000, 'hello world');"

	// sql_str := "create table customer (ID int, NAME varchar(255), RANK_ int);"
	// sql_str := "drop table customer;"

	// stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.Id = b.Id", "", "")
	// stmt, _ := my_parser.ParseOneStmt("select test.a, test2.b from test, test2 where test.a >= 2 and test2.b < 30;", "", "")

	sql_strs := []string{
		// "create table publisher (ID int, NAME varchar(255), NATION varchar(255));",
		// "create table customer (ID int, NAME varchar(255), RANK_ int);",
		// "insert into publisher values(103999, 'zzq', 'PRC');",
		// "insert into publisher values(103999, 'zzq2', 'USA');",
		// "insert into publisher values(104000, 'aa1', 'PRC');",
		// "insert into publisher values(104000, 'dss2', 'USA');",
		// "insert into customer values(20000, 'hello world', 2);",
		// "insert into customer values(20000, 'hello world', 2);",
		// "drop table customer;",
		// `create partition on |PUBLISHER| [horizontal]
		// 	at (10.77.110.145, 10.77.110.146, 10.77.110.145, 10.77.110.146)
		// 	where {
		// 	 "PUBLISHER.1" : ID < 104000 and NATION = 'PRC';
		// 	 "PUBLISHER.2" : ID < 104000 and NATION = 'USA';
		// 	 "PUBLISHER.3" : ID >= 104000 and NATION = 'PRC';
		// 	 "PUBLISHER.4" : ID >= 104000 and NATION = 'USA'
		// 	};`,
		// `create partition on |CUSTOMER| [vertical]
		// 	at (10.77.110.145, 10.77.110.146)
		// 	where {
		// 	"CUSTOMER.1" : ID, NAME;
		// 	"CUSTOMER.2" : ID, rank
		// 	};`,
		// "select * from Customer;",
		// "select Publisher.name from Publisher;",
		// `select Customer.name,Orders.quantity
		// from Customer,Orders
		// where Customer.id=Orders.customer_id`,
		// ` select Book.title,Book.copies,
		//   Publisher.name,Publisher.nation
		//   from Book,Publisher
		//   where Book.publisher_id=Publisher.id
		//   and Publisher.nation='USA'
		//   and Book.copies > 1000`,
		// `select Customer.name, Book.title, Publisher.name, Orders.quantity
		// from Customer, Book, Publisher, Orders
		// where
		// Customer.id=Orders.customer_id
		// and Book.id=Orders.book_id
		// and Book.publisher_id=Publisher.id
		// and Customer.id>308000
		// and Book.copies>100
		// and Orders.quantity>1
		// and Publisher.nation='PRC'
		// `,
		`select Customer.name, Book.title,   
		Publisher.name, Orders.quantity 
		from Customer, Book, Publisher, 
		Orders 
		where 
		Customer.id=Orders.customer_id 
		and Book.id=Orders.book_id 
		and Book.publisher_id=Publisher.id 
		and Customer.id>308000 
		and Book.copies>100 
		and Orders.quantity>1 
		and Publisher.nation='PRC'`,
	}

	for _, sql_str := range sql_strs {
		sql_str = strings.ToUpper(sql_str)
		sql_str = strings.Replace(sql_str, "\t", " ", -1)
		sql_str = strings.Replace(sql_str, "\n", " ", -1)

		fmt.Println(sql_str)
		if strings.Contains(sql_str, "PARTITION") {
			sql_str = strings.Replace(sql_str, " ", "", -1)
			_, err := HandlePartitionSQL(sql_str)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			stmt, err := my_parser.ParseOneStmt(sql_str, "", "")
			if err != nil {
				fmt.Println(err)
			}

			// Otherwise do something with stmt
			switch x := stmt.(type) {
			case *ast.SelectStmt:
				fmt.Println("Select")
				_, _ = HandleSelect(ctx, x)
			case *ast.InsertStmt:
				fmt.Println("Insert") // same as delete
				HandleInsert(ctx, x)
			case *ast.CreateTableStmt:
				fmt.Println("create table") // same as delete
				HandleCreateTable(ctx, x)
			case *ast.DropTableStmt:
				HandleDropTable(ctx, x)
			case *ast.DeleteStmt:
				fmt.Println("delete") // same as delete
				HandleDelete(ctx, x)
			default:
				// createdb, dropdb, create table, drop table, all broadcast
				BroadcastSQL(ctx, c, x)
			}
		}
	}
}

// func ParseAndExecute(c *core.Coordinator, sql_str string) (*plan.PlanTreeNode, []SqlRouter, error) {
// 	var p *plan.PlanTreeNode
// 	var ret []SqlRouter
// 	var err error

// 	my_parser := parser.New()

// 	sql_str = strings.ToUpper(sql_str)
// 	sql_str = strings.Replace(sql_str, "\t", "", -1)
// 	sql_str = strings.Replace(sql_str, "\n", "", -1)

// 	fmt.Println(sql_str)
// 	if strings.Contains(sql_str, "PARTITION") {
// 		sql_str = strings.Replace(sql_str, " ", "", -1)
// 		_, err := HandlePartitionSQL(sql_str)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	} else {
// 		stmt, err := my_parser.ParseOneStmt(sql_str, "", "")
// 		if err != nil {
// 			fmt.Println(err)
// 		}

// 		// Otherwise do something with stmt
// 		switch x := stmt.(type) {
// 		case *ast.SelectStmt:
// 			fmt.Println("Select")
// 			p, err = HandleSelect(c, x)
// 		case *ast.InsertStmt:
// 			fmt.Println("Insert") // same as delete
// 			ret, err = HandleInsert(c, x)
// 		case *ast.CreateTableStmt:
// 			fmt.Println("create table") // same as delete
// 			ret, err = HandleCreateTable(c, x)
// 		case *ast.DropTableStmt:
// 			ret, err = HandleDropTable(c, x)
// 		case *ast.DeleteStmt:
// 			fmt.Println("delete") // same as delete
// 			ret, err = HandleDelete(c, x)
// 		default:
// 			// createdb, dropdb, create table, drop table, all broadcast
// 			ret, err = BroadcastSQL(c, x)
// 		}
// 	}
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	return p, ret, err

// }
