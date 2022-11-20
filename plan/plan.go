package plan

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"project/etcd"
	"project/meta"
	"strconv"
	"strings"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/test_driver"
)

type NodeType int32

const (
	//1 for table, 2 for select, 3 for projuection, 4 for join, 5 for union
	DataSourceType NodeType = 1
	SelectType     NodeType = 2
	ProjectionType NodeType = 3
	JoinType       NodeType = 4
	UnionType      NodeType = 5
)

func (s NodeType) String() string {
	switch s {
	case DataSourceType:
		return "DataSourceType"
	case SelectType:
		return "SelectType"
	case ProjectionType:
		return "ProjectionType"
	case JoinType:
		return "JoinType"
	case UnionType:
		return "UnionType"
	default:
		return "Unknown"
	}
}

// type BasePlan interface {

// 	// SetChildren sets the children for the
// 	SetChildren(...BasePlan)
// 	GetChild(index int) BasePlan
// 	GetChildrenNum() int
// }

type PlanTreeNode struct {
	// NodeId int
	// self     BasePlan
	children []*PlanTreeNode
	Type     NodeType

	// DataSourceType
	Status           int    // table row count?
	DestCoordintorId int    // fragment
	FromTableName    string // valid if NodeType = DataSource
	// SelectType
	Conditions    []ast.ExprNode
	ConditionsStr []string

	// ProjectionType
	ColsName []string

	IsPruned      bool
	ExecuteSiteIP string
	DestSiteIP    string
}

func (p PlanTreeNode) Init() *PlanTreeNode {
	return &p
}

func (p *PlanTreeNode) SetChildren(children ...*PlanTreeNode) {
	p.children = children
}

func (p PlanTreeNode) GetChildrenNum() int {
	return len(p.children)
}

func (p *PlanTreeNode) GetChild(index int) *PlanTreeNode {
	return p.children[index]
}

func (p *PlanTreeNode) ResetChild(index int, new_ *PlanTreeNode) {
	p.children[index] = new_
}

func (p *PlanTreeNode) AddChild(new_child *PlanTreeNode) {
	p.children = append(p.children, new_child)
}

func (p *PlanTreeNode) RemoveChild(index int) {
	p.children = append(p.children[:index], p.children[index+1:]...)
}

func (p *PlanTreeNode) RemoveAllChild() {
	len := p.GetChildrenNum()
	for i := 0; i < len; i++ {
		p.RemoveChild(0)
	}
}

/************************************************************/

type InsertRequest struct {
	Siteinfo     meta.SiteInfo
	InsertValues []InsertValue
}

type InsertValue struct {
	ColName string
	Val     string
}

type DataRange struct {
	FieldType string
	LValueStr string
	RValueStr string
	//
	LValueInt int
	RValueInt int
}

type ColType int

const (
	//1 for table, 2 for select, 3 for projuection, 4 for join, 5 for union
	AttrType  ColType = 1
	ValueType ColType = 2
)

func FindMetaInfo(ctx meta.Context, tablename string) (meta.TableMeta, meta.Partition, error) {
	var table_meta meta.TableMeta
	var partition_meta meta.Partition
	var err error

	// find metaData
	is_find_ := false
	// find PartitionMetaData
	for _, element := range ctx.TablePartitions.Partitions {
		if strings.EqualFold(element.TableName, tablename) {
			partition_meta = element
			is_find_ = true
			break
		}
	}
	if !is_find_ {
		err = errors.New("fail to find partition info about " + tablename + " in current database")
		return table_meta, partition_meta, err
	}
	for _, element := range ctx.TableMetas.TableMetas {
		if strings.EqualFold(element.TableName, tablename) {
			table_meta = element
			is_find_ = true
			break
		}
	}
	if !is_find_ {
		err = errors.New("fail to find table" + tablename + " in current database")
		return table_meta, partition_meta, err
	}

	return table_meta, partition_meta, err
}

func SwapNode(a *PlanTreeNode, b *PlanTreeNode) {
	tem := *a
	*a = *b
	*b = tem
}

func ReturnFragType(ctx meta.Context, table_name string) string {
	var ret string
	for _, partition := range ctx.TablePartitions.Partitions {
		if strings.EqualFold(partition.TableName, table_name) {
			//
			ret = partition.FragType
			break
		}
	}
	return ret
}

func FindMainTableNode(ctx meta.Context, from *PlanTreeNode, table_name string) (int, *PlanTreeNode) {
	var ret *PlanTreeNode
	ret = nil
	index_ret := -1
	cur_ptr := from
	for {
		child_num := cur_ptr.GetChildrenNum()
		if child_num == 1 && cur_ptr.FromTableName == "" {
			cur_ptr = cur_ptr.GetChild(0)
			continue
		}

		if cur_ptr.FromTableName == "" {
			child_num = cur_ptr.GetChildrenNum()
			for i := 0; i < child_num; i++ {
				cur_table_ptr := cur_ptr.GetChild(i)
				if strings.Contains(cur_table_ptr.FromTableName, strings.ToUpper(table_name)) {
					ret = cur_table_ptr
					index_ret = i
					break
				}
			}
			break
		} else {
			break
		}
	}

	return index_ret, ret
}

func RetureType(table_metas meta.TableMetas, table_name string, col_name string) (string, error) {
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

func GetFragType(ctx meta.Context, frag_name string) (string, error) {
	var str string
	var err error
	//
	for _, partition := range ctx.TablePartitions.Partitions {
		if strings.Contains(frag_name, partition.TableName) {
			str = partition.FragType
			break
		}
	}
	if len(str) == 0 {
		err = errors.New("not find frag_name " + frag_name)
	}
	return str, err
}

func PrintPlanTree(p *PlanTreeNode) string {
	var result string
	if p == nil {
		return result
	}
	//新建一个队列
	queue := []*PlanTreeNode{p}

	i := 0
	for len(queue) > 0 {
		//新建临时队列，用于重新给queue赋值
		temp := []*PlanTreeNode{}
		//新建每一行的一维数组

		for _, v := range queue {
			//
			if v.IsPruned {
				continue
			}
			if v.Type == DataSourceType {
				result += v.Type.String() + "[" + v.FromTableName + "]" + " "
			} else if v.Type == JoinType || v.Type == UnionType {
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

func OptimizeTransmission(ctx meta.Context, p *PlanTreeNode) {
	// TODO
	// direct send to current sql
	node_id := 0
	if p == nil {
		return
	}
	//新建一个队列
	queue := []*PlanTreeNode{p}

	i := 0
	for len(queue) > 0 {
		//新建临时队列，用于重新给queue赋值
		temp := []*PlanTreeNode{}
		//新建每一行的一维数组

		for _, v := range queue {
			node_id++

			nums := v.GetChildrenNum()
			if nums == 0 {
				continue
			}
			for i := 0; i < nums; i++ {
				cur_node := v.GetChild(i)
				if cur_node.Type == JoinType || cur_node.Type == UnionType {
					cur_node.ExecuteSiteIP = ctx.IP
					cur_node.DestSiteIP = ctx.IP
				}

				temp = append(temp, cur_node)
			}
		}
		i++
		//二叉树新的一行的节点放入队列中
		queue = temp
	}
	return
}

func PrintPlanTreePlot(p *PlanTreeNode) string {
	edge_id := 0
	node_id := 0
	var result string
	if p == nil {
		return result
	}
	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		fmt.Println("graph error")
	}

	//新建一个队列
	queue := []*PlanTreeNode{p}

	n, err := graph.CreateNode(p.Type.String())
	if err != nil {
		return result
	}

	graphvis_queue := []*cgraph.Node{n}

	i := 0
	for len(queue) > 0 {
		//新建临时队列，用于重新给queue赋值
		temp := []*PlanTreeNode{}
		graphvis_temp := []*cgraph.Node{}
		//新建每一行的一维数组

		for index_, v := range queue {
			node_id++

			nums := v.GetChildrenNum()
			if nums == 0 {
				continue
			}
			for i := 0; i < nums; i++ {
				cur_node := v.GetChild(i)
				var cur_val string
				if cur_node.Type == ProjectionType {
					cur_val = "Proj.["
					for _, col_ := range cur_node.ColsName {
						cur_val += col_ + " "
					}
					cur_val += "]"
				} else if cur_node.Type == SelectType {
					cur_val = "Sel.["
					for _, cond_ := range cur_node.ConditionsStr {
						cur_val += cond_ + " "
					}
					cur_val += "]"
				} else {
					cur_val = cur_node.FromTableName
					if strings.Contains(cur_val, "|") {
						cur_val = cur_node.Type.String()
					}
				}

				cur_node_graphvis, err := graph.CreateNode(strconv.Itoa(node_id) + "_" + cur_val)
				node_id++

				if err != nil {
					fmt.Println("Error")
				}

				e, err := graph.CreateEdge(strconv.Itoa(edge_id), graphvis_queue[index_], cur_node_graphvis)
				e.SetLabel(strconv.Itoa(edge_id))
				edge_id++

				if err != nil {
					fmt.Println("Error")
				}

				temp = append(temp, cur_node)
				graphvis_temp = append(graphvis_temp, cur_node_graphvis)
			}
		}
		result += "\n"
		i++
		//二叉树新的一行的节点放入队列中
		queue = temp
		graphvis_queue = graphvis_temp
	}

	// 1. write encoded PNG data to buffer
	var buf bytes.Buffer
	if err := g.Render(graph, graphviz.PNG, &buf); err != nil {
		log.Fatal(err)
	}

	// 3. write to file directly
	if err := g.RenderFilename(graph, graphviz.PNG, "./file/graph.png"); err != nil {
		log.Fatal(err)
	}

	return result
}

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

func TransExprNode2Str(expr *ast.BinaryOperationExpr) string {
	left, right, left_attr, right_attr, _, right_val, _ := GetCondition(expr)
	var left_str string
	var right_str string

	if left == AttrType && right == ValueType {
		left_str = left_attr.Name.String()
		right_str = right_val.GetDatumString() + strconv.Itoa(int(right_val.GetInt64()))
	} else if left == AttrType && right == AttrType {
		left_str = left_attr.Name.String()
		right_str = right_attr.Name.String()
	} else {
		fmt.Println("not supported")
	}
	return left_str + " " + expr.Op.String() + " " + right_str
}

func ParseAndExecute(ctx meta.Context, sql_str string) (*PlanTreeNode, []meta.SqlRouter, error) {
	var p *PlanTreeNode
	var ret []meta.SqlRouter
	var err error

	if !ctx.IsDebugLocal {
		err := etcd.RefreshContext(&ctx)
		if err != nil {
			return p, ret, err
		}
	}
	my_parser := parser.New()

	sql_str = strings.ToUpper(sql_str)
	sql_str = strings.Replace(sql_str, "\t", "", -1)
	sql_str = strings.Replace(sql_str, "\n", "", -1)

	fmt.Println(sql_str)
	if strings.Contains(sql_str, "PARTITION") {
		sql_str = strings.Replace(sql_str, " ", "", -1)
		partition_infos, err := HandlePartitionSQL(ctx, sql_str)
		if err != nil {
			fmt.Println(err)
		}
		if !ctx.IsDebugLocal {
			err := etcd.SaveFragmenttoEtcd(partition_infos)
			if err != nil {
				fmt.Println(err)
			}
		}
	} else {
		stmt, err1 := my_parser.ParseOneStmt(sql_str, "", "")
		if err1 != nil {
			fmt.Println(err1)
			return p, ret, err1
		}

		// Otherwise do something with stmt
		switch x := stmt.(type) {
		case *ast.SelectStmt:
			fmt.Println("Select")
			p, err = HandleSelect(ctx, x)
		case *ast.InsertStmt:
			fmt.Println("Insert") // same as delete
			ret, err = HandleInsert(ctx, x)
		case *ast.CreateTableStmt:
			fmt.Println("create table") // same as delete
			ret, err = HandleCreateTable(ctx, x)
		case *ast.DropTableStmt:
			ret, err = HandleDropTable(ctx, x)
		case *ast.DeleteStmt:
			fmt.Println("delete") // same as delete
			ret, err = HandleDelete(ctx, x)
		default:
			// createdb, dropdb, all broadcast
			ret, err = BroadcastSQL(ctx, x)
		}
	}
	if err != nil {
		fmt.Println(err)
	}
	return p, ret, err
}
