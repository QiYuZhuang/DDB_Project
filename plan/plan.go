package plan

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
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

type SqlRouter struct {
	sql     string
	site_ip string
}

type Context struct {
	TableMetas      TableMetas
	TablePartitions Partitions
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

func FindMetaInfo(ctx Context, tablename string) (TableMeta, Partition, error) {
	var table_meta TableMeta
	var partition_meta Partition
	var err error
	// find metaData
	is_find_ := false
	for _, element := range ctx.TableMetas.TableMetas {
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
	for _, element := range ctx.TablePartitions.Partitions {
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

func SwapNode(a *PlanTreeNode, b *PlanTreeNode) {
	tem := *a
	*a = *b
	*b = tem
}

func ReturnFragType(ctx Context, table_name string) string {
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

func FindMainTableNode(ctx Context, from *PlanTreeNode, table_name string) (int, *PlanTreeNode) {
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

func GetFragType(ctx Context, frag_name string) (string, error) {
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
	if err := g.RenderFilename(graph, graphviz.PNG, "/home/bigdata/Course3-DDB/DDB_Project/file/graph.png"); err != nil {
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
