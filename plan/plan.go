package plan

import (
	"github.com/pingcap/tidb/parser/ast"
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

// 	// SetChildren sets the children for the plan.
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
	Conditions []ast.ExprNode

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

func (p PlanTreeNode) GetChild(index int) *PlanTreeNode {
	return p.children[index]
}

func (p *PlanTreeNode) AddChild(new_child *PlanTreeNode) {
	p.children = append(p.children, new_child)
}

func (p *PlanTreeNode) RemoveChild(index int) {
	p.children = append(p.children[:index], p.children[index+1:]...)
}
