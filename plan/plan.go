package plan

type NodeType int32

const (
	//1 for table, 2 for select, 3 for projuection, 4 for join, 5 for union
	DataSourceType NodeType = 1
	SelectType     NodeType = 2
	ProjectionType NodeType = 3
	JoinType       NodeType = 4
	UnionType      NodeType = 5
)

type BasePlan interface {

	// SetChildren sets the children for the plan.
	SetChildren(...BasePlan)
}

type PlanTreeNode struct {
	// NodeId int
	// self     BasePlan
	children []BasePlan
	Type     NodeType

	// DataSourceType
	Status           int    // table row count?
	DestCoordintorId int    // fragment
	FromTableName    string // valid if NodeType = DataSource
	// SelectType
	Conditions string

	// ProjectionType
	Cols string
}

func (p PlanTreeNode) Init() *PlanTreeNode {
	return &p
}

func (p *PlanTreeNode) SetChildren(children ...BasePlan) {
	p.children = children
}
