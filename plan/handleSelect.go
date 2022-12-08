package plan

import (
	"errors"
	"fmt"
	"project/meta"
	"strings"

	"github.com/pingcap/tidb/parser/ast"

	"github.com/pingcap/tidb/parser/opcode"
)

func unfoldWildStar(ctx meta.Context, fields []*ast.SelectField, from *PlanTreeNode) ([]*ast.SelectField, error) {
	var ret []*ast.SelectField
	var err error

	for _, field_ := range fields {
		if field_.WildCard != nil {
			table_meta, _, err := FindMetaInfo(ctx, from.GetChild(0).FromTableName)
			if err != nil {
				return ret, err
			}

			for _, col_ := range table_meta.Columns {
				var new_field ast.SelectField
				var new_expr ast.ColumnNameExpr
				var name ast.ColumnName

				name.Name.O = col_.ColumnName
				name.Table.O = table_meta.TableName

				new_expr.Name = &name
				new_field.Expr = &new_expr

				ret = append(ret, &new_field)
			}
			// fmt.Println(field_.WildCard.Table)

			// from.FromTableName
		} else {
			ret = append(ret, field_)
		}
	}

	return ret, err
}

// buildProjection returns a Projection plan and non-aux columns length.
func buildProjection(ctx meta.Context, fields []*ast.SelectField, from *PlanTreeNode) (*PlanTreeNode, error) {
	proj := PlanTreeNode{
		Type: ProjectionType,
	}.Init()
	for _, field := range fields {
		var proj_name string

		if field.Expr.(*ast.ColumnNameExpr).Name.Table.String() == "" {
			proj_name = from.GetChild(0).FromTableName + "." + strings.ToUpper(field.Expr.(*ast.ColumnNameExpr).Name.Name.String())
		} else {
			proj_name = strings.ToUpper(field.Expr.(*ast.ColumnNameExpr).Name.Table.String()) + "." + strings.ToUpper(field.Expr.(*ast.ColumnNameExpr).Name.Name.String())
		}

		proj.ColsName = append(proj.ColsName, proj_name)
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

func buildSelection(ctx meta.Context, where ast.ExprNode) (*PlanTreeNode, error) {

	conditions := splitWhere(where)
	// expressions := make([]expression.Expression, 0, len(conditions))
	selection := PlanTreeNode{
		Type: SelectType,
	}.Init()

	selection.Conditions = conditions
	// selection.SetChildren(p)
	return selection, nil
}

func buildJoin(ctx meta.Context, joinNode *ast.Join) (*PlanTreeNode, error) {

	//新建一个队列
	queue := []*ast.Join{joinNode}

	joinPlan := PlanTreeNode{Type: JoinType}.Init()

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
	return joinPlan, nil
}

func buildDataSource(ctx meta.Context, tn *ast.TableName) (*PlanTreeNode, error) {
	// dbName := tn.Schema
	// TODO check table in this db
	// tbl, err := b.is.TableByName(dbName, tn.Name)
	// if err != nil {
	// 	return nil, err
	// }

	result := PlanTreeNode{
		Type:          DataSourceType,
		FromTableName: tn.Name.String(),
	}.Init()

	return result, nil
}

func buildResultSetNode(ctx meta.Context, node ast.ResultSetNode) (p *PlanTreeNode, err error) {
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

func HandleSelect(ctx meta.Context, sel *ast.SelectStmt) (p *PlanTreeNode, err error) {
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

	var from *PlanTreeNode
	var where *PlanTreeNode
	var proj *PlanTreeNode

	if sel.From != nil {
		from, err = buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("can not parse sql without from")
	}

	sel.Fields.Fields, err = unfoldWildStar(ctx, sel.Fields.Fields, from)
	if err != nil {
		return nil, err
	}

	if sel.Where != nil {
		where, err = buildSelection(ctx, sel.Where)
		if err != nil {
			return nil, err
		}
	}

	proj, err = buildProjection(ctx, sel.Fields.Fields, from)
	if err != nil {
		return nil, err
	}

	fmt.Println(PrintPlanTree(from))
	fmt.Println(PrintPlanTree(where))
	fmt.Println(PrintPlanTree(proj))

	SplitFragTable(ctx, from)

	fmt.Println(PrintPlanTree(from))

	SelectionAndProjectionPushDown(ctx, from, where, proj)

	fmt.Println(PrintPlanTree(from))
	PrunedNodeName, UnPrunedNodeName, err := PredicatePruning(ctx, from, where)
	if err != nil {
		return p, err
	}
	fmt.Println(PrunedNodeName)
	fmt.Println("==================")
	fmt.Println(UnPrunedNodeName)
	fmt.Println("==================")
	new_joined_tree, err := JoinUsingPruning(ctx, from, where, PrunedNodeName, UnPrunedNodeName)
	if err != nil {
		return p, err
	}

	err = AddProjectionAndSelectionNode(ctx, new_joined_tree, 0, nil)
	if err != nil {
		return p, err
	}

	cur_index := 0
	SetNodeId(ctx, new_joined_tree, &cur_index)

	OptimizeTransmission(ctx, new_joined_tree)

	// todo test
	if new_joined_tree != nil && new_joined_tree.GetChildrenNum() != 0 {
		new_joined_tree = new_joined_tree.GetChild(0)
	}
	//
	return new_joined_tree, err
}

func SetNodeId(ctx meta.Context, from *PlanTreeNode, cur_index *int) error {
	var err error

	from.NodeId = *cur_index
	*cur_index += 1

	if from.GetChildrenNum() == 0 {

	} else {
		for i := 0; i < from.GetChildrenNum(); i++ {

			err := SetNodeId(ctx, from.GetChild(i), cur_index)
			if err != nil {
				return err
			}
		}
	}
	return err
}
