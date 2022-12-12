package plan

import (
	"errors"
	"fmt"
	meta "project/meta"
	utils "project/utils"
	"sort"
	"strconv"
	"strings"

	mapset "github.com/deckarep/golang-set"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/parser/test_driver"
)

func InitFragWithCondition(frag_info meta.Partition, frag_name string) ([]ast.ExprNode, []string) {
	// put frag condition into table-node
	var conds_ []ast.ExprNode
	var cols_ []string

	if strings.EqualFold(frag_info.FragType, "HORIZONTAL") {
		for _, frag_ := range frag_info.HFragInfos {
			if strings.EqualFold(frag_.FragName, frag_name) {
				for _, cond_ := range frag_.Conditions {
					var new_expr_node ast.BinaryOperationExpr
					var col_attr_node ast.ColumnNameExpr
					var name ast.ColumnName
					name.Table.O = strings.Split(frag_.FragName, ".")[0] // without frag_id
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

							val_int, _ := strconv.Atoi(cond_.LessThan)
							val_attr_node.Datum.SetInt64(int64(val_int))
							new_expr_node_.R = &val_attr_node

							conds_ = append(conds_, &new_expr_node_)
						}
						if cond_.GreaterEqualThan != "" {
							new_expr_node_ := new_expr_node
							new_expr_node_.Op = opcode.GE

							var val_attr_node test_driver.ValueExpr
							val_attr_node.Datum.SetString(cond_.GreaterEqualThan)

							val_int, _ := strconv.Atoi(cond_.GreaterEqualThan)
							val_attr_node.Datum.SetInt64(int64(val_int))
							new_expr_node_.R = &val_attr_node

							conds_ = append(conds_, &new_expr_node_)
						}
					} else {
						// ==
						new_expr_node_ := new_expr_node
						new_expr_node_.Op = opcode.EQ

						var val_attr_node test_driver.ValueExpr
						val_attr_node.Datum.SetString(cond_.Equal)

						val_int, _ := strconv.Atoi(cond_.Equal)
						val_attr_node.Datum.SetInt64(int64(val_int))

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

func GetFilterCondition(frags_ meta.Partition, frag_name string) ([]string, []string, error) {
	// filter condition
	var condition_str_ []string

	conds_, col_str_ := InitFragWithCondition(frags_, frag_name)

	if strings.EqualFold(frags_.FragType, "HORIZONTAL") {
		for _, cond_ := range conds_ {
			expr, ok := cond_.(*ast.BinaryOperationExpr)
			if !ok {
				return condition_str_, col_str_, errors.New("fail to type cast into BinaryOperationExpr")
			}
			condition_str_ = append(condition_str_, TransExprNode2Str(expr))
		}
	}
	sort.Strings(condition_str_)
	sort.Strings(col_str_)
	//
	return condition_str_, col_str_, nil
}

func SplitFragTable_(ctx meta.Context, p *PlanTreeNode) error {
	//
	var frags_ meta.Partition
	var err error
	is_find_ := false

	for _, partition := range ctx.TablePartitions.Partitions {
		if strings.EqualFold(partition.TableName, p.FromTableName) {
			frags_ = partition
			is_find_ = true
			break
		}
	}
	if !is_find_ {
		err = errors.New("fail to find table " + p.FromTableName + " in any frags")
		return err
	}

	// find and expand the frag tables
	for _, site_info := range frags_.SiteInfos {
		if strings.EqualFold(frags_.FragType, "HORIZONTAL") {
			p.Type = UnionType
			node := PlanTreeNode{
				Type:          DataSourceType,
				FromTableName: site_info.Name,
				ExecuteSiteIP: site_info.IP,
				DestSiteIP:    ctx.IP,
			}.Init()
			//
			conds_, _ := InitFragWithCondition(frags_, site_info.Name)
			node.Conditions = append(node.Conditions, conds_...)
			p.AddChild(node)

		} else {
			p.Type = JoinType
			node := PlanTreeNode{
				Type:          DataSourceType,
				FromTableName: site_info.Name,
				ExecuteSiteIP: site_info.IP,
				DestSiteIP:    ctx.IP,
			}.Init()
			_, cols_ := InitFragWithCondition(frags_, site_info.Name)
			// node.ColsName = append(node.ColsName, cols_...)
			fmt.Println(cols_)
			p.AddChild(node)
		}
	}
	return nil
}

func SplitFragTable(ctx meta.Context, p *PlanTreeNode) {
	nums := p.GetChildrenNum()
	for i := 0; i < nums; i++ {
		child_ := p.GetChild(i)
		if child_.Type == DataSourceType {
			//
			SplitFragTable_(ctx, child_)
		} else {
			SplitFragTable(ctx, child_)
		}
	}
}

func PushDownPredicates(ctx meta.Context, frag_node *PlanTreeNode, where *PlanTreeNode) (err error) {
	if where == nil {
		return nil
	}
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
				frag_node.ConditionsStr = append(frag_node.ConditionsStr, TransExprNode2Str(expr))
				fmt.Println("Condition [" + strconv.FormatInt(int64(index_), 10) + "] adds to " + frag_node.FromTableName)
			}

		} else {
			return errors.New("not support")
		}
	}
	return nil
}

func PushDownProjections(ctx meta.Context, frag_node *PlanTreeNode, where *PlanTreeNode, proj *PlanTreeNode) (err error) {

	// var frag_projection []string

	frag_projection := mapset.NewSet()
	if where != nil {
		// add where projection
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
					if !frag_projection.Contains(left_attr.Name.Name.String()) {
						frag_projection.Add(left_attr.Name.Name.String())
						fmt.Println("ColName from Condition [" + strconv.FormatInt(int64(index_), 10) + "] :" + left_attr.Name.Name.String() + " adds to " + frag_node.FromTableName)
					}
				}

			} else {
				return errors.New("not support")
			}
		}

	}
	// add projection
	frag_type, err := GetFragType(ctx, frag_node.FromTableName)
	if err != nil {
		return err
	}
	if proj != nil {
		for index_, p_ := range proj.ColsName {
			table_name := strings.Split(p_, ".")[0]
			col_name := strings.Split(p_, ".")[1]
			if !strings.Contains(frag_node.FromTableName, table_name) {
				continue
			}
			col_exists, _ := RetureType(ctx.TableMetas, frag_node.FromTableName, col_name)
			if len(col_exists) > 0 {
				if !frag_projection.Contains(col_name) {
					frag_projection.Add(col_name)
					fmt.Println("ColName [" + strconv.FormatInt(int64(index_), 10) + "]: " + p_ + " adds to " + frag_node.FromTableName)
				}
			}
		}

		if strings.EqualFold(frag_type, "HORIZONTAL") {
			it := frag_projection.Iterator()
			for elem := range it.C {
				frag_node.ColsName = append(frag_node.ColsName, elem.(string))
			}
		} else {
			// already assigned at split-stage, check if `frag_projection` is already satisfied with other vertical frags
			// TODO: has bug...
			frag_projection_other := mapset.NewSet()
			for _, partition := range ctx.TablePartitions.Partitions {
				if strings.Contains(frag_node.FromTableName, partition.TableName) {
					for _, frag_ := range partition.VFragInfos {
						if frag_.SiteName != frag_node.FromTableName {
							// get other table v-info
							for _, col_ := range frag_.ColumnName {
								frag_projection_other.Add(col_)
							}
						}
					}
				}
			}

			if frag_projection.Intersect(frag_projection_other).Cardinality() == frag_projection.Cardinality() {
				// frag_projection_other is much greater
				frag_node.IsPruned = true
				fmt.Println("frag_node " + frag_node.FromTableName + " IS PRUNED")
			} else {
				//
				it := frag_projection.Iterator()
				for elem := range it.C {
					frag_node.ColsName = append(frag_node.ColsName, elem.(string))
				}
			}
		}
	}

	return nil
}

func GetPredicatePruning(ctx meta.Context, frag_node *PlanTreeNode) error {
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
			attr_num := left_attr.Name.Table.String() + "." + left_attr.Name.Name.String()
			var new_range DataRange
			type_, err := RetureType(ctx.TableMetas, left_attr.Name.Table.String(), left_attr.Name.Name.String())
			if err != nil {
				return nil
			}
			new_range.FieldType = type_

			switch expr.Op {
			case opcode.EQ:
				if type_ == "string" {
					new_range.LValueStr = right_val.GetDatumString()
					new_range.RValueStr = right_val.GetDatumString()
				} else {
					new_range.LValueInt = int(right_val.GetInt64())
					new_range.RValueInt = int(right_val.GetInt64())
				}
			case opcode.LT:
				if type_ == "string" {
					new_range.LValueStr = ""
					new_range.RValueStr = right_val.GetDatumString()
				} else {
					new_range.LValueInt = utils.MinInt
					new_range.RValueInt = int(right_val.GetInt64())
				}
			case opcode.GE, opcode.GT:
				if type_ == "string" {
					new_range.LValueStr = right_val.GetDatumString()
					new_range.RValueStr = ""
				} else {
					new_range.LValueInt = int(right_val.GetInt64())
					new_range.RValueInt = utils.MaxInt
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

func SelectionAndProjectionPushDown(ctx meta.Context, from *PlanTreeNode,
	where *PlanTreeNode, proj *PlanTreeNode) error {
	// single table predicate pushdown
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
					err = PushDownProjections(ctx, frag_ptr, where, proj)
					if err != nil {
						return err
					}
				}
			}

			break
		} else {
			// UnionType[ORDERS]
			// DataSourceType[ORDERS.1] DataSourceType[ORDERS.2] DataSourceType[ORDERS.3]
			// child_num = cur_ptr.GetChildrenNum()
			// cur_table_ptr := cur_ptr.GetChild(0)
			frag_num := cur_ptr.GetChildrenNum()
			cur_table_ptr := cur_ptr
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

				err = PushDownProjections(ctx, frag_ptr, where, proj)
				if err != nil {
					return err
				}
			}
			break
		}

	}
	return nil
}

func AddJoinCondition(join_cond_expr *ast.BinaryOperationExpr, new_join *PlanTreeNode) ([]ast.ExprNode, error) {
	// @brief add join-where conditions which cross two tables
	// i.e. Customer.id=Orders.customer_id
	var ret []ast.ExprNode
	var err error

	join_left, join_right, join_left_attr, join_right_attr, _, _, err := GetCondition(join_cond_expr)
	if join_left != AttrType || join_right != AttrType || err != nil {
		return ret, errors.New("should all be attributes")
	}

	switch join_cond_expr.Op {
	case opcode.EQ:
		{
			for _, cond_ := range new_join.Conditions {
				expr, ok := cond_.(*ast.BinaryOperationExpr)
				if !ok {
					return ret, errors.New("fail to type cast into BinaryOperationExpr")
				}
				left, right, left_attr, _, _, _, err := GetCondition(expr)
				if err != nil {
					return ret, err
				}
				if left == AttrType && right == ValueType {
					if left_attr.Name.Table.String()+"."+left_attr.Name.Name.String() == join_left_attr.Name.Table.String()+"."+join_left_attr.Name.Name.String() {
						// Customer.id=Orders.customer_id <-join_cond_expr
						// Customer.id>308000 <-left_attr
						// Orders.customer_id>308000 should be added
						var new_cond_ ast.BinaryOperationExpr
						new_cond_.Op = expr.Op
						new_cond_.L = join_cond_expr.R
						new_cond_.R = expr.R
						ret = append(ret, &new_cond_)
					}

					if left_attr.Name.Table.String()+"."+left_attr.Name.Name.String() == join_right_attr.Name.Table.String()+"."+join_right_attr.Name.Name.String() {
						// Orders.customer_id=Customer.id
						// Customer.id>308000 <-left_attr
						// Orders.customer_id>308000 should be added
						var new_cond_ ast.BinaryOperationExpr
						new_cond_.Op = expr.Op
						new_cond_.L = join_cond_expr.L
						new_cond_.R = expr.R
						ret = append(ret, &new_cond_)
					}
				} else {
					fmt.Println("do not support...")
				}
			}

		}
	default:
		{
			fmt.Println("do not support else join pruning")
		}

	}
	return ret, err
}

func PredicatePruning(ctx meta.Context, from *PlanTreeNode, where *PlanTreeNode) ([]string, []string, error) {
	// PlanTreeNode,
	var PrunedNodeName []string
	var UnPrunedNodeName []string
	var err error
	// var new_joined_node *PlanTreeNode
	if where == nil {
		return PrunedNodeName, UnPrunedNodeName, err
	}
	for _, cond_ := range where.Conditions {
		// join table by the where predicates
		expr, ok := cond_.(*ast.BinaryOperationExpr)
		if !ok {
			return PrunedNodeName, UnPrunedNodeName, errors.New("fail to type cast into BinaryOperationExpr")
		}
		left, right, left_attr, right_attr, _, _, err := GetCondition(expr)
		if err != nil {
			return PrunedNodeName, UnPrunedNodeName, err
		}
		// only consider join in where condition first
		if left == AttrType && right == AttrType {
			l_table := left_attr.Name.Table.String()
			r_table := right_attr.Name.Table.String()

			index_l, l_table_node := FindMainTableNode(ctx, from, l_table)
			index_r, r_table_node := FindMainTableNode(ctx, from, r_table)
			if l_table_node == nil || r_table_node == nil {
				return PrunedNodeName, UnPrunedNodeName, errors.New("fail to find table " + l_table + " " + r_table)
			}

			if index_r > index_l {
				// make sure un-joined frags always be on right
				utils.Swap(&index_r, &index_l)
				SwapNode(l_table_node, r_table_node)
			}
			// new_union := PlanTreeNode{
			// 	Type:          UnionType,
			// 	FromTableName: l_table_node.FromTableName + "|" + r_table_node.FromTableName,
			// }.Init()

			// start to join one by one
			for i := 0; i < l_table_node.GetChildrenNum(); i++ {
				cur_l_frag := *l_table_node.GetChild(i)

				// iterate right frags
				for j := 0; j < r_table_node.GetChildrenNum(); j++ {
					cur_r_frag := *r_table_node.GetChild(j)

					new_join := PlanTreeNode{
						Type:          JoinType,
						FromTableName: cur_l_frag.FromTableName + "|" + cur_r_frag.FromTableName,
					}.Init()

					new_join.AddChild(&cur_l_frag)
					new_join.AddChild(&cur_r_frag)

					new_join.Conditions = append(new_join.Conditions, cur_l_frag.Conditions...)
					new_join.Conditions = append(new_join.Conditions, cur_r_frag.Conditions...)

					new_cond_join, err := AddJoinCondition(expr, new_join)
					if err != nil {
						return PrunedNodeName, UnPrunedNodeName, err
					}
					// add all conditions
					new_join.Conditions = append(new_join.Conditions, new_cond_join...)
					//
					new_join.IsPruned = cur_l_frag.IsPruned || cur_r_frag.IsPruned
					// check if it can be pruned
					err = GetPredicatePruning(ctx, new_join)
					if err != nil {
						return PrunedNodeName, UnPrunedNodeName, err
					}
					if new_join.IsPruned {
						PrunedNodeName = append(PrunedNodeName, new_join.FromTableName)
						// new_union.AddChild(new_join)
					} else {
						UnPrunedNodeName = append(UnPrunedNodeName, new_join.FromTableName)
					}
				}
			}
		} else {
			continue
		}

	}
	return PrunedNodeName, UnPrunedNodeName, err
}

func GetJoinSeq(ctx meta.Context, from *PlanTreeNode, where *PlanTreeNode, PrunedNodeName []string) ([]string, error) {
	// join sequence: A = B
	var err error
	var join_seq []string
	var join_table_set []string
	// join_seq := mapset.NewSet()
	// join_table_set := mapset.NewSet()
	if where == nil {
		return join_seq, err
	}
	for _, cond_ := range where.Conditions {
		// join table by the where predicates
		expr, ok := cond_.(*ast.BinaryOperationExpr)
		if !ok {
			return join_seq, errors.New("fail to type cast into BinaryOperationExpr")
		}
		left, right, left_attr, right_attr, _, _, err := GetCondition(expr)
		if err != nil {
			return join_seq, err
		}
		// only consider join in where condition first
		if left == AttrType && right == AttrType {
			l_table := left_attr.Name.Table.String()
			r_table := right_attr.Name.Table.String()
			if l_table > r_table {
				utils.SwapStr(&l_table, &r_table)
			}
			//
			join_str := l_table + "=" + r_table
			if utils.FindStr(join_seq, join_str) == -1 {
				// if !join_seq.Contains(join_str) {
				// join_seq.Add(join_str)
				join_seq = append(join_seq, join_str)
				join_table_set = append(join_table_set, l_table)
				join_table_set = append(join_table_set, r_table)
				// join_table_set.Add(l_table)
				// join_table_set.Add(r_table)
			}
		}
	}

	// get join from the remaining table
	for i := 0; i < from.GetChildrenNum(); i++ {
		child := from.GetChild(i)
		// if join_table_set.Contains(child.FromTableName) {
		if utils.FindStr(join_table_set, child.FromTableName) != -1 {
			continue
		} else {
			//
			// iter := join_table_set.Iterator()
			// for elem := range iter.C {
			for _, elem := range join_table_set {
				cur_head_table := elem // elem.(*string)
				join_str := cur_head_table + "=" + child.FromTableName

				if utils.FindStr(join_seq, join_str) != -1 {
					// if join_seq.Contains(join_str) {
					return join_seq, errors.New("join_seq.Contains(join_str) should not contain" + join_str)
				}
				join_seq = append(join_seq, join_str)
				// join_seq.Add(join_str)
				join_table_set = append(join_table_set, child.FromTableName)
				// join_table_set.Add(child.FromTableName)
				// iter.Stop()
			}
		}
	}

	return join_seq, err
}

func CheckPruned(PrunedNodeName []string, NodeName string) bool {
	ret := false
	for _, cur_ := range PrunedNodeName {

		cur_node_name := strings.Split(NodeName, "|")
		cur_pruned_condition := strings.Split(cur_, "|")

		// if every pruned node name is in cur node name
		// then should be pruned
		all_in := true
		for _, pruned_ := range cur_pruned_condition {
			if utils.FindStr(cur_node_name, pruned_) == -1 {
				all_in = false
				break
			}
		}
		if all_in {
			ret = true
			break
		}
	}
	return ret
}

func FindJoinNode(table_name_left string, table_name string, UnPrunedNodeName []string) string {
	// CUSTOMER.1|ORDERS.3 CUSTOMER.1|ORDERS.4
	// table_name_left = CUSTOMER.1
	// table_name_left = ORDERS
	// output: [ORDERS.3, ORDERS.4]
	var res string
	for i, cur := range UnPrunedNodeName {
		if strings.Contains(cur, table_name) && strings.Contains(cur, table_name_left) {
			UnPrunedNodeName[i] = ""
		} else {
			if strings.Contains(cur, table_name) {
				res += strings.Replace(cur, table_name, "", -1) + ", " //
			}
		}
	}
	return res
}

func JoinUsingPruning(ctx meta.Context, from *PlanTreeNode, where *PlanTreeNode,
	PrunedNodeName []string, UnPrunedNodeName []string) (*PlanTreeNode, error) {
	//

	var new_joined_node *PlanTreeNode
	var err error

	PrintPlanTreePlot(from)

	// join sequence: A = B
	// where condition first
	join_seq, err := GetJoinSeq(ctx, from, where, PrunedNodeName)
	if err != nil {
		return new_joined_node, err
	}
	// join all tables
	// it := join_seq.Iterator()
	// for elem := range it.C {
	for _, elem := range join_seq {
		join_str_ := elem // .(string)
		join_strs := strings.Split(join_str_, "=")
		l_table := strings.Trim(join_strs[0], " ")
		r_table := strings.Trim(join_strs[1], " ")

		index_l, l_table_node := FindMainTableNode(ctx, from, l_table)
		index_r, r_table_node := FindMainTableNode(ctx, from, r_table)
		if l_table_node == nil || r_table_node == nil {
			return new_joined_node, errors.New("fail to find table " + l_table + " " + r_table)
		}

		if index_r > index_l {
			// make sure un-joined frags always be on right
			utils.Swap(&index_r, &index_l)
			SwapNode(l_table_node, r_table_node)
		}

		new_union := PlanTreeNode{
			Type:          UnionType,
			FromTableName: l_table_node.FromTableName + "|" + r_table_node.FromTableName,
		}.Init()

		s_whole_right := mapset.NewSet()
		var r_whole_frag_join_ *PlanTreeNode
		// start to join one by one
		for i := 0; i < l_table_node.GetChildrenNum(); i++ {
			cur_l_frag := *l_table_node.GetChild(i)
			// check if can be union
			s_ := mapset.NewSet()
			is_union_able := true
			for j := 0; j < r_table_node.GetChildrenNum(); j++ {
				cur_r_frag := *r_table_node.GetChild(j)
				//!!! remove UnPrunedNodeName!!!
				res := FindJoinNode(l_table, cur_r_frag.FromTableName, UnPrunedNodeName)
				s_.Add(res)
			}
			if s_.Cardinality() > 1 {
				is_union_able = false
			}

			var r_frag_join_ *PlanTreeNode
			if is_union_able {
				//TODO: joinable frags prefer to union or join together first
				// maybe optimized
				// var r_frags_join_type NodeType
				// r_frags_type := ReturnFragType(ctx, r_table)
				// if strings.EqualFold(r_frags_type, "VERTICAL") {
				// 	r_frags_join_type = UnionType
				// } else {
				// 	r_frags_join_type = JoinType
				// }
				r_frag_join_ = PlanTreeNode{
					// Type: r_frags_join_type,
					Type: UnionType,
				}.Init()
			}

			// iterate right frags
			for j := 0; j < r_table_node.GetChildrenNum(); j++ {
				cur_r_frag := *r_table_node.GetChild(j)

				new_join := PlanTreeNode{
					Type:          JoinType,
					FromTableName: cur_l_frag.FromTableName + "|" + cur_r_frag.FromTableName,
				}.Init()

				new_join.AddChild(&cur_l_frag)
				new_join.AddChild(&cur_r_frag)

				new_join.Conditions = append(new_join.Conditions, cur_l_frag.Conditions...)
				new_join.Conditions = append(new_join.Conditions, cur_r_frag.Conditions...)

				if !CheckPruned(PrunedNodeName, new_join.FromTableName) {
					//
					if is_union_able {
						// add to r_frag
						r_frag_join_.AddChild(&cur_r_frag)
						r_frag_join_.FromTableName += cur_r_frag.FromTableName + "|"
					} else {
						new_union.AddChild(new_join)
					}
				}
				if err != nil {
					return new_joined_node, err
				}
			}
			// r_frag_join_is_not_empty
			if is_union_able {
				if r_frag_join_.GetChildrenNum() > 0 {
					new_join := PlanTreeNode{
						Type:          JoinType,
						FromTableName: cur_l_frag.FromTableName + "|" + r_frag_join_.FromTableName,
					}.Init()
					new_join.AddChild(&cur_l_frag)
					new_join.AddChild(r_frag_join_)

					new_union.AddChild(new_join)
					// r_frag_join_
					s_whole_right.Add(r_frag_join_.FromTableName)
					r_whole_frag_join_ = r_frag_join_
				}
			}
		}

		if s_whole_right.Cardinality() == 1 {
			// whole right is the same, join together
			new_union.RemoveAllChild()

			new_union_left := PlanTreeNode{
				Type:          UnionType,
				FromTableName: "UnionType",
			}.Init()

			for i := 0; i < l_table_node.GetChildrenNum(); i++ {
				cur_l_frag := *l_table_node.GetChild(i)
				new_union_left.AddChild(&cur_l_frag)
			}

			new_union_join := PlanTreeNode{
				Type:          JoinType,
				FromTableName: "JoinType",
			}.Init()

			new_union_join.AddChild(new_union_left)
			new_union_join.AddChild(r_whole_frag_join_)

			new_union.AddChild(new_union_join)
		}
		//
		if index_l > index_r {
			from.RemoveChild(index_l)
			from.RemoveChild(index_r)
		} else {
			from.RemoveChild(index_r)
			from.RemoveChild(index_l)
		}

		if new_union.GetChildrenNum() > 0 {
			// joined successfully
			from.AddChild(new_union)
		}
		// test
		PrintPlanTreePlot(from)
	}

	new_joined_node = from

	return new_joined_node, err
}

type PartitionInfo struct {
	PartitionType string
	HFrag         meta.HFragInfo
	VFrag         meta.VFragInfo
}

func GetPartitionMeta(ctx meta.Context, frag_name string) (PartitionInfo, error) {
	//
	var partition_meta meta.Partition
	var err error
	var ret PartitionInfo

	table_name := strings.Split(frag_name, "_")[0]

	_, partition_meta, err = FindMetaInfo(ctx, table_name)
	if err != nil {
		return ret, err
	}
	ret.PartitionType = partition_meta.FragType

	if strings.EqualFold(ret.PartitionType, "HORIZONTAL") {
		//
		for _, partition_ := range partition_meta.HFragInfos {
			if strings.EqualFold(partition_.FragName, frag_name) {
				ret.HFrag = partition_
				break
			}
		}
	} else {
		// Vertical
		for _, partition_ := range partition_meta.VFragInfos {
			if strings.EqualFold(partition_.SiteName, frag_name) {
				ret.VFrag = partition_
				break
			}
		}
	}
	return ret, err
}

func filterCondition(partition_info PartitionInfo, from *PlanTreeNode) ([]ast.ExprNode, []string) {
	// filter condition
	var condition_ []ast.ExprNode
	var condition_str_ []string

	if strings.EqualFold(partition_info.PartitionType, "VERTICAL") {
		for i_, cond_ := range from.Conditions {
			is_in_ := true

			expr, ok := cond_.(*ast.BinaryOperationExpr)
			if !ok {
				fmt.Println("ERROR expr, ok := cond_.(*ast.BinaryOperationExpr)")
				break
			}
			//
			left, right, left_attr, right_attr, _, _, err := GetCondition(expr)
			if err != nil {
				fmt.Println("ERROR filterCondition")
				break
			}
			if left == AttrType {
				is_find_ := false
				for _, cur_col := range partition_info.VFrag.ColumnName {
					if strings.EqualFold(cur_col, left_attr.Name.Name.String()) {
						is_find_ = true
						break
					}
				}
				if !is_find_ {
					is_in_ = false
				}
			}

			if right == AttrType {
				is_find_ := false
				for _, cur_col := range partition_info.VFrag.ColumnName {
					if strings.EqualFold(cur_col, right_attr.Name.Name.String()) {
						is_find_ = true
						break
					}
				}
				if !is_find_ {
					is_in_ = false
				}
			}

			if is_in_ {
				condition_ = append(condition_, from.Conditions[i_])
				condition_str_ = append(condition_str_, from.ConditionsStr[i_])
			}
		}
	} else {
		condition_ = from.Conditions
		condition_str_ = from.ConditionsStr
	}
	sort.Strings(from.ConditionsStr)
	//
	return condition_, condition_str_
}

func filterColumns(partition_info PartitionInfo, from *PlanTreeNode) []string {
	// filter columns
	var colsName []string

	if strings.EqualFold(partition_info.PartitionType, "VERTICAL") {
		for i_, cur_cond := range from.ColsName {
			is_in_ := false
			for _, col := range partition_info.VFrag.ColumnName {
				if col == cur_cond {
					is_in_ = true
					break
				}
			}
			if is_in_ {
				colsName = append(colsName, from.ColsName[i_])
			}
		}
	} else {
		colsName = from.ColsName
	}
	sort.Strings(colsName)
	//
	return colsName
}

func AddProjectionAndSelectionNode(ctx meta.Context, from *PlanTreeNode, node_index_ int, parent *PlanTreeNode) error {
	var err error
	if from.GetChildrenNum() == 0 {
		if from.Type != DataSourceType {
			err = errors.New("from.Type != DataSourceType ")
			return err
		}
		// frag node
		var select_node *PlanTreeNode
		var proj_node *PlanTreeNode
		select_node = nil
		proj_node = nil

		if err != nil {
			return err
		}

		partition_info, err := GetPartitionMeta(ctx, from.FromTableName)
		if err != nil {
			return err
		}

		if len(from.ConditionsStr) > 0 {
			from.Conditions, from.ConditionsStr = filterCondition(partition_info, from)

			select_node = PlanTreeNode{
				Type:          SelectType,
				Conditions:    from.Conditions,
				ConditionsStr: from.ConditionsStr,
				ExecuteSiteIP: from.ExecuteSiteIP,
				DestSiteIP:    from.DestSiteIP,
			}.Init()

		}
		if len(from.ColsName) > 0 {
			from.ColsName = filterColumns(partition_info, from)

			proj_node = PlanTreeNode{
				Type:          ProjectionType,
				ColsName:      from.ColsName,
				ExecuteSiteIP: from.ExecuteSiteIP,
				DestSiteIP:    from.DestSiteIP,
			}.Init()
		}

		if proj_node != nil {
			proj_node.SetChildren(from)
		}
		if select_node != nil && proj_node != nil {
			select_node.SetChildren(proj_node)
		}

		if parent == nil && parent.GetChildrenNum() < node_index_ {
			return errors.New("unkown error")
		}
		if select_node != nil {
			parent.ResetChild(node_index_, select_node)
		} else if proj_node != nil {
			parent.ResetChild(node_index_, proj_node)
		}
	} else {
		for i := 0; i < from.GetChildrenNum(); i++ {

			err := AddProjectionAndSelectionNode(ctx, from.GetChild(i), i, from)
			if err != nil {
				return err
			}
		}
	}
	return err
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
