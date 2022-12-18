package plan

import (
	"errors"
	"fmt"
	"project/meta"
	utils "project/utils"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
)

func GenInsertRequest(table_meta meta.TableMeta, partition_meta meta.Partition, values []string) ([]InsertRequest, error) {
	//
	var returns []InsertRequest
	var err error

	// if strings.EqualFold(partition_meta.FragType, "HORIZONTAL") {
	if partition_meta.FragType == meta.Horizontal {
		// insert to one table only
		var ret InsertRequest

		for frag_index_, frag_ := range partition_meta.HFragInfos {
			is_satisfied_ := true

			for _, cond_ := range frag_.Conditions {
				// cond_ are joined with AND only

				// get index and value for cur cond_
				cur_col_index_ := -1
				var cur_col meta.Column

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
				// val_ = strings.Trim(val_, "'")

				// value get compared with condition
				if len(cond_.Equal) != 0 {
					// EQ
					if cond_.Equal != val_ {
						is_satisfied_ = false
						break
					}
				} else {
					// GT LT
					if cur_col.Type == meta.Varchar {
						if !(cond_.GreaterEqualThan <= val_ &&
							val_ < cond_.LessThan) {
							is_satisfied_ = false
							break
						}
					} else if cur_col.Type == meta.Int32 {
						var get int
						var lt int
						if len(cond_.GreaterEqualThan) > 0 {
							get, _ = strconv.Atoi(cond_.GreaterEqualThan)
						} else {
							get = utils.MinInt
						}

						if len(cond_.LessThan) > 0 {
							lt, _ = strconv.Atoi(cond_.LessThan)
						} else {
							lt = utils.MaxInt
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
	} else if partition_meta.FragType == meta.Vertical {
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

func GenInsertSQL(insert_requests []InsertRequest) ([]meta.SqlRouter, error) {
	var err error
	var ret []meta.SqlRouter

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
		cur_sql := "insert into " + insert_req.Siteinfo.FragName + "(" + table_cols + ") values " + "(" + table_vals + ");"
		var cur_insert meta.SqlRouter
		cur_insert.Site_ip = insert_req.Siteinfo.IP
		cur_insert.Site_port = insert_req.Siteinfo.Port
		cur_insert.Sql = cur_sql
		ret = append(ret, cur_insert)
	}
	return ret, err
}

func HandleInsert(ctx meta.Context, stmt ast.StmtNode) ([]meta.SqlRouter, error) {
	// router the insert sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []meta.SqlRouter
	sel := stmt.(*ast.InsertStmt)
	fmt.Println(sel)
	sql := sel.Text()
	fmt.Println(sql)
	// ret.SQL = sql

	tablename := sel.Table.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.String()
	values := strings.Split(utils.GetMidStr(sel.Text(), "(", ")"), ",")

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}

	// check current value is in plan.Partition or not
	insert_requests, err := GenInsertRequest(table_meta, partition_meta, values)
	if err != nil {
		return ret, err
	}
	// generate sql with router ip
	ret, err = GenInsertSQL(insert_requests)
	fmt.Println(tablename)
	return ret, err
}

func HandleDelete(ctx meta.Context, stmt ast.StmtNode) ([]meta.SqlRouter, error) {
	// router the delete sql to partitioned site
	// Horizontal fragmentation only
	// only consider one fragmentation condition
	var ret []meta.SqlRouter
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
	if partition_meta.FragType == meta.Horizontal {
		for frag_index := range partition_meta.HFragInfos {
			site_name_ := partition_meta.HFragInfos[frag_index].FragName
			var sql_router_ meta.SqlRouter
			sql_router_.Site_ip = partition_meta.SiteInfos[frag_index].IP
			sql_router_.Site_port = partition_meta.SiteInfos[frag_index].Port
			sql_router_.Sql = sql
			sql_router_.Sql = strings.Replace(sql_router_.Sql, table_meta.TableName, site_name_, 1)
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

func BroadcastSQL(ctx meta.Context, stmt ast.StmtNode) ([]meta.SqlRouter, error) {
	var ret []meta.SqlRouter
	sql := stmt.Text()
	fmt.Println(sql)

	for _, peer := range ctx.Peers {
		var per meta.SqlRouter
		per.Site_ip = peer.Ip
		per.Site_port = peer.Port
		per.Sql = sql
		ret = append(ret, per)
	}
	return ret, nil
}
