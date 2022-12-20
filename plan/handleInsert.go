package plan

import (
	"errors"
	"fmt"
	"project/etcd"
	"project/meta"
	utils "project/utils"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
)

func GenInsertRequest(table_meta meta.TableMeta, partition_meta meta.Partition, columns []string, values []string) ([]InsertRequest, error) {
	//
	var returns []InsertRequest
	var err error

	// if strings.EqualFold(partition_meta.FragType, "HORIZONTAL") {
	if partition_meta.FragType == meta.Horizontal {
		// insert to one table only
		var ret InsertRequest

		for frag_index_, frag_ := range partition_meta.HFragInfos {
			is_satisfied_ := true

			var my_cols_ []meta.Column
			if len(columns) == 0 {
				// insert into publisher values(104001,'High Education Press', 'PRC');
				my_cols_ = table_meta.Columns
			} else {
				// generate from input parameter columns
				// insert into publisher(id, name, nation) values(104001,'High Education Press', 'PRC');
				for _, col_ := range columns {
					for _, table_col_ := range table_meta.Columns {
						col_ = strings.Replace(col_, " ", "", -1)
						if strings.EqualFold(col_, table_col_.ColumnName) {
							my_cols_ = append(my_cols_, table_col_)
						}
					}
				}
			}

			for _, cond_ := range frag_.Conditions {
				// cond_ are joined with AND only
				// get index and value for cur cond_
				cur_col_index_ := -1
				var cur_col meta.Column

				for col_index_, col_ := range my_cols_ {
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
				val_ = strings.Replace(val_, " ", "", -1)
				val_ = strings.Replace(val_, "'", "", -1)
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

				for col_index_, col_ := range columns {
					col_ = strings.Replace(col_, " ", "", -1)
					if strings.EqualFold(col_, col_name) {
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
	col_and_val := strings.Split(sql, "values")

	var columns, values []string
	if strings.Contains(col_and_val[0], "(") && strings.Contains(col_and_val[0], ")") {
		columns = strings.Split(utils.GetMidStr(col_and_val[0], "(", ")"), ",")
	}
	if strings.Contains(col_and_val[1], "(") && strings.Contains(col_and_val[1], ")") {
		values = strings.Split(utils.GetMidStr(col_and_val[1], "(", ")"), ",")
	}

	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, tablename)
	if err != nil {
		return ret, err
	}

	// check current value is in plan.Partition or not
	insert_requests, err := GenInsertRequest(table_meta, partition_meta, columns, values)
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

	// tablename := sel.Tables.Tables[0].Name.String()
	tablename := strings.Split(sql, "from")[1]
	tablename = strings.Replace(tablename, " ", "", -1)
	tablename = strings.Replace(tablename, ";", "", -1)

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
			ret = append(ret, sql_router_)
		}
	} else {
		// TODO vertical fragment
		// ask certain table for primary key
		// and broadcast delete to all frags
		for frag_index := range partition_meta.VFragInfos {
			site_name_ := partition_meta.VFragInfos[frag_index].FragName
			var sql_router_ meta.SqlRouter
			sql_router_.Site_ip = partition_meta.SiteInfos[frag_index].IP
			sql_router_.Site_port = partition_meta.SiteInfos[frag_index].Port
			sql_router_.Sql = sql
			sql_router_.Sql = strings.Replace(sql_router_.Sql, table_meta.TableName, site_name_, 1)
			ret = append(ret, sql_router_)
		}
		// err = errors.New("todo not implemented vertical delete")
		//
	}

	fmt.Println(tablename)
	return ret, err
}

func BroadcastSQL(ctx meta.Context, stmt ast.StmtNode) ([]meta.SqlRouter, meta.StmtType, error) {
	var ret []meta.SqlRouter
	var sql_type_ meta.StmtType
	sql := stmt.Text()
	fmt.Println(sql)

	if utils.ContainString(sql, "drop", true) && utils.ContainString(sql, "database", true) {
		if !ctx.IsDebugLocal {
			//drop datameta from etcd
			for _, table_ := range ctx.TableMetas.TableMetas {
				err := etcd.DropTablefromEtcd(table_.TableName, false)
				if err != nil {
					fmt.Println("fail to drop table in etcd when drop database : ", err, table_.TableName)
				}

			}

			for _, partition_ := range ctx.TablePartitions.Partitions {
				// Need to check
				err := etcd.DropPartitionfromEtcd(partition_.TableName)
				if err != nil {
					fmt.Println("fail to drop partition in etcd when drop database : ", err, partition_.TableName)
				}
			}
		}
		sql_type_ = meta.DropTableStmtType
	} else if utils.ContainString(sql, "create", true) && utils.ContainString(sql, "database", true) {
		sql_type_ = meta.CreateDatabaseStmtType
	} else if utils.ContainString(sql, "use", true) {
		sql_type_ = meta.UseDatabaseStmtType
	} else {
		sql_type_ = meta.StmtTypeNum
	}

	for _, peer := range ctx.Peers {
		var per meta.SqlRouter
		per.Site_ip = peer.Ip
		per.Site_port = peer.Port
		per.Sql = sql
		ret = append(ret, per)
	}
	return ret, sql_type_, nil
}
