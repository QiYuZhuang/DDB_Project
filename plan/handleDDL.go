package plan

import (
	"errors"
	"fmt"
	"project/utils"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
)

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

	create_cols := strings.Split(utils.GetMidStr(sel.Text(), "(", ")"), ",")

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

func HandlePartitionSQL(sql_str string) (Partition, error) {
	// type Partition struct {
	// 	TableName  string      `json:"table_name"`
	// 	SiteInfos  []SiteInfo  `json:"site_info"`
	// 	HFragInfos []HFragInfo `json:"horizontal_fragmentation"`
	// 	VFragInfos []VFragInfo `json:"vertical_fragmentation"`
	// }
	var partition Partition
	var err error

	partition.TableName = utils.GetMidStr(sql_str, "|", "|")

	frag_type := utils.GetMidStr(sql_str, "[", "]")
	site_ips := strings.Split(utils.GetMidStr(sql_str, "(", ")"), ",")
	site_details := strings.Split(utils.GetMidStr(sql_str, "{", "}"), ";")
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
