package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os/user"
	"project/meta"
	"project/plan"
	"project/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Exec(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode) (string, error) {
	var (
		err      error
		filename string
	)
	l := ctx.Logger
	internal_results := make(chan meta.TempResult, 20)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	err = execPlanNode(ctx, txn, node, internal_results, wg)
	wg.Wait()
	close(internal_results)
	for f := range internal_results {
		l.Info("query result is in ", f.Filename)
		filename = f.Filename
	}
	return filename, err
}

func execPlanNode(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, internal_results chan meta.TempResult, wg *sync.WaitGroup) error {
	defer wg.Done()
	var temp_result meta.TempResult
	var err error

	temp_result.Filename = ""

	l := ctx.Logger

	if node == nil {
		return nil
	}

	query_id := node.NodeId

	if node.GetChildrenNum() == 0 {
		var (
			is_remote  bool
			select_sql string
			table_meta meta.TableMeta
		)
		table_name := node.FromTableName
		raw_table_name := strings.Split(table_name, "_")[0]
		raw_table_meta, _, _ := plan.FindMetaInfo(*ctx, raw_table_name)
		u, _ := user.Current()
		table_meta.TableName = raw_table_name
		table_meta.IsTemp = true

		is_remote = !(utils.ContainString(ctx.IP, node.ExecuteSiteIP, false) && node.ExecuteSitePort == "10800")

		attributes := ""
		references := ""
		predicates := ""

		select_sql = "SELECT "

		for i := 0; i < len(node.ColsName); i++ {
			if i == 0 {
				attributes += node.ColsName[i]
			} else {
				attributes = attributes + ", " + node.ColsName[i]
			}
			column, err := raw_table_meta.FindColumnByName(node.ColsName[i])
			if err != nil {
				l.Errorln(err.Error())
			}
			column.ColumnName = raw_table_name + "$" + column.ColumnName
			table_meta.Columns = append(table_meta.Columns, column)
		}

		if len(table_name) != 0 {
			references = " FROM " + table_name
		}

		for i := 0; i < len(node.ConditionsStr); i++ {
			if i == 0 {
				predicates = " WHERE " + node.ConditionsStr[i]
			} else {
				predicates += (" AND " + node.ConditionsStr[i])
			}
		}

		temp_result.Table_meta = table_meta

		select_sql = select_sql + attributes + references + predicates

		// tmp path is `/home/<username>`, filename is `TMP_<IP>_<table_name>.csv`
		if is_remote {
			// create message
			m := meta.NewQueryRequestMessage(ctx.IP, ctx.Port, node.ExecuteSiteIP, node.DestSitePort, txn.TxnId)
			m.SetQueryId(query_id)
			m.SetQuery(select_sql)
			m.SetTableName(table_name)

			// flush messages
			*ctx.Messages <- *m

			// wait for reply3
			for !txn.Responses[query_id] {
				time.Sleep(time.Duration(10) * time.Nanosecond)
			}

			// mv tmp file to tmp path
			temp_result.Filename = txn.TmpResultInFile[query_id]
			node.Status = txn.EffectRows[query_id]

			internal_results <- temp_result
		} else {
			tmp_filename := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(query_id) + ".csv"
			raw_path := "/tmp/data/" + tmp_filename
			tmp_path := "/home/" + u.Username + "/" + tmp_filename
			select_sql := utils.GenerateSelectIntoFileSql(strings.Trim(select_sql, ";"), raw_path, "|", "")
			if node.Status, err = LocalExecInternalSql(ctx, select_sql, "", meta.SelectIntoFileStmtType); err != nil {
				internal_results <- temp_result
				l.Errorln("interal execute failed", err.Error())
				return err
			}

			if err := utils.Chown(u.Username, raw_path, false); err != nil {
				internal_results <- temp_result
				l.Errorln("change owner failed", err.Error())
				return err
			}

			if err := utils.MvFile(raw_path, tmp_path, false); err != nil {
				internal_results <- temp_result
				l.Errorln("move file failed", err.Error())
				return err
			}

			temp_result.Filename = tmp_filename

			internal_results <- temp_result
		}
	} else {
		// create new wait group and threads
		// thread:
		// 		- generate sqls
		//		- flush messages
		// 		- wait for all children finished
		inner_wg := new(sync.WaitGroup)
		inner_channel := make(chan meta.TempResult, 20)
		inner_wg.Add(node.GetChildrenNum())

		for i := 0; i < node.GetChildrenNum(); i++ {
			go execPlanNode(ctx, txn, node.GetChild(i), inner_channel, inner_wg)
		}

		inner_wg.Wait()
		close(inner_channel)

		var file_list meta.TempResultList
		for c := range inner_channel {
			file_list = append(file_list, c)
		}

		// exec union or join
		if node.Type == plan.UnionType {
			interal_result, err := handleUnionOperation(ctx, txn, node, file_list)
			if err != nil {
				l.Errorln("handle union operation failed", err)
				return err
			}
			internal_results <- interal_result
		} else if node.Type == plan.JoinType {
			interal_result, err := handleJoinOperation(ctx, txn, node, file_list)
			if err != nil {
				l.Errorln("handle join operation failed", err)
				return err
			}
			internal_results <- interal_result
		} else if node.Type == plan.SelectType || node.Type == plan.ProjectionType {
			if len(file_list) != 1 {
				l.Errorln("handle invaild operation")
			}
			internal_results <- file_list[0]
		}
		// else if node.Type == plan.FinalProjectionType {
		// 	//
		// 	interal_result, err := handleProjectionOperation(ctx, txn, node, file_list)
		// 	if err != nil {
		// 		l.Errorln("handle join operation failed", err)
		// 		return err
		// 	}
		// 	internal_results <- interal_result
		// }
	}
	return nil
}

func handleProjectionOperation(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, file_list meta.TempResultList) (meta.TempResult, error) {
	var (
		ret meta.TempResult
		// table_names []string
		// filepath         string
		// final_meta       meta.TableMeta
		// final_conditions []string
		// temp_table_map   map[int]string
		err error
	)
	l := ctx.Logger

	// u, _ := user.Current()

	if len(file_list) != 1 {
		l.Errorln("len(file_list) != 1")
		return ret, errors.New("len(file_list) != 1")
	}

	table_meta := file_list[0].Table_meta
	ret.Table_meta = table_meta

	// create tmp table
	tmp_table_name := "INTER_TMP_" + strconv.Itoa(node.NodeId) + "_" + strconv.Itoa(int(time.Now().Unix()))
	if err = createTmpTable(tmp_table_name, table_meta, ctx.DB, true); err != nil {
		return ret, err
	}

	return ret, err
}

func handleJoinOperation(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, file_list meta.TempResultList) (meta.TempResult, error) {
	var (
		ret         meta.TempResult
		table_names []string
		// filepath         string
		final_meta       meta.TableMeta
		final_conditions []string
		temp_table_map   map[int]string
	)
	ret.Filename = ""
	temp_table_map = make(map[int]string)

	l := ctx.Logger
	conditions := parseCondition(node.ConditionsStr)
	u, _ := user.Current()

	sort.Sort(meta.TempResultList(file_list))

	for i := 0; i < len(file_list); i++ {
		tmp_table_name := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(node.NodeId) + "_" + strconv.Itoa(i)
		temp_table_map[i] = tmp_table_name
		table_names = append(table_names, tmp_table_name)

		tmp_table_meta := file_list[i].Table_meta
		if err := createTmpTable(tmp_table_name, tmp_table_meta, ctx.DB, true); err != nil {
			return ret, err
		}

		filename := file_list[i].Filename
		if len(filename) == 0 {
			l.Error("execute sub query id: ", i)
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, errors.New("execute sub query failed")
		}

		filepath := "/home/" + u.Username + "/" + filename
		dst_path := "/tmp/data/" + filename
		if err := utils.CpFile(filepath, dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		if err := utils.Chown("mysql", dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		// insert into tmp table
		if err := insertIntoTable(dst_path, tmp_table_name, ctx.DB); err != nil {
			l.Errorln("insert into tmp table ", tmp_table_name, ", error is ", err.Error())
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		if err := utils.RmFile(dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			l.Errorln("remove tmp file")
			return ret, err
		}
	}

	// rewrite conditions

	var is_used_ []bool
	for i := 0; i < len(file_list); i++ {
		is_used_ = append(is_used_, false)
	}

	for i := 0; i < len(conditions); i++ {
		lrewrite_ok, rrewrite_ok := false, false
		l_idx, r_idx, rf_idx := -1, -1, -1
		l_tname, l_cname, r_tname, r_cname := conditions[i].GetColumnName()
		need_compress := conditions[i].NeedCompress()
		for j := 0; j < len(file_list); j++ {
			l_idx = findColumn(file_list[j].Table_meta, l_tname, l_cname)
			if l_idx != -1 && !is_used_[j] {
				lrewrite_ok = true
				is_used_[j] = true
				conditions[i].SetConditionField(true, temp_table_map[j], file_list[j].Table_meta.Columns[l_idx].ColumnName)
				break
			}
		}
		for j := 0; j < len(file_list); j++ {
			r_idx = findColumn(file_list[j].Table_meta, r_tname, r_cname)
			if r_idx != -1 && !is_used_[j] {
				rf_idx = j
				rrewrite_ok = true
				is_used_[j] = true
				conditions[i].SetConditionField(false, temp_table_map[j], file_list[j].Table_meta.Columns[r_idx].ColumnName)
				break
			}
		}

		if lrewrite_ok && rrewrite_ok {
			if need_compress {
				file_list[rf_idx].Table_meta.EraseColumnByIdx(r_idx)
			}
			final_conditions = append(final_conditions, conditions[i].GetConditionStr())
		} else {
			l.Errorln("can not find columns")
			return ret, errors.New("rewrite failed")
		}
	}
	//
	for _, file_ := range file_list {
		final_meta.Columns = append(final_meta.Columns, file_.Table_meta.Columns...)
		if len(final_meta.TableName) > 0 {
			final_meta.TableName += "$"
		}
		final_meta.TableName += file_.Table_meta.TableName
	}

	join_sql := generateJoinSql(file_list, table_names, final_conditions)
	// select into tmp file
	tmp_filename := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(node.NodeId) + ".csv"
	tmp_filepath_1 := "/tmp/data/" + tmp_filename
	tmp_filepath_2 := "/home/" + u.Username + "/" + tmp_filename

	join_sql = utils.GenerateSelectIntoFileSql(join_sql, tmp_filepath_1, "|", "")
	if _, err := ctx.DB.Exec(join_sql); err != nil {
		dropTmpJoinTables(table_names, ctx.DB)
		return ret, err
	}

	if err := utils.Chown(u.Username, tmp_filepath_1, false); err != nil {
		dropTmpJoinTables(table_names, ctx.DB)
		return ret, err
	}

	if err := utils.MvFile(tmp_filepath_1, tmp_filepath_2, false); err != nil {
		dropTmpJoinTables(table_names, ctx.DB)
		return ret, err
	}

	if err := dropTmpJoinTables(table_names, ctx.DB); err != nil {
		return ret, err
	}

	ret.Filename = tmp_filename
	ret.Table_meta = final_meta

	return ret, nil
}

func generateJoinSql(file_list meta.TempResultList, table_list []string, conds []string) string {
	select_sql := "SELECT"
	attrs := " "
	table_refs := " "
	predicates := " "
	first := true

	for i := 0; i < len(file_list); i++ {
		table_meta := file_list[i].Table_meta
		for j := 0; j < len(table_meta.Columns); j++ {
			attr_name := table_list[i] + "." + table_meta.Columns[j].ColumnName
			if first {
				attrs += attr_name
				first = false
			} else {
				attrs += ", " + attr_name
			}
		}
	}

	first = true
	for i := 0; i < len(table_list); i++ {
		if first {
			table_refs += "FROM " + table_list[i]
			first = false
		} else {
			table_refs += ", " + table_list[i]
		}
	}

	first = true
	for i := 0; i < len(conds); i++ {
		if first {
			predicates += "WHERE " + conds[i]
			first = false
		} else {
			predicates += " AND " + conds[i]
		}
	}

	select_sql = select_sql + attrs + table_refs + predicates
	return select_sql
}

func dropTmpJoinTables(table_list []string, db *sql.DB) error {
	var err error

	for i := 0; i < len(table_list); i++ {
		err = dropTmpTable(table_list[i], db)
	}

	return err
}

func findColumn(table_meta meta.TableMeta, table_name string, column_name string) int {
	for i := 0; i < len(table_meta.Columns); i++ {
		if strings.EqualFold(table_meta.Columns[i].ColumnName, table_name+"$"+column_name) {
			return i
		}
	}
	return -1
}

func handleUnionOperation(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, file_list meta.TempResultList) (meta.TempResult, error) {
	var (
		err error
		ret meta.TempResult
	)
	ret.Filename = ""

	l := ctx.Logger

	u, _ := user.Current()

	is_vaild, table_meta := checkUnionSchema(file_list)
	if !is_vaild {
		return ret, errors.New("tables in the file list are not the same")
	}
	ret.Table_meta = table_meta

	// create tmp table
	tmp_table_name := "INTER_TMP_" + strconv.Itoa(node.NodeId) + "_" + strconv.Itoa(int(time.Now().Unix()))
	if err = createTmpTable(tmp_table_name, table_meta, ctx.DB, true); err != nil {
		return ret, err
	}

	for i := 0; i < len(file_list); i++ {
		filename := file_list[i].Filename
		if len(filename) == 0 {
			l.Error("execute sub query id: ", i)
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, errors.New("execute sub query failed")
		}

		filepath := "/home/" + u.Username + "/" + filename
		dst_path := "/tmp/data/" + filename
		if err = utils.CpFile(filepath, dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		if err = utils.Chown("mysql", dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		// insert into tmp table
		if err = insertIntoTable(dst_path, tmp_table_name, ctx.DB); err != nil {
			l.Errorln("insert into tmp table ", tmp_table_name, ", error is ", err.Error())
			dropTmpTable(tmp_table_name, ctx.DB)
			return ret, err
		}

		if err = utils.RmFile(dst_path, false); err != nil {
			dropTmpTable(tmp_table_name, ctx.DB)
			l.Errorln("remove tmp file")
		}
	}

	// select into tmp file
	tmp_filename := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(node.NodeId) + ".csv"
	tmp_filepath_1 := "/tmp/data/" + tmp_filename
	tmp_filepath_2 := "/home/" + u.Username + "/" + tmp_filename

	if err = selectIntoFile(node, tmp_filepath_1, tmp_table_name, ctx.DB); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return ret, err
	}

	if err = utils.Chown(u.Username, tmp_filepath_1, false); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return ret, err
	}

	if err = utils.MvFile(tmp_filepath_1, tmp_filepath_2, false); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return ret, err
	}

	if err = dropTmpTable(tmp_table_name, ctx.DB); err != nil {
		return ret, err
	}

	ret.Filename = tmp_filename
	return ret, nil
}

func createTmpTable(table_name string, table_meta meta.TableMeta, db *sql.DB, save_to_etcd bool) error {
	create_sql := "CREATE TABLE " + table_name

	table_fields := "("

	for idx, col := range table_meta.Columns {
		type_str, err := meta.FieldType2String(col.Type)
		if err != nil {
			return err
		}
		if idx == len(table_meta.Columns)-1 {

			table_fields += col.ColumnName + " " + type_str
			break
		}
		table_fields += col.ColumnName + " " + type_str + ", "
	}

	table_fields += ")"

	create_sql += table_fields
	_, err := db.Exec(create_sql)
	if err != nil {
		fmt.Println(err, create_sql)
	}
	return err
}

func dropTmpTable(table_name string, db *sql.DB) error {
	drop_sql := "DROP TABLE " + table_name + ";"
	_, err := db.Exec(drop_sql)
	if err != nil {
		fmt.Println(err, drop_sql)
	}
	return err
}

func insertIntoTable(filepath string, table_name string, db *sql.DB) error {
	load_data_sql := utils.GenerateDataLoaderSql2(filepath, table_name)
	_, err := db.Exec(load_data_sql)
	if err != nil {
		fmt.Println(err, load_data_sql)
	}
	return err
}

func selectIntoFile(node *plan.PlanTreeNode, filepath string, table_name string, db *sql.DB) error {
	attributes := "*"
	references := ""
	predicates := ""

	select_sql := "SELECT "

	for i := 0; i < len(node.ColsName); i++ {
		if i == 0 {
			attributes = node.ColsName[i]
		} else {
			attributes = attributes + ", " + node.ColsName[i]
		}
	}

	if len(table_name) != 0 {
		references = " FROM " + table_name
	}

	for i := 0; i < len(node.ConditionsStr); i++ {
		if i == 0 {
			predicates = " WHERE " + node.ConditionsStr[i]
		} else {
			predicates += (" AND " + node.ConditionsStr[i])
		}
	}

	select_sql = select_sql + attributes + references + predicates
	select_sql = utils.GenerateSelectIntoFileSql(select_sql, filepath, "|", "")
	res, err := db.Exec(select_sql)
	if err != nil {
		fmt.Println(err, select_sql)
	}
	row_cnt, _ := res.RowsAffected()
	node.Status = int(row_cnt)

	return err
}

func checkUnionSchema(table_list meta.TempResultList) (bool, meta.TableMeta) {
	var failed_res meta.TableMeta

	if len(table_list) == 0 {
		return false, failed_res
	}

	origin_table_meta := table_list[0].Table_meta
	for i := 1; i < len(table_list); i++ {
		if !origin_table_meta.EqualTableMeta(table_list[i].Table_meta) {
			return false, failed_res
		}
	}

	return true, origin_table_meta
}

type condition struct {
	left_table_name   string
	left_column_name  string
	right_table_name  string
	right_column_name string
	Compress          bool
	ConditionStr      string
}

func (c condition) GetColumnName() (string, string, string, string) {
	return c.left_table_name, c.left_column_name, c.right_table_name, c.right_column_name
}

func (c *condition) SetConditionField(is_left bool, table_name string, column_name string) {
	if is_left {
		c.left_column_name = column_name
		c.left_table_name = table_name
	} else {
		c.right_column_name = column_name
		c.right_table_name = table_name
	}
}

func (c *condition) SetConditionStr(str string) {
	c.ConditionStr = str
}

func (c condition) GetConditionStr() string {
	return c.left_table_name + "." + c.left_column_name + "=" + c.right_table_name + "." + c.right_column_name
}

func (c *condition) NeedCompress() bool {
	// 前提是等值连接
	if utils.ContainString(c.left_table_name+"_"+c.left_column_name, c.right_table_name+"_"+c.right_column_name, true) {
		// utils.ContainString(c.left_column_name, c.right_table_name+"_"+c.right_column_name, true) ||
		// utils.ContainString(c.right_column_name, c.left_table_name+"_"+c.left_column_name, true) {
		c.Compress = true
		return true
	}
	c.Compress = false
	return false
}

func parseCondition(conditions []string) []condition {
	var conds []condition
	for i := 0; i < len(conditions); i++ {
		var cond condition
		col_list := strings.Split(strings.Trim(conditions[i], " "), "=")
		if len(col_list) == 2 {
			l_fields := strings.Split(strings.Trim(col_list[0], " "), ".")
			cond.SetConditionField(true, l_fields[0], l_fields[1])
			r_fields := strings.Split(strings.Trim(col_list[1], " "), ".")
			cond.SetConditionField(false, r_fields[0], r_fields[1])
			cond.SetConditionStr(conditions[i])
		}
		conds = append(conds, cond)
	}

	return conds
}
