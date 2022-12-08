package mysql

import (
	"database/sql"
	"errors"
	"os/user"
	"project/meta"
	"project/plan"
	"project/utils"
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
	internal_results := make(chan string, 20)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	err = execPlanNode(ctx, txn, node, internal_results, wg)
	wg.Wait()
	close(internal_results)
	for f := range internal_results {
		l.Info("query result is in ", f)
		filename = f
	}
	return filename, err
}

func execPlanNode(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, internal_results chan string, wg *sync.WaitGroup) error {
	defer wg.Done()

	l := ctx.Logger

	if node == nil {
		return nil
	}

	query_id := node.NodeId
	u, _ := user.Current()

	if node.GetChildrenNum() == 0 {
		var (
			is_remote  bool
			select_sql string
		)

		is_remote = !utils.ContainString(ctx.IP, node.ExecuteSiteIP, false)

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
		}

		if len(node.FromTableName) != 0 {
			references = " FROM " + node.FromTableName
		}

		for i := 0; i < len(node.ConditionsStr); i++ {
			if i == 0 {
				predicates = " WHERE " + node.ConditionsStr[i]
			} else {
				predicates += (" AND " + node.ConditionsStr[i])
			}
		}

		select_sql = select_sql + attributes + references + predicates + ";"

		// tmp path is `/home/<username>`, filename is `TMP_<IP>_<table_name>.csv`
		if is_remote {
			// create message
			m := meta.NewQueryRequestMessage(ctx.IP, node.ExecuteSiteIP, txn.TxnId)
			m.SetQueryId(query_id)
			m.SetQuery(select_sql)

			// flush messages
			*ctx.Messages <- *m

			// wait for reply3
			for !txn.Responses[query_id] {
				time.Sleep(time.Duration(10) * time.Nanosecond)
			}

			// mv tmp file to tmp path
			tmp_filename := txn.TmpResultInFile[query_id]

			internal_results <- tmp_filename
		} else {
			tmp_filename := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(query_id) + ".csv"
			raw_path := "/tmp/data/" + tmp_filename
			tmp_path := "/home/" + u.Username + "/" + tmp_filename
			select_sql := utils.GenerateSelectIntoFileSql(strings.Trim(select_sql, ";"), raw_path, "|", "")
			if err := LocalExecInternalSql(ctx, select_sql, "", meta.SelectIntoFileStmtType); err != nil {
				internal_results <- ""
				l.Errorln("interal execute failed", err.Error())
				return err
			}

			if err := utils.Chown(u.Username, raw_path, false); err != nil {
				internal_results <- ""
				l.Errorln("change owner failed", err.Error())
				return err
			}

			if err := utils.MvFile(raw_path, tmp_path, false); err != nil {
				internal_results <- ""
				l.Errorln("move file failed", err.Error())
				return err
			}

			internal_results <- tmp_filename
		}
	} else {
		// create new wait group and threads
		// thread:
		// 		- generate sqls
		//		- flush messages
		// 		- wait for all children finished
		inner_wg := new(sync.WaitGroup)
		inner_channel := make(chan string, 20)
		inner_wg.Add(node.GetChildrenNum())

		for i := 0; i < node.GetChildrenNum(); i++ {
			go execPlanNode(ctx, txn, node.GetChild(i), inner_channel, inner_wg)
		}

		inner_wg.Wait()
		close(inner_channel)

		var file_list []string
		for c := range inner_channel {
			file_list = append(file_list, c)
		}

		// exec union or join
		if node.Type == plan.UnionType {
			filepath, err := handleUnionOperation(ctx, txn, node, file_list)
			if err != nil {
				l.Errorln("handle union operation failed", err)
				return err
			}
			internal_results <- filepath
		} else if node.Type == plan.JoinType {
			filepath, err := handleJoinOperation(ctx, txn, node, file_list)
			if err != nil {
				l.Errorln("handle join operation failed", err)
				return err
			}
			internal_results <- filepath
		} else if node.Type == plan.SelectType || node.Type == plan.ProjectionType {
			if len(file_list) != 1 {
				l.Errorln("handle invaild operation")
			}
			internal_results <- file_list[0]
		}
	}
	return nil
}

func handleJoinOperation(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, file_list []string) (string, error) {
	partition_meta, err := plan.GetPartitionMeta(*ctx, node.FromTableName)
	if err != nil {
		return "", errors.New("cannot find partition meta")
	}

	if partition_meta.PartitionType == meta.Horizontal {
		return "", errors.New("partition type is not expected")
	}

	// u, _ := user.Current()
	var filepath string

	for i := 0; i < len(file_list); i++ {
	}
	return filepath, nil
}

func handleUnionOperation(ctx *meta.Context, txn *meta.Transaction, node *plan.PlanTreeNode, file_list []string) (string, error) {
	l := ctx.Logger
	table_name := node.FromTableName

	table_meta, partition_meta, err := plan.FindMetaInfo(*ctx, table_name)
	if err != nil {
		return "", errors.New("cannot find partition meta")
	}

	if partition_meta.FragType != meta.Horizontal {
		return "", errors.New("partition type is not expected")
	}

	u, _ := user.Current()

	// create tmp table
	tmp_table_name := "INTER_TMP_" + table_name + "_" + strconv.Itoa(int(time.Now().Unix()))
	if err = createTmpTable(tmp_table_name, table_meta, ctx.DB); err != nil {
		return "", err
	}

	for i := 0; i < len(file_list); i++ {
		filename := file_list[i]
		if len(filename) == 0 {
			l.Error("execute sub query id: ", i)
			return "", errors.New("execute sub query failed")
		}

		filepath := "/home/" + u.Username + "/" + filename
		dst_path := "/tmp/data/" + filename
		if err = utils.MvFile(filepath, dst_path, false); err != nil {
			return "", err
		}

		if err = utils.Chown("mysql", dst_path, false); err != nil {
			return "", err
		}

		// insert into tmp table
		if err = insertIntoTable(dst_path, tmp_table_name, ctx.DB); err != nil {
			l.Errorln("insert into tmp table ", tmp_table_name, ", error is ", err.Error())
			dropTmpTable(tmp_table_name, ctx.DB)
			return "", err
		}

		if err = utils.RmFile(dst_path, false); err != nil {
			l.Errorln("remove tmp file")
		}
	}

	// select into tmp file
	tmp_filename := "INTER_TMP_" + strconv.Itoa(int(txn.TxnId)) + "_" + strconv.Itoa(node.NodeId) + ".csv"
	tmp_filepath_1 := "/tmp/data/" + tmp_filename
	tmp_filepath_2 := "/home/" + u.Username + "/" + tmp_filename

	if err = selectIntoFile(node, tmp_filepath_1, tmp_table_name, ctx.DB); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return "", err
	}

	if err := utils.Chown(u.Username, tmp_filepath_1, false); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return "", err
	}

	if err = utils.MvFile(tmp_filepath_1, tmp_filepath_2, false); err != nil {
		dropTmpTable(tmp_table_name, ctx.DB)
		return "", err
	}

	if err = dropTmpTable(tmp_table_name, ctx.DB); err != nil {
		return "", err
	}

	return tmp_filename, nil
}

func createTmpTable(table_name string, table_meta meta.TableMeta, db *sql.DB) error {
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
	return err
}

func dropTmpTable(table_name string, db *sql.DB) error {
	drop_sql := "DROP TABLE " + table_name + ";"
	_, err := db.Exec(drop_sql)
	return err
}

func insertIntoTable(filepath string, table_name string, db *sql.DB) error {
	load_data_sql := utils.GenerateDataLoaderSql2(filepath, table_name)
	_, err := db.Exec(load_data_sql)
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
	_, err := db.Exec(select_sql)
	return err
}
