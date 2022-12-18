package plan

import (
	"database/sql"
	"errors"
	"fmt"
	"os/user"
	"project/meta"
	"project/utils"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
)

func HandleLoadDataInfile(ctx meta.Context, stmt ast.StmtNode) ([]meta.SqlRouter, error) {
	l := ctx.Logger

	var (
		ret []meta.SqlRouter
	)
	sql := stmt.Text()
	l.Infoln(sql)
	binsert := stmt.(*ast.LoadDataStmt) // batch insert
	table_name := binsert.Table.Name.O
	// find meta
	table_meta, partition_meta, err := FindMetaInfo(ctx, table_name)
	if err != nil {
		return ret, err
	}

	tmp_table_name := "TMP_" + table_meta.TableName
	if err = createTmpTable(tmp_table_name, table_meta, ctx.DB); err != nil {
		l.Errorln("create tmp table failed, error is ", err.Error())
		err2 := dropTmpTable(tmp_table_name, ctx.DB)
		if err2 != nil {
			l.Errorln("Drop tmp table failed.")
		}
		return ret, err
	}

	// INSERT INTO TMP_<TABLE_NAME>
	if err = insertIntoTable(binsert.Path, tmp_table_name, ctx.DB); err != nil {
		l.Errorln("insert into tmp table failed, error is ", err.Error())
		err2 := dropTmpTable(tmp_table_name, ctx.DB)
		if err2 != nil {
			l.Errorln("Drop tmp table failed.")
		}
		return ret, err
	}

	if partition_meta.FragType == meta.Horizontal {
		// split the file according to partition infos
		for idx, p := range partition_meta.HFragInfos {
			r, err := fragDataLoadHandle(ctx, partition_meta, p.FragName, tmp_table_name, idx)
			if err != nil {
				l.Errorln("split tmp table failed.")
				err2 := dropTmpTable(tmp_table_name, ctx.DB)
				if err2 != nil {
					l.Errorln("Drop tmp table failed.")
				}
				return ret, err
			}
			ret = append(ret, r)
		}
	} else if partition_meta.FragType == meta.Vertical {
		for idx, p := range partition_meta.VFragInfos {
			r, err := fragDataLoadHandle(ctx, partition_meta, p.FragName, tmp_table_name, idx)
			if err != nil {
				l.Errorln("split tmp table failed.")
				err2 := dropTmpTable(tmp_table_name, ctx.DB)
				if err2 != nil {
					l.Errorln("Drop tmp table failed.")
				}
				return ret, err
			}
			ret = append(ret, r)
		}
	} else {
		l.Errorln("not support fragment strategy")
		return ret, errors.New("invaild fragment strategy")
	}

	if err = dropTmpTable(tmp_table_name, ctx.DB); err != nil {
		l.Errorln("Drop tmp table failed.")
		return ret, err
	}

	return ret, nil
}

func fragDataLoadHandle(ctx meta.Context, partition_meta meta.Partition, frag_name string, table_name string, idx int) (meta.SqlRouter, error) {
	l := ctx.Logger
	u, _ := user.Current()

	var ret meta.SqlRouter

	tmp_filepath := "/tmp/data/" + "TMP_" + frag_name + ".csv"
	if err := selectIntoFile(tmp_filepath, partition_meta, table_name, frag_name, ctx.DB); err != nil {
		l.Errorln("select into file failed, error is ", err.Error())
		if err2 := dropTmpTable(table_name, ctx.DB); err2 != nil {
			l.Errorln("Drop tmp table failed.")
		}
		return ret, err
	}

	if err := utils.Chown(u.Username, tmp_filepath, false); err != nil {
		l.Errorln("chown failed, error is ", err)
		return ret, err
	}

	if err := sendToDestMachine(u.Username, tmp_filepath, ctx.IP, partition_meta.SiteInfos[idx]); err != nil {
		l.Errorln("send file, error is ", err)
		if err2 := dropTmpTable(table_name, ctx.DB); err2 != nil {
			l.Errorln("Drop tmp table failed.")
		}
		return ret, err
	}
	data_load_sql := utils.GenerateDataLoaderSql(tmp_filepath, frag_name)
	router := meta.SqlRouter{
		File_path: tmp_filepath,
		Sql:       data_load_sql,
		Site_ip:   partition_meta.SiteInfos[idx].IP,
		Site_port: partition_meta.SiteInfos[idx].Port,
	}

	return router, nil
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
	load_data_sql := utils.GenerateDataLoaderSql(filepath, table_name)
	_, err := db.Exec(load_data_sql)
	return err
}

func selectIntoFile(filepath string, metas meta.Partition, table_name string, partition_name string, db *sql.DB) error {
	predicates := " "
	attributes := "*"
	a, b, _ := GetFilterCondition(metas, partition_name)
	fmt.Println(a, b)
	for i := 0; i < len(a); i++ {
		if i == 0 {
			predicates += "WHERE "
		} else {
			predicates += " AND "
		}
		predicates += a[i]
	}

	for i := 0; i < len(b); i++ {
		if i == 0 {
			attributes = b[i]
		} else {
			attributes = attributes + " ," + b[i]
		}
	}

	sql := "SELECT " + attributes + " FROM " + table_name + predicates
	sql = utils.GenerateSelectIntoFileSql(sql, filepath, "\t", "\"")
	_, err := db.Exec(sql)
	return err
}

func sendToDestMachine(username string, filepath string, src string, site meta.SiteInfo) error {
	if !utils.ContainString(filepath, site.FragName, true) {
		return errors.New("filepath and fragment name are not matched")
	}

	if strings.EqualFold(src, site.IP) {
		return nil
	}

	// scp send to remote file system
	dest_path := "/home/" + username
	err := utils.ScpFile(username, dest_path, filepath, site.IP, false)
	if err != nil {
		return err
	}

	err = utils.RmFile(filepath, false)
	if err != nil {
		return err
	}

	return nil
}
