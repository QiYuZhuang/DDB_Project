package mysql

import (
	"database/sql"
	"project/meta"
	"project/utils"

	"github.com/sirupsen/logrus"
)

type ExecuteContext struct {
	sql_id   int
	sql      string
	filepath string
	db       *sql.DB
	logger   *logrus.Logger
	txn      *meta.Transaction
}

func CreateExecuteContext(sql_id int, sql string, filepath string, db *sql.DB, logger *logrus.Logger, txn *meta.Transaction) *ExecuteContext {
	return &ExecuteContext{
		sql_id:   sql_id,
		sql:      sql,
		filepath: filepath,
		db:       db,
		logger:   logger,
		txn:      txn,
	}
}

func LocalExecSql(ctx *ExecuteContext, is_select bool) {
	l := ctx.logger
	// active_trans := c.ActiveTransactions
	db := ctx.db
	txn := ctx.txn
	err := db.Ping()
	if err != nil {
		l.Errorln("db connect failed. err: ", err.Error())
		return
	}
	l.Infoln("local exec sql: ", ctx.sql)
	is_data_loader := ctx.filepath != ""
	if !is_select {
		if is_data_loader {
			err = utils.Chown("mysql", ctx.filepath, false)
			if err != nil {
				l.Errorln("chown failed, error is ", err.Error())
			}
		}
		_, err := db.Exec(ctx.sql)
		if err != nil {
			l.Errorln("local exec failed. err: ", err.Error())
		}
		if is_data_loader {
			err = utils.RmFile(ctx.filepath, false)
			if err != nil {
				l.Errorln("delete failed, error is ", err.Error())
			}
		}

		// txn.Results[sql_id] = res
	} else {
		rows, err := db.Query(ctx.sql)
		if err != nil {
			l.Errorln("local exec failed. err: ", err.Error())
		}

		txn.QueryResult[ctx.sql_id], txn.EffectRows[ctx.sql_id] = ParseRows(rows)
	}
	txn.Responses[ctx.sql_id] = true
	txn.Error = err
}
