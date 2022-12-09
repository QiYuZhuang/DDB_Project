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

func LocalExecSql(ctx *ExecuteContext, query_type meta.StmtType) {
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
	if query_type == meta.LoadDataStmtType {
		err = utils.Chown("mysql", ctx.filepath, false)
		if err != nil {
			l.Errorln("chown failed, error is ", err.Error())
		}

		_, err := db.Exec(ctx.sql)
		if err != nil {
			l.Errorln("local exec failed. err: ", err.Error())
		}

		err = utils.RmFile(ctx.filepath, false)
		if err != nil {
			l.Errorln("delete failed, error is ", err.Error())
		}
	} else if query_type == meta.SelectStmtType {
		rows, err := db.Query(ctx.sql)
		if err != nil {
			l.Errorln("local exec failed. err: ", err.Error())
		}

		txn.EffectRows[ctx.sql_id] = ParseRows(rows)
	} else {
		_, err := db.Exec(ctx.sql)
		if err != nil {
			l.Errorln("local exec failed. err: ", err.Error())
		}
	}
	txn.Responses[ctx.sql_id] = true
	txn.Error = err
}

func LocalExecDataLoad(ctx meta.Context, sql string, filepath string) error {
	l := ctx.Logger
	// active_trans := c.ActiveTransactions
	db := ctx.DB

	err := db.Ping()
	if err != nil {
		l.Errorln("db connect failed. err: ", err.Error())
		return err
	}
	l.Infoln("local exec sql: ", sql)

	err = utils.Chown("mysql", filepath, false)
	if err != nil {
		l.Errorln("chown failed, error is ", err.Error())
	}

	_, err = db.Exec(sql)
	if err != nil {
		l.Errorln("local exec failed. err: ", err.Error())
		return err
	}

	err = utils.RmFile(filepath, false)
	if err != nil {
		l.Errorln("delete failed, error is ", err.Error())
		return err
	}

	return nil
}

func LocalExecInternalSql(ctx *meta.Context, sql string, filepath string, query_type meta.StmtType) error {
	// file path is used for batch insert
	l := ctx.Logger
	// active_trans := c.ActiveTransactions
	db := ctx.DB

	err := db.Ping()
	if err != nil {
		l.Errorln("db connect failed. err: ", err.Error())
		return err
	}
	l.Infoln("local exec sql: ", sql)

	if query_type == meta.LoadDataStmtType {
		err = utils.Chown("mysql", filepath, false)
		if err != nil {
			l.Errorln("chown failed, error is ", err.Error())
			return err
		}
	}

	_, err = db.Exec(sql)
	if err != nil {
		l.Errorln("local exec failed. err: ", err.Error())
		return err
	}

	if query_type == meta.LoadDataStmtType {
		err = utils.RmFile(filepath, false)
		if err != nil {
			l.Errorln("delete failed, error is ", err.Error())
			return err
		}
	}

	return nil
}
