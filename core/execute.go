package core

import (
	cfg "project/config"
	"project/meta"
)

type Executor struct {
	Context *cfg.Context
}

func NewExecutor(c *Coordinator) *Executor {
	return &Executor{
		Context: c.Context,
	}
}

func LocalExecSql(sql_id int, txn *meta.Transaction, sql string, c *Coordinator) {
	l := c.Context.Logger
	// active_trans := c.ActiveTransactions
	db := c.Context.DB
	err := db.Ping()
	if err != nil {
		l.Errorln("db connect failed. err: ", err.Error())
		return
	}
	l.Infoln("local exec sql: ", sql)
	res, err := db.Exec(sql)
	if err != nil {
		l.Errorln("local exec failed. err: ", err.Error())
	}
	txn.Results[sql_id] = res
}
