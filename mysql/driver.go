package mysql

import (
	"database/sql"
	"fmt"
	cfg "project/config"

	_ "github.com/go-sql-driver/mysql"
)

// connect to local database
func SQLDriverInit(ctx *cfg.Context) {
	l := ctx.Logger
	dataSourceName := fmt.Sprintf("%s:Bigdata123!@#@tcp(%s:%s)/%s?charset=utf8", ctx.DB_name, ctx.DB_host, ctx.DB_port, ctx.DB_name)
	driverName := ctx.DB_type
	l.Debugln("dataSourceName", dataSourceName)
	l.Debugln("driverName", driverName)
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		l.Fatalln("connection failed!")
		return
	}
	err = db.Ping()
	if err != nil {
		l.Errorln("db connect failed. err: ", err.Error())
		return
	}
	l.Infoln("connect to", driverName)
	ctx.DB = db

}
