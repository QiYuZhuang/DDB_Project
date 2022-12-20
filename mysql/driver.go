package mysql

import (
	"database/sql"
	"fmt"
	cfg "project/config"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// connect to default database
func SQLDriverInit(ctx *cfg.Context) {
	l := ctx.Logger
	if strings.EqualFold(ctx.ServerPort, "10800") {
		ctx.DB_port = "3306"
	} else {
		ctx.DB_port = "3307"
	}
	// dataSourceName := fmt.Sprintf("%s:Bigdata123!@#@tcp(%s:%s)/%s?charset=utf8", ctx.DB_name, ctx.DB_host, ctx.DB_port, ctx.DB_name)
	dataSourceName := fmt.Sprintf("%s:Bigdata123!@#@tcp(%s:%s)/?charset=utf8", ctx.DB_name, ctx.DB_host, ctx.DB_port)
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
	l.Infoln("connect to", driverName, ctx.DB_host, ctx.DB_port)
	ctx.DB = db

}

// connect to default database
func SQLDriverRefresh(ctx *cfg.Context, database_name string) {
	l := ctx.Logger
	if strings.EqualFold(ctx.ServerPort, "10800") {
		ctx.DB_port = "3306"
	} else {
		ctx.DB_port = "3307"
	}
	dataSourceName := fmt.Sprintf("%s:Bigdata123!@#@tcp(%s:%s)/%s?charset=utf8", ctx.DB_name, ctx.DB_host, ctx.DB_port, database_name)
	// dataSourceName := fmt.Sprintf("%s:Bigdata123!@#@tcp(%s:%s)/?charset=utf8", ctx.DB_name, ctx.DB_host, ctx.DB_port)
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
	l.Infoln("connect to", driverName, ctx.DB_host, ctx.DB_port)
	ctx.DB = db

}
