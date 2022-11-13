package main

import (
	"fmt"
	"os"
	cfg "project/config"
	core "project/core"
	mysql "project/mysql"
	utils "project/utils"
	logger "project/utils/log"
)

func main() {
	// fill context with command args
	fmt.Println("命令行参数数量:", len(os.Args))
	for k, v := range os.Args {
		fmt.Printf("args[%v]=[%v]\n", k, v)
	}
	var ctx cfg.Context
	utils.ParseArgs(&ctx)
	// init log level, log file...
	logger.LoggerInit(&ctx)
	// a test connection to db engine
	mysql.SQLDriverInit(&ctx)
	// start coordinator <worker, socket_input, socket_dispatcher>
	c := core.NewCoordinator(&ctx)

	c.Start()
	// wait for terminal
}
