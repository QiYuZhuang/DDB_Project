package main

import (
	"fmt"
	"log"
	"net"
	cfg "project/config"
	utils "project/utils"
)

func main() {
	// print the title
	fmt.Println("welcome to ddb")
	// connect to server port = 10900
	var ctx cfg.Context
	utils.ParseArgs(&ctx)

	ctx.DB_port = "10900"
	address := fmt.Sprintf("%s:%s", ctx.DB_host, ctx.DB_port)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalln("client dial failed.", err)
		return
	}
	defer conn.Close()
	// for-loop to
	for {
		fmt.Print("ddb> ")
		buf := make([]byte, 1024)
		fmt.Scanln(&buf)
		_, err = conn.Write(buf)
		if err != nil {
			log.Fatalln("when write conn, conn closed", err.Error())
			break
		}
	}
}
