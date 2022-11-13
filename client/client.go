package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	cfg "project/config"
	utils "project/utils"
)

func main() {
	// print the title
	fmt.Println("welcome to ddb")
	// connect to server port = 10900
	var ctx cfg.Context
	utils.ParseArgs(&ctx)
	inputReader := bufio.NewReader(os.Stdin)

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

		input, err := inputReader.ReadString('\n')
		if err != nil {
			log.Fatalln("read error", err.Error())
			break
		}

		_, err = conn.Write([]byte(input))
		if err != nil {
			log.Fatalln("when write conn, conn closed", err.Error())
			break
		}
		// wait_for_res
		buf := make([]byte, 1024)
		conn.Read(buf)
		fmt.Println(buf)
	}
}
