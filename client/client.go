package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/user"
	cfg "project/config"
	"project/meta"
	utils "project/utils"
	"strings"
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
		// fmt.Println(input)
		if strings.Contains(strings.ToLower(input), "exit") ||
			strings.Contains(strings.ToLower(input), "quit") {
			fmt.Println("Bye.")
			break
		}

		_, err = conn.Write([]byte(input))
		if err != nil {
			log.Fatalln("when write conn, conn closed", err.Error())
			break
		}
		// wait_for_res
		// max_buf_size := 100 * (1 << 20)
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			log.Fatalln("read error")
		}

		//
		var resp meta.BackToClient
		if err := json.Unmarshal(buf[:n], &resp); err != nil {
			fmt.Println("unmarshal failed, package is broken")
			continue
		}

		displayResult(resp)
	}
}

func displayResult(resp meta.BackToClient) {
	if len(resp.Error) != 0 {
		fmt.Println(resp.Error)
	}

	var (
		row_count int
		filepath  string
	)

	u, _ := user.Current()

	if len(resp.ResponseStr) > 0 {
		// show partitions
		fmt.Println(resp.ResponseStr)
	} else {
		if resp.Filepath != "" {
			filepath = resp.Filepath
		} else if resp.Filename != "" {
			filepath = "/home/" + u.Username + "/" + resp.Filename
		} else {
			filepath = ""
		}

		if filepath != "" {
			row_count = 0
			if file, err := os.Open(filepath); err != nil {
				log.Fatalln("open file failed", filepath, resp.Filename, err)
			} else {
				fileScanner := bufio.NewScanner(file)

				// read line by line
				for fileScanner.Scan() {
					fmt.Println(fileScanner.Text())
					row_count++
				}
				// handle first encountered error while reading
				if err := fileScanner.Err(); err != nil {
					log.Fatalf("Error while reading file: %s", err)
				}
			}
			fmt.Println("Total row number: ", row_count, ".")
		} else {
			fmt.Println("Success.")
		}
	}

	fmt.Println("Exec time: ", float32(resp.ExecTime.Microseconds())/1000/1000, "s.")
}
