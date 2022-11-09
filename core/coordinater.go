package core

import (
	"bufio"
	"container/list"
	"fmt"
	"io"
	"net"
	"os"
	cfg "project/config"
	"strings"
	"time"
)

type Coordinator struct {
	id                int16
	peers             []Peer
	InputSockets      []net.Conn
	DispatcherSockets []net.Conn
	InputMessages     *list.List
	DispatchMessages  *list.List
	context           *cfg.Context
}

type Peer struct {
	id   int16
	ip   string
	port string
}

func (c *Coordinator) find_peers(filename string) {
	l := c.context.Logger
	str, _ := os.Getwd()
	l.Debugln("temp path is", str)
	file, err := os.OpenFile(c.context.Peer_file, os.O_RDWR, 0666)
	if err != nil {
		l.Fatalln("Open file error!", err)
		return
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	var machine_id = int16(0)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			if err == io.EOF {
				l.Infoln("File read ok!")
				break
			} else {
				l.Fatalln("Read file error!", err)
				return
			}
		}
		if line[0] != '[' {
			l.Debugln(line)
			arr := strings.Fields(line)
			if arr[0] == c.context.DB_host {
				c.id = machine_id
			}
			p := Peer{
				id:   machine_id,
				ip:   arr[0],
				port: arr[1],
			}
			c.peers = append(c.peers, p)
		}
		machine_id++
	}
}

func NewCoordinator(ctx *cfg.Context) *Coordinator {
	c := Coordinator{}
	c.context = ctx
	c.find_peers(ctx.Peer_file)

	return &c
}

func (c *Coordinator) connect_to_peers() {
	l := c.context.Logger
	// init message list
	c.DispatchMessages = new(list.List)
	c.InputMessages = new(list.List)
	// create dispatcher sockets
	l.Infoln("Start to create dispatcher sockets.")
	CreateDispatcherSockets(c)
	l.Infoln("Create dispatcher sockets success.")
	// create input sockets
	time.Sleep(30 * 1000)
	l.Infoln("Start to create input sockets.")
	CreateInputSockets(c)
	l.Infoln("Create input sockets success.")
}

func (c *Coordinator) LocalConnectionHandler(conn net.Conn) {
	l := c.context.Logger
	buf := make([]byte, 4096)
	defer conn.Close()
	for {
		l.Debugln("wait for local query")
		n, err := conn.Read(buf)
		if err != nil {
			l.Debugln("when read conn, conn closed", err.Error())
			break
		}
		l.Debugln("sql:", n)
		panic("handle sql, not implement now")
	}
}

func (c *Coordinator) wait_for_local_connection() {
	l := c.context.Logger
	port := "10900"
	address := fmt.Sprintf("%s:%s", c.context.DB_host, port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		l.Error("client listen failed.", err)
		return
	}
	defer listener.Close()

	for {
		new_conn, err := listener.Accept()
		if err != nil {
			l.Error("local client accept failed", err.Error())
		}
		go c.LocalConnectionHandler(new_conn)
	}
}

func (c *Coordinator) Start() {
	c.connect_to_peers()
	// create a goroutine, which listen to local connection
	c.wait_for_local_connection()
}
