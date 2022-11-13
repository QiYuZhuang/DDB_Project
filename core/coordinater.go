package core

import (
	"bufio"
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	cfg "project/config"
	"project/meta"
	"project/plan"
	"sync"

	"strings"
	"time"
)

const MaxPeerNum = 10

type Coordinator struct {
	Id                  int16
	Peer_num            int16
	Peers               [MaxPeerNum]meta.Peer
	InputSockets        [MaxPeerNum]net.Conn
	DispatcherSockets   [MaxPeerNum]net.Conn
	InputMessages       [MaxPeerNum]list.List
	DispatchMessages    [MaxPeerNum]list.List
	Context             *cfg.Context
	TableMetas          meta.TableMetas
	Partitions          meta.Partitions
	GlobalTransactionId uint64
	ActiveTransactions  map[uint64]*meta.Transaction

	d_mutex sync.Mutex
	t_mutex sync.Mutex // for global_transaction_id
	// i_mutex sync.Mutex
}

func (c *Coordinator) FindPeers(filename string) {
	l := c.Context.Logger
	str, _ := os.Getwd()
	l.Infoln("temp path is", str)
	file, err := os.OpenFile(c.Context.Peer_file, os.O_RDWR, 0666)
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
			if arr[0] == c.Context.DB_host {
				c.Id = machine_id
			}
			p := meta.Peer{
				Id:   machine_id,
				Ip:   arr[0],
				Port: arr[1],
			}
			c.Peers[machine_id] = p
			machine_id++
		}
	}
	c.Peer_num = machine_id
}

func NewCoordinator(ctx *cfg.Context) *Coordinator {
	c := Coordinator{}
	c.Context = ctx
	c.FindPeers(ctx.Peer_file)

	// TODO:
	jsonFileDir := "config/partition.json"
	jsonFile, err := os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")
	byteValue, _ := ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	json.Unmarshal([]byte(byteValue), &c.Partitions)
	fmt.Println(c.Partitions)
	////

	// read table meta info
	jsonFileDir = "config/table_meta.json"
	jsonFile, err = os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")

	byteValue, _ = ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	json.Unmarshal([]byte(byteValue), &c.TableMetas)
	fmt.Println(c.TableMetas)

	c.ActiveTransactions = make(map[uint64]*meta.Transaction)
	return &c
}

func (c *Coordinator) connect_to_peers() {
	l := c.Context.Logger
	// init message list
	// c.DispatchMessages = new([]list.List)
	// c.InputMessages = new([]list.List)
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
	l := c.Context.Logger
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
		// panic("handle sql, not implement now")
		// parser_tree := parser(buf)

		// 创建新事务
		txn := NewTransaction(string(buf), c)
		ctx := plan.Context{
			TableMetas:      c.TableMetas,
			TablePartitions: c.Partitions,
			Peers:           c.Peers[:],
		}
		_, sqls, err := plan.ParseAndExecute(ctx, string(buf[:n]))
		if err != nil {
			l.Errorln(err.Error())
		}

		// plan-tree
		// var sqls []SqlRouter
		txn.Participants = make([]string, len(sqls))
		txn.Results = make([]sql.Result, len(sqls))
		txn.Responses = make([]bool, len(sqls))
		for i, s := range sqls {
			m := c.NewQueryRequestMessage(s, txn)
			id := FlushMessage(c, m)
			if id == -1 {
				l.Error("can not send to ip:", s.Site_ip)
			} else if id == int(c.Id) {
				go LocalExecSql(i, txn, s.Sql, c)
			} else {
				c.DispatchMessages[id].PushBack(*m)
			}
			txn.Participants[i] = s.Site_ip
			txn.Results[i] = nil
			txn.Responses[i] = false
		}

		// wait
		// result = plan_tree_root->execute(c, )
		// ---- insert -> ip / sql
		// ---- select -> execute_tree
		for i := 0; i < len(sqls); i++ {
			l.Infoln("wait for response for subquery id(%d), expect response from %s", sqls[i].Sql, sqls[i].Site_ip)
			for !txn.Responses[i] {
				time.Sleep(time.Duration(1) * time.Nanosecond)
			}
		}

		// response
		response := "ok"
		conn.Write([]byte(response))
		delete(c.ActiveTransactions, txn.TxnId)
	}
}

func (c *Coordinator) wait_for_local_connection() {
	l := c.Context.Logger
	port := "10900"
	address := fmt.Sprintf("%s:%s", c.Context.DB_host, port)
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

// func (c *Coordinator) BroadcastToPeers(sql string, peers []string) {
// 	for _, p := range peers {
// 		for _, pp := range c.peers {
// 			if pp.ip == p {
// 				message := c.NewQueryRequestMessage(new plan.SqlRouter{

// 				})
// 			}
// 		}
// 	}
// }
