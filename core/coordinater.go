package core

import (
	"bufio"
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	cfg "project/config"
	"project/meta"
	"project/mysql"
	"project/plan"
	"project/utils"
	"strings"
	"sync"
	"time"
)

const MaxPeerNum_ = 10

type Coordinator struct {
	Id                  int16
	Peer_num            int16
	Peers               [MaxPeerNum_]meta.Peer
	InputSockets        [MaxPeerNum_]net.Conn
	DispatcherSockets   [MaxPeerNum_]net.Conn
	InputMessages       [MaxPeerNum_]list.List
	DispatchMessages    [MaxPeerNum_]list.List
	Context             *cfg.Context
	TableMetas          meta.TableMetas
	Partitions          meta.Partitions
	GlobalTransactionId uint64
	ActiveTransactions  map[uint64]*meta.Transaction
	Messages            chan meta.Message

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

	c.Messages = make(chan meta.Message, 100)
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
		sql := string(buf[:n])
		resp := c.process(sql)
		// 返回一个结构体，里面包含了最终的临时文件，以及执行状态
		// client 去处理结构体里的信息
		if data, err := json.Marshal(resp); err == nil {
			conn.Write(data)
		}
	}
}

func (c *Coordinator) process(sql string) meta.BackToClient {
	var resp meta.BackToClient
	l := c.Context.Logger
	// 创建新事务
	txn := NewTransaction(sql, c)
	ctx := meta.Context{
		Messages:        &c.Messages,
		TableMetas:      c.TableMetas,
		TablePartitions: c.Partitions,
		Peers:           c.Peers[:],
		IP:              c.Context.DB_host,
		Port:            c.Context.DB_port,
		DB:              c.Context.DB,
		Logger:          c.Context.Logger,
		IsDebugLocal:    true,
	}
	sql_type, plan_tree, sqls, err := plan.ParseAndExecute(ctx, sql)
	if err != nil {
		l.Errorln(err.Error())
		resp.Error = err.Error()
		return resp
	}

	// var eachNodeColNames [][]string
	var filename string

	// plan-tree
	if sql_type == meta.SelectStmtType && plan_tree != nil {
		fmt.Println(plan_tree)
		txn.Init(100)
		filename, err = mysql.Exec(&ctx, txn, plan_tree)
		if err != nil {
			resp.Error = err.Error()
		}
	} else {
		txn.Init(len(sqls))

		for i, s := range sqls {
			var m *meta.Message
			if len(s.File_path) != 0 {
				m = meta.NewDataLoadRequestMessage(ctx.IP, s.Site_ip, txn.TxnId)
				m.SetFilepath(s.File_path)
			} else {
				m = meta.NewQueryRequestMessage(ctx.IP, s.Site_ip, txn.TxnId)
			}
			m.SetQueryId(i)
			m.SetQuery(s.Sql)
			id := FindDestMachineId(c.Peers[:], *m)
			if id == -1 {
				l.Error("can not send to ip:", s.Site_ip)
				resp.Error = "invaild remote ip"
				break
			} else if id == int(c.Id) {
				exec_ctx := mysql.CreateExecuteContext(i, s.Sql, s.File_path, c.Context.DB, c.Context.Logger, txn)
				go mysql.LocalExecSql(exec_ctx, sql_type)
			} else {
				l.Infoln(m.Query, m.Src, m.Dst)
				c.DispatchMessages[id].PushBack(*m)
			}
			txn.Participants[i] = s.Site_ip
			// txn.Results[i] = nil
			// txn.Rows[i] = nil
			txn.Responses[i] = false
		}

		// wait
		for i := 0; i < len(sqls); i++ {
			l.Infoln("wait for response for subquery id(", sqls[i].Sql, "), expect response from", sqls[i].Site_ip)
			for !txn.Responses[i] {
				time.Sleep(time.Duration(10) * time.Nanosecond)
			}
		}

		for i := 0; i < len(sqls); i++ {
			if txn.Error != nil {
				resp.Error = txn.Error.Error()
				// assign resp.ExecTime
			}
		}
	}

	if sql_type == meta.SelectStmtType && plan_tree != nil {
		// select: set file path
		resp.Filename = filename
	}

	delete(c.ActiveTransactions, txn.TxnId)

	return resp
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

func (c *Coordinator) RemoveUselessFiles() {
	filepath1 := "/tmp/data/"
	utils.RmFile(filepath1+"*TMP_*", false)

	u, _ := user.Current()
	filepath2 := "/home/" + u.Username + "/"
	utils.RmFile(filepath2+"*TMP_*", false)
}

func (c *Coordinator) Start() {
	c.RemoveUselessFiles()

	c.connect_to_peers()
	// create a goroutine, which listen to local connection
	c.wait_for_local_connection()
}
