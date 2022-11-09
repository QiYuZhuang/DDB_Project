package core

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

func ConnectionHandler(c *Coordinator, conn net.Conn) {
	l := c.context.Logger
	buf := make([]byte, 4096)
	defer conn.Close()
	for {
		l.Debugln("wait for data")
		n, err := conn.Read(buf)
		if err != nil {
			l.Debugln("when read conn, conn closed", err.Error())
			break
		}
		l.Debugln("recv data:", string(buf))

		// buf possibly json format
		// transform to struct Message and call MessageHandler
		var ki Message
		json.Unmarshal(buf[:n], &ki)
		ki.MessageHandler(c.context)
		// c.InputMessages.PushBack(ki)
	}
}

func ClientConnectionHandler(c *Coordinator, peer_idx int) {
	l := c.context.Logger
	port := "10800"
	address := fmt.Sprintf("%s:%s", c.peers[peer_idx].ip, port)
	maxRetry := 25
	cntRetry := 1
	var conn net.Conn
	var err error
	// retry
	for {
		conn, err = net.Dial("tcp", address)
		if err == nil {
			break
		} else {
			cntRetry++
			l.Warn(address, ", client dial failed. Retry times ", cntRetry, err)
		}
		if cntRetry > maxRetry && err != nil {
			l.Error(address, ", client dial failed.", err)
			return
		}
		time.Sleep(1 * time.Second)
	}
	defer conn.Close()
	l.Debug(address, ", client connect")

	for {
		if c.DispatchMessages.Len() != 0 {
			m := c.DispatchMessages.Front()
			if data, err := json.Marshal(m); err == nil {
				_, err = conn.Write(data)
				if err != nil {
					l.Errorln("when write conn, conn closed.", err.Error())
				}
			} else {
				l.Errorln("json marshal failed, ", err.Error())
			}

			c.DispatchMessages.Remove(m)
		}
	}
}

func CreateInputSockets(c *Coordinator) {
	for i := 0; i < len(c.peers); i++ {
		if c.peers[i].id != c.id {
			go ClientConnectionHandler(c, i)
		}
	}
}

func CreateDispatcherSockets(c *Coordinator) {
	l := c.context.Logger
	port := "10800"
	address := fmt.Sprintf("%s:%s", c.context.DB_host, port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		l.Error("socket listen failed", err)
	}
	go func(listen *net.Listener) {
		defer listener.Close()
		for {
			new_conn, err := listener.Accept()
			if err != nil {
				l.Error("socket accept failed.", err.Error())
			}
			c.DispatcherSockets = append(c.DispatcherSockets, new_conn)
			go ConnectionHandler(c, new_conn)
		}
	}(&listener)
}
