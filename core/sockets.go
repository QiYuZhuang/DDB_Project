package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

func ConnectionHandler(c *Coordinator, conn net.Conn) {
	l := c.Context.Logger
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
		l.Infoln("var ki Message sql: ", n, buf, string(buf))

		cur_pos := 0
		for cur_pos < n {
			var ki Message

			data_len := binary.BigEndian.Uint32(buf[cur_pos : cur_pos+4])
			cur_pos += 4

			json.Unmarshal(buf[cur_pos:cur_pos+int(data_len)], &ki)
			l.Infoln("var ki Message sql: ", ki.Query, n, buf, string(buf))
			ki.MessageHandler(c)

			cur_pos += int(data_len)
		}

		// c.InputMessages.PushBack(ki)
	}
}

func ClientConnectionHandler(c *Coordinator, peer_idx int) {
	l := c.Context.Logger
	port := "10800"
	address := fmt.Sprintf("%s:%s", c.Peers[peer_idx].Ip, port)
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
	c.DispatcherSockets[peer_idx] = conn
	l.Debug(address, ", client connect")

	for {
		c.d_mutex.Lock()
		// for i := 0; i < int(c.Peer_num); i++ {
		// p := c.Peers[peer_idx]
		for c.DispatchMessages[peer_idx].Len() != 0 {
			i := c.DispatchMessages[peer_idx].Front()
			// if reflect.TypeOf(m).Name() != "Message" {
			// 	l.Errorln("when dispatch, message type error", reflect.TypeOf(m).Name())
			// }
			m, ok := (i.Value).(Message)
			if !ok {
				l.Errorln("element to message failed")
			}
			l.Infoln("src: " + m.Src + " dst: " + m.Dst)

			if data, err := json.Marshal(m); err == nil {
				data_len := make([]byte, 4)
				binary.BigEndian.PutUint32(data_len, uint32(len(data)))
				data = append(data_len, data...)
				_, err = conn.Write(data)
				if err != nil {
					l.Errorln("when write conn, conn closed.", err.Error(), data, string(data))
				}
			} else {
				l.Errorln("json marshal failed, ", err.Error())
			}

			c.DispatchMessages[peer_idx].Remove(i)
		}
		// }
		c.d_mutex.Unlock()
	}
}

func CreateDispatcherSockets(c *Coordinator) {
	for i := 0; i < int(c.Peer_num); i++ {
		if c.Peers[i].Id != c.Id {
			go ClientConnectionHandler(c, i)
		}
	}
}

func CreateInputSockets(c *Coordinator) {
	l := c.Context.Logger
	port := "10800"
	address := fmt.Sprintf("%s:%s", c.Context.DB_host, port)
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
			var idx int
			for i := 0; i < int(c.Peer_num); i++ {
				p := c.Peers[i]
				if p.Ip == new_conn.RemoteAddr().String() {
					idx = i
					break
				}
			}
			c.DispatcherSockets[idx] = new_conn
			go ConnectionHandler(c, new_conn)
		}
	}(&listener)
}

func FlushMessage(c *Coordinator, m *Message) int {
	c.d_mutex.Lock()
	defer c.d_mutex.Unlock()

	var idx int

	for i := 0; i < int(c.Peer_num); i++ {
		p := c.Peers[i]
		if p.Ip == m.Dst {
			idx = int(p.Id)
			break
		}
		if i == int(c.Peer_num)-1 {
			return -1
		}
	}

	return idx
}
