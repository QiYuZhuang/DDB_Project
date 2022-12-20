package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"project/meta"
	"time"
)

func ConnectionHandler(c *Coordinator, conn net.Conn) {
	l := c.Context.Logger
	max_buf_size := 20 * (1 << 20)
	buf := make([]byte, max_buf_size)
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
		// l.Infoln("var ki Message sql: ", n, buf, string(buf))

		cur_pos := 0
		for cur_pos < n {
			var ki meta.Message

			data_len := binary.BigEndian.Uint32(buf[cur_pos : cur_pos+4])
			cur_pos += 4

			json.Unmarshal(buf[cur_pos:cur_pos+int(data_len)], &ki)
			// l.Infoln("var ki Message sql: ", ki.Query, n, buf, string(buf))
			MessageHandler(ki, c)

			cur_pos += int(data_len)
		}

		// c.InputMessages.PushBack(ki)
	}
}

func ClientConnectionHandler(c *Coordinator, peer_idx int) {
	l := c.Context.Logger
	// port := "1080" + strconv.Itoa(peer_idx)
	address := fmt.Sprintf("%s:%s", c.Peers[peer_idx].Ip, c.Peers[peer_idx].Port)
	maxRetry := 120
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
	l.Infoln("Connection established: ", address, ", client connect")

	for {
		c.d_mutex.Lock()
		// for i := 0; i < int(c.Peer_num); i++ {
		// p := c.Peers[peer_idx]
		for c.DispatchMessages[peer_idx].Len() != 0 {
			i := c.DispatchMessages[peer_idx].Front()
			// if reflect.TypeOf(m).Name() != "Message" {
			// 	l.Errorln("when dispatch, message type error", reflect.TypeOf(m).Name())
			// }
			m, ok := (i.Value).(meta.Message)
			if !ok {
				l.Errorln("element to message failed")
			}
			l.Infoln("src: [ip: " + m.Src + " port: " + m.SrcPort + "] dst: [ip: " + m.Dst + " port: " + m.DstPort + "]")

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

	go AsyncFlushMessage(c)
}

func CreateInputSockets(c *Coordinator) {
	l := c.Context.Logger
	// port := "1080" + strconv.Itoa(int(c.Id))
	address := fmt.Sprintf("%s:%s", c.Context.DB_host, c.Context.ServerPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		l.Error("socket listen failed", err)
	}
	go func(*net.Listener) {
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

func FindDestMachineId(peers []meta.Peer, m meta.Message) int {
	var idx int
	idx = -1

	for i := 0; i < len(peers); i++ {
		p := peers[i]
		if p.Ip == m.Dst && p.Port == m.DstPort {
			idx = int(p.Id)
			break
		}
	}

	return idx
}

func AsyncFlushMessage(c *Coordinator) {
	for m := range c.Messages {
		idx := FindDestMachineId(c.Peers[:], m)
		c.DispatchMessages[idx].PushBack(m)
	}
}
