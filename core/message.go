package core

import (
	"database/sql"
	"errors"
	"project/meta"
	"project/mysql"
	"strings"
	"time"
)

type MessageType uint32

const (
	QueryRequest  MessageType = 0
	QueryResponse MessageType = 1
	HeartBeat     MessageType = 2
	HeartBeatAck  MessageType = 3
	// add more message type here
)

/*
 * QueryId is the unique identify of a subquery
 * TxnId is the identify of the query(out of order)
 */
type Message struct {
	Type    MessageType `json:"type"`
	Length  uint32      `json:"length"`
	Src     string      `json:"src_machine_id"`
	Dst     string      `json:"dest_machine_id"`
	TxnId   uint64      `json:"txn_id"`
	QueryId uint16      `json:"query_id"`
	Query   string      `json:"query"`
	Result  sql.Result  `json:"result"`
	// Rows    sql.Rows      `json:"rows"`
	QueryResult meta.QueryResults `json: query_result`
	Time        time.Duration     `json:"time"`
	Error       bool              `json:"error"`
}

func NewMessage(t MessageType, src string, dst string, txn_id uint64) *Message {
	return &Message{
		Type:  t,
		Src:   src,
		Dst:   dst,
		TxnId: txn_id,
	}
}

func (m Message) MessageHandler(c *Coordinator) error {
	l := c.Context.Logger
	l.Infoln("src: " + m.Src + " dst: " + m.Dst)

	var err error
	err = nil
	switch m.Type {
	case QueryRequest:
		err = m.QueryRequestHandler(c)
	case QueryResponse:
		err = m.QueryResponseHandler(c)
	case HeartBeat:
		err = m.HeartBeatHandler(c)
	case HeartBeatAck:
		err = m.HeartBeatAckHandler(c)
	default:
		l.Errorln("Unsupport message type, message", m)
	}
	return err
}

func FlushAndPush(msg *Message, c *Coordinator) error {
	idx := FlushMessage(c, msg)
	if idx == -1 {
		return errors.New("can not find ip" + msg.Dst)
	}
	c.DispatchMessages[idx].PushBack(*msg)
	return nil
}

func (m Message) QueryRequestHandler(c *Coordinator) error {
	// sql := m.Query
	// run (sql, src, txn_id)
	l := c.Context.Logger
	l.Infoln("query is ", m.Query)
	err := c.Context.DB.Ping()
	if err != nil {
		l.Errorln("database ping failed")
	}
	resp := NewMessage(QueryResponse, m.Dst, m.Src, m.TxnId)
	resp.SetQueryId(m.QueryId)
	if strings.Contains(m.Query, "SELECT") {
		rows, err := c.Context.DB.Query(m.Query)
		if err != nil {
			l.Errorln("exec failed", err)
		}

		result := mysql.ParseRows(rows)
		resp.SetQueryResult(result)
	} else {
		res, err := c.Context.DB.Exec(m.Query)
		if err != nil {
			l.Errorln("exec failed", err)
		}
		resp.SetResult(res)
	}

	if err != nil {
		// fmt.Println("exec failed, ", err)
		resp.SetError(true)
		FlushAndPush(resp, c)
		return errors.New("query request handler failed")
	}
	// resp.SetResult(res)
	resp.SetError(false)
	FlushAndPush(resp, c)
	return nil
}

func (m Message) QueryResponseHandler(c *Coordinator) error {
	// move result to specific variable, according to txn_id and query_id
	// trigger a check (if remote queries in the executor are already, join the results and go to next)
	l := c.Context.Logger
	txn, ok := c.ActiveTransactions[uint64(m.TxnId)]
	if ok {
		// for id, part := range txn.Participants {
		// 	if part == m.Src {
		// 		txn.Results[id] = m.Result
		// 		txn.Responses[id] = true
		// 		return nil
		// 	}
		// }
		query_id := m.QueryId
		if int(query_id) >= len(txn.Participants) {
			l.Errorln("not a vaild sub query result")
			return errors.New("invaild arguments")
		}

		// txn.Results[query_id] = m.Result
		txn.QueryResult[query_id] = m.QueryResult
		txn.Responses[query_id] = true
		// l.Errorln("not a vaild sub query result")
		return nil
	} else {
		l.Errorln("can not find active transcation, id is ", txn.TxnId)
	}
	return errors.New("query response handler failed")
}

func (m Message) HeartBeatHandler(c *Coordinator) error {
	// no-use
	return nil
}

func (m Message) HeartBeatAckHandler(c *Coordinator) error {
	// no-use
	return nil
}

func (m *Message) SetMessageLength(size uint32) {
	m.Length = size
}

func (m *Message) SetQuery(sql string) {
	m.Query = sql
}

func (m *Message) SetQueryId(query_id uint16) {
	m.QueryId = query_id
}

// TODO: have no idea now
func (m *Message) SetResult(res sql.Result) {
	m.Result = res
	// m.Rows = nil
}

func (m *Message) SetQueryResult(rows meta.QueryResults) {
	m.QueryResult = rows
	m.Result = nil
}

func (m *Message) SetError(is_error bool) {
	m.Error = is_error
}

func (c *Coordinator) NewQueryRequestMessage(query_id uint16, router meta.SqlRouter, txn *meta.Transaction) *Message {
	// router
	message := NewMessage(QueryRequest, c.Context.DB_host, router.Site_ip, txn.TxnId)
	message.SetQuery(router.Sql)
	message.SetQueryId(query_id)
	message.SetMessageLength(0)
	return message
}
