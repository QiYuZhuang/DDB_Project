package core

import (
	"time"

	cfg "project/config"
	p "project/plan"
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
	Type    MessageType   `json:"type"`
	Length  uint32        `json:"length"`
	Src     string        `json:"src_machine_id"`
	Dst     string        `json:"dest_machine_id"`
	TxnId   uint32        `json:"txn_id"`
	QueryId uint16        `json:"query_id"`
	Query   string        `json:"query"`
	Result  interface{}   `json:"result"`
	Time    time.Duration `json:"time"`
}

func NewMessage(t MessageType, src string, dst string, txn_id uint32) *Message {
	return &Message{
		Type:  t,
		Src:   src,
		Dst:   dst,
		TxnId: txn_id,
	}
}

func (m Message) MessageHandler(ctx *cfg.Context) {
	l := ctx.Logger

	switch m.Type {
	case QueryRequest:
		m.QueryRequestHandler(ctx)
	case QueryResponse:
		m.QueryResponseHandler(ctx)
	case HeartBeat:
		m.HeartBeatHandler(ctx)
	case HeartBeatAck:
		m.HeartBeatAckHandler(ctx)
	default:
		l.Errorln("Unsupport message type, message", m)
	}
}

func (m Message) QueryRequestHandler(ctx *cfg.Context) {
	// sql := m.Query
	// run (sql, src, txn_id)
}

func (m Message) QueryResponseHandler(ctx *cfg.Context) {
	// move result to specific variable, according to txn_id and query_id
	// trigger a check (if remote queries in the executor are already, join the results and go to next)
}

func (m Message) HeartBeatHandler(ctx *cfg.Context) {
	// no-use
}

func (m Message) HeartBeatAckHandler(ctx *cfg.Context) {
	// no-use
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
func (m *Message) SetResult() {

}

func (c *Coordinator) NewQueryRequestMessage(router p.SqlRouter) *Message {
	// router
	message := NewMessage(QueryRequest, c.context.DB_host, router.Site_ip, 0)
	message.SetQuery(router.Sql)
	message.SetMessageLength(0)
	return message
}
