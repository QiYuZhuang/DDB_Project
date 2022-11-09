package core

import (
	"time"

	cfg "project/config"
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
	Src     int16         `json:"src_machine_id"`
	Dst     int16         `json:"dest_machine_id"`
	TxnId   uint32        `json:"txh_id"`
	QueryId uint16        `json:"query_id"`
	Query   string        `json:"query"`
	Result  interface{}   `json:"result"`
	Time    time.Duration `json:"time"`
}

func NewMessage(t MessageType, src int16, dst int16, txn_id uint32) Message {
	return Message{
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

/*	struct Message
	| data | -> | message_length | message_type | src_thread_id | dest_thread_id |
*/
// func (m *Message) ResizeData(size int32) {
// 	m.data = make([]byte, size)
// }

// func (m *Message) GetMessageLength() int32 {
// 	return int32(binary.LittleEndian.Uint32(m.data[0:4]))
// }

// func (m *Message) SetMessageLength(length int32) {
// 	m.data[0] = uint8(length)
// 	m.data[1] = uint8(length >> 8)
// 	m.data[2] = uint8(length >> 16)
// 	m.data[3] = uint8(length >> 24)
// }
