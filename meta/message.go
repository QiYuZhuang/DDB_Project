package meta

import (
	"database/sql"
	"time"
)

type MessageType uint32

const (
	QueryRequest  MessageType = 0
	QueryResponse MessageType = 1
	HeartBeat     MessageType = 2
	HeartBeatAck  MessageType = 3
	// add more message type here
	DataLoadRequest  MessageType = 4
	DataLoadResponse MessageType = 5
)

/*
 * QueryId is the unique identify of a subquery
 * TxnId is the identify of the query(out of order)
 */
type Message struct {
	Type      MessageType `json:"type"`
	Length    uint32      `json:"length"`
	Src       string      `json:"src_machine_id"`
	Dst       string      `json:"dest_machine_id"`
	TxnId     uint64      `json:"txn_id"`
	QueryId   int         `json:"query_id"`
	Query     string      `json:"query"`
	TableName string      `json:"table_name"`
	Filepath  string      `json:"data_file_path"`
	Filename  string      `json:"data_file_name"`
	Result    sql.Result  `json:"result"`
	RowCnt    int         `json:"row_cnt"`
	Time      time.Time   `json:"time"`
	Error     bool        `json:"error"`
}

func NewMessage(t MessageType, src string, dst string, txn_id uint64) *Message {
	return &Message{
		Type:  t,
		Src:   src,
		Dst:   dst,
		TxnId: txn_id,
		Time:  time.Now(),
	}
}

func (m *Message) SetMessageLength(size uint32) {
	m.Length = size
}

func (m *Message) SetQuery(sql string) {
	m.Query = sql
}

func (m *Message) SetQueryId(query_id int) {
	m.QueryId = query_id
}

// TODO: have no idea now
func (m *Message) SetResult(res sql.Result) {
	m.Result = res
	// m.Rows = nil
}

func (m *Message) SetError(is_error bool) {
	m.Error = is_error
}

func (m *Message) SetRowCnt(row_cnt int) {
	m.RowCnt = row_cnt
}

func (m *Message) SetFilepath(filepath string) {
	m.Filepath = filepath
}

func (m *Message) SetFilename(filename string) {
	m.Filename = filename
}

func (m *Message) SetTableName(table_name string) {
	m.TableName = table_name
}

func NewQueryRequestMessage(local_ip string, remote_ip string, txnId uint64) *Message {
	// router
	message := NewMessage(QueryRequest, local_ip, remote_ip, txnId)
	message.SetMessageLength(0)
	return message
}

func NewDataLoadRequestMessage(local_ip string, remote_ip string, txnId uint64) *Message {
	// router
	message := NewMessage(DataLoadRequest, local_ip, remote_ip, txnId)
	message.SetMessageLength(0)
	return message
}
