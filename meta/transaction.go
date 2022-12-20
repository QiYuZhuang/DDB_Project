package meta

import (
	"sync"
	"time"
)

type TransactionType uint32

const (
	NormalTransaction TransactionType = 0
	TPCCTransaction   TransactionType = 1
	YCSBTransaction   TransactionType = 2
	TransactionNum    TransactionType = 3
	// add more message type here
)

type Transaction struct {
	TxnId           uint64
	Type            TransactionType
	Sql             string
	SubQueryNumber  int
	Participants    []string
	TmpResultInFile []string // file path from remote
	EffectRows      []int
	Responses       []bool
	Error           error
	StartTimestamp  time.Time
	b_mutex         sync.Mutex
}

func (t *Transaction) Init(len int) {
	t.Participants = make([]string, len)
	t.TmpResultInFile = make([]string, len)
	t.Responses = make([]bool, len)
	// t.SubSqlIsRemote = make([]bool, len)
	t.EffectRows = make([]int, len)
}

func (t *Transaction) AssignSubQueryId() int {
	t.b_mutex.Lock()
	defer t.b_mutex.Unlock()

	t.SubQueryNumber++
	return t.SubQueryNumber
}

type StmtType int32

const (
	SelectStmtType StmtType = iota
	InsertStmtType
	CreateTableStmtType
	DropTableStmtType
	CreateDatabaseStmtType
	UseDatabaseStmtType
	DropDatabaseStmtType
	LoadDataStmtType
	SelectIntoFileStmtType
	StmtTypeNum
)
