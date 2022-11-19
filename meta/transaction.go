package meta

import "database/sql"

type TransactionType uint32

const (
	NormalTransaction TransactionType = 0
	TPCCTransaction   TransactionType = 1
	YCSBTransaction   TransactionType = 2
	TransactionNum    TransactionType = 3
	// add more message type here
)

type Transaction struct {
	TxnId        uint64
	Type         TransactionType
	Sql          string
	Participants []string
	Results      []sql.Result
	Rows         []*sql.Rows
	Responses    []bool
}
