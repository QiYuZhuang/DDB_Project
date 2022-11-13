package core

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
	Responses    []bool
}

func NewTransaction(sql string, c *Coordinator) *Transaction {
	c.t_mutex.Lock()
	defer c.t_mutex.Unlock()
	c.GlobalTransactionId++
	txn := &Transaction{
		TxnId: c.GlobalTransactionId,
		Type:  NormalTransaction,
		Sql:   string(sql),
	}
	c.ActiveTransactions[txn.TxnId] = txn
	return txn
}
