package core

import (
	"project/meta"
	"time"
)

func NewTransaction(sql string, c *Coordinator) *meta.Transaction {
	c.t_mutex.Lock()
	defer c.t_mutex.Unlock()
	c.GlobalTransactionId++
	txn := &meta.Transaction{
		TxnId:          c.GlobalTransactionId,
		Type:           meta.NormalTransaction,
		Sql:            string(sql),
		StartTimestamp: time.Now(),
	}
	c.ActiveTransactions[txn.TxnId] = txn
	return txn
}
