package meta

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
	Participants    []string
	TmpResultInFile []string // file path from remote
	EffectRows      []int
	QueryResult     []QueryResults
	SubSqlIsRemote  []bool
	Responses       []bool
	Error           error
}

func (t *Transaction) Init(len int) {
	t.Participants = make([]string, len)
	t.TmpResultInFile = make([]string, len)
	t.QueryResult = make([]QueryResults, len)
	t.Responses = make([]bool, len)
	t.SubSqlIsRemote = make([]bool, len)
	t.EffectRows = make([]int, len)
}
