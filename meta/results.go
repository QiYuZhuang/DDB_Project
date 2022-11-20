package meta

type QueryType int

const (
	SelectStmt QueryType = 1
	CreateStmt QueryType = 2
	InsertStmt QueryType = 3
)

type QueryResults struct {
	Type      QueryType
	Error     error
	TableName string
	Results   []Publish
}
