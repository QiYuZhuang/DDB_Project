package meta

import "time"

type QueryType int

const (
	SelectStmt QueryType = 1
	CreateStmt QueryType = 2
	InsertStmt QueryType = 3
)

type BackToClient struct {
	Error    error         `json:"error"`
	Filepath string        `json:"filepath"`
	Filename string        `json:"filename"`
	ExecTime time.Duration `json:"exec_time"`
}
