package mysql

import (
	"database/sql"
	"project/meta"

	"github.com/pingcap/log"
)

func ParseRows(rows *sql.Rows) (meta.QueryResults, int) {
	res := &meta.QueryResults{
		Type:  meta.SelectStmt,
		Error: nil,
	}

	row_cnt := 0
	for rows.Next() {
		var data meta.Publish
		err := rows.Scan(&data.Id, &data.Name, &data.Nation)
		if err != nil {
			// l.Error(err)
			log.Error(err.Error())
			break
		} else {
			// Output = append(Output, data)
			res.Results = append(res.Results, data)
		}
		row_cnt++
	}
	return *res, row_cnt
}
