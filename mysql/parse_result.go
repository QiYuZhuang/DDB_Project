package mysql

import (
	"database/sql"
	"project/meta"

	"github.com/pingcap/log"
)

func ParseRows(rows *sql.Rows) int {

	row_cnt := 0
	for rows.Next() {
		var data meta.Publish
		if err := rows.Scan(&data.Id, &data.Name, &data.Nation); err != nil {
			// l.Error(err)
			log.Error(err.Error())
			break
		}

		row_cnt++
	}
	return row_cnt
}
