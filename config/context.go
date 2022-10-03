package config

import (
	"database/sql"

	log "github.com/sirupsen/logrus"
)

type Context struct {
	DB_type           string `default:"mysql"`
	DB_name           string `default:"ddb"`
	DB_host           string `default:"127.0.0.1"`
	DB_port           string `default:"3306"`
	Release           bool   `default:"false"` // performance test(release)
	Debug             bool   `default:"false"` // correctness test(debug)
	Fragment_strategy string `default:"none"`
	Peer_file         string `default:"/home/bigdata/Course3-DDB/DDB_Project/config/cluster.ips"`
	Logger            *log.Logger
	DB                *sql.DB
}
