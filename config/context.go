package config

import (
	"database/sql"

	"github.com/sirupsen/logrus"
)

type Context struct {
	DB_type           string `default:"mysql"`
	DB_name           string `default:"ddb"`
	DB_host           string `default:"127.0.0.1"`
	DB_port           string `default:"3306"`
	ServerPort        string `default:"10800"`
	Release           bool   `default:"false"` // performance test(release)
	Debug             bool   `default:"false"` // correctness test(debug)
	Fragment_strategy string `default:"none"`
	Peer_file         string `default:"/home/bigdata/Course3-DDB/DDB_Project/config/cluster.ips"`
	Logger            *logrus.Logger
	DB                *sql.DB
}
