package utils

import (
	"flag"
	cfg "project/config"
)

type argvs struct {
	db_type           string
	db_name           string
	db_host           string
	server_port       string
	release           bool
	debug             bool
	fragment_strategy string
	peer_file         string
}

func (args *argvs) ParseArgsInit() {
	flag.StringVar(&args.db_type, "engine", "mysql", "type of database: mysql/custome")
	flag.StringVar(&args.db_name, "dbname", "ddb", "name of database")
	flag.StringVar(&args.db_host, "host", "127.0.0.1", "machine's ip")
	flag.StringVar(&args.server_port, "port", "10800", "machine's port")
	flag.BoolVar(&args.debug, "debug", false, "debug version")
	flag.BoolVar(&args.release, "release", false, "release version")
	flag.StringVar(&args.fragment_strategy, "strategy", "", "fragment strategy: horizontal/vertical/hybrid")
	flag.StringVar(&args.peer_file, "pfile", "config/cluster.ips", "config file of cluster")
}

func (args *argvs) assign_to_context(ctx *cfg.Context) {
	if args.db_type != "" {
		ctx.DB_type = args.db_type
	}
	if args.db_name != "" {
		ctx.DB_name = args.db_name
	}
	if args.db_host != "" {
		ctx.DB_host = args.db_host
	}
	if args.server_port != "" {
		ctx.ServerPort = args.server_port
	}
	if args.debug {
		ctx.Debug = true
		ctx.Release = false
	}
	if args.release {
		ctx.Release = true
		ctx.Debug = false
	}
	if args.fragment_strategy != "" {
		ctx.Fragment_strategy = args.fragment_strategy
	}
	if args.peer_file != "" {
		ctx.Peer_file = args.peer_file
	}
	// add here
}

func ParseArgs(ctx *cfg.Context) {
	args := &argvs{}
	args.ParseArgsInit()
	flag.Parse()

	if args.debug {
		flag.PrintDefaults()
	}

	args.assign_to_context(ctx)
}
