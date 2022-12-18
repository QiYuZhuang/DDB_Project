package plan_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	cfg "project/config"
	core "project/core"
	"project/meta"
	mysql "project/mysql"
	plan "project/plan"
	utils "project/utils"
	logger "project/utils/log"

	_ "github.com/pingcap/tidb/parser/test_driver"
)

// func TestParseDebug(t *testing.T) {
// 	// read partion meta info
// 	/*
// 		jsonFileDir := "/home/bigdata/Course3-DDB/DDB_Project/config/partition.json"
// 		jsonFile, err := os.Open(jsonFileDir)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		fmt.Println("Successfully Opened users.json")
// 		byteValue, _ := ioutil.ReadAll(jsonFile)
// 		jsonFile.Close()
// 		var partitions meta.Partitions
// 		json.Unmarshal([]byte(byteValue), &partitions)
// 		fmt.Println(partitions)
// 		////

// 		// read table meta info
// 		jsonFileDir = "/home/bigdata/Course3-DDB/DDB_Project/config/table_meta.json"
// 		jsonFile, err = os.Open(jsonFileDir)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 		fmt.Println("Successfully Opened users.json")

// 		byteValue, _ = ioutil.ReadAll(jsonFile)
// 		jsonFile.Close()
// 		var table_metas meta.TableMetas
// 		json.Unmarshal([]byte(byteValue), &table_metas)
// 		fmt.Println(table_metas)
// 	*/
// 	///////
// 	var table_metas meta.TableMetas
// 	var partitions meta.Partitions
// 	var g_ctx cfg.Context
// 	etcd.Connect_etcd() //连接etcd客户端
// 	utils.ParseArgs(&g_ctx)
// 	// init log level, log file...
// 	logger.LoggerInit(&g_ctx)
// 	// a test connection to db engine
// 	mysql.SQLDriverInit(&g_ctx)
// 	// start coordinator <worker, socket_input, socket_dispatcher>
// 	c := core.NewCoordinator(&g_ctx)

// 	ctx := meta.Context{
// 		TablePartitions: partitions,
// 		TableMetas:      table_metas,
// 		Peers:           c.Peers[:],
// 		IP:              c.Peers[c.Id].Ip,
// 		IsDebugLocal:    false,
// 	}
// 	return
// 	// parser and hanlder insert and select
// 	// sql_str := "insert into publisher values(200000, 'hello world');"

// 	// sql_str := "create table customer (ID int, NAME varchar(255), RANK_ int);"
// 	// sql_str := "drop table customer;"

// 	// stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.Id = b.Id", "", "")
// 	// stmt, _ := my_parser.ParseOneStmt("select test.a, test2.b from test, test2 where test.a >= 2 and test2.b < 30;", "", "")

// 	sql_strs := []string{
// 		//"create table publisher (ID int, NAME varchar(255), NATION varchar(255));",
// 		// "create table customer (ID int, NAME varchar(255), RANK_ int);",
// 		// "insert into publisher values(103999, 'zzq', 'PRC');",
// 		// "insert into publisher values(103999, 'zzq2', 'USA');",
// 		// "insert into publisher values(104000, 'aa1', 'PRC');",
// 		// "insert into publisher values(104000, 'dss2', 'USA');",
// 		// "insert into customer values(20000, 'hello world', 2);",
// 		// "insert into customer values(20000, 'hello world', 2);",
// 		// "drop table customer;",
// 		`create partition on |PUBLISHER| [horizontal]
// 			at (10.77.110.145, 10.77.110.146, 10.77.110.145, 10.77.110.146)
// 			where {
// 			 "PUBLISHER.1" : ID < 104000 and NATION = 'PRC';
// 			 "PUBLISHER.2" : ID < 104000 and NATION = 'USA';
// 			 "PUBLISHER.3" : ID >= 104000 and NATION = 'PRC';
// 			 "PUBLISHER.4" : ID >= 104000 and NATION = 'USA'
// 			};`,
// 		"create table publisher (ID int, NAME varchar(255), NATION varchar(255));",
// 		// `create partition on |CUSTOMER| [vertical]
// 		// 	at (10.77.110.145, 10.77.110.146)
// 		// 	where {
// 		// 	"CUSTOMER.1" : ID, NAME;
// 		// 	"CUSTOMER.2" : ID, rank_
// 		// 	};`,
// 		// "select * from Customer;",
// 		// "select Publisher.name from Publisher;",
// 		// `select Customer.name,Orders.quantity
// 		// from Customer,Orders
// 		// where Customer.id=Orders.customer_id`,
// 		// ` select Book.title,Book.copies,
// 		//   Publisher.name,Publisher.nation
// 		//   from Book,Publisher
// 		//   where Book.publisher_id=Publisher.id
// 		//   and Publisher.nation='USA'
// 		//   and Book.copies > 1000`,
// 		// `select Customer.name, Book.title, Publisher.name, Orders.quantity
// 		// from Customer, Book, Publisher, Orders
// 		// where
// 		// Customer.id=Orders.customer_id
// 		// and Book.id=Orders.book_id
// 		// and Book.publisher_id=Publisher.id
// 		// and Customer.id>308000
// 		// and Book.copies>100
// 		// and Orders.quantity>1
// 		// and Publisher.nation='PRC'
// 		// `,

// 		// 1
// 		//`select *
// 		//from Customer`,

// 		// 2
// 		// `select Publisher.name
// 		// from Publisher`,

// 		// 3
// 		// `select Book.title
// 		// from Book
// 		// where copies>5000`,
// 		// `select customer_id, quantity from Orders
// 		// where quantity < 8`,
// 		// `select Book.title,Book.copies,  Publisher.name,Publisher.nation
// 		// from Book,Publisher
// 		// where Book.publisher_id=Publisher.id
// 		// and Publisher.nation='USA'
// 		// and Book.copies > 1000`,
// 		// `select Customer.name,Orders.quantity
// 		// from Customer,Orders
// 		// where Customer.id=Orders.customer_id`, // wrong
// 		// `select Customer.name,Customer.rank_, Orders.quantity
// 		// from Customer,Orders
// 		// where Customer.id=Orders.customer_id
// 		// and Customer.rank_=1`, // wrong projection pushdown
// 		// `select Customer.name ,Orders.quantity, Book.title
// 		// from Customer,Orders,Book
// 		// where Customer.id=Orders.customer_id
// 		// and Book.id=Orders.book_id
// 		// and Customer.rank_=1
// 		// and Book.copies>5000`, // wrong

// 		// ` select Customer.name, Book.title, Publisher.name, Orders.quantity
// 		// from Customer, Book, Publisher, Orders
// 		// where Customer.id=Orders.customer_id
// 		// and Book.id=Orders.book_id
// 		// and Book.publisher_id=Publisher.id
// 		// and Book.id>220000
// 		// and Publisher.nation='USA'
// 		// and Orders.quantity>1`, // wrong

// 		// 10
// 		// `select Customer.name, Book.title,
// 		// Publisher.name, Orders.quantity
// 		// from Customer, Book, Publisher,
// 		// Orders
// 		// where
// 		// Customer.id=Orders.customer_id
// 		// and Book.id=Orders.book_id
// 		// and Book.publisher_id=Publisher.id
// 		// and Customer.id>308000
// 		// and Book.copies>100
// 		// and Orders.quantity>1
// 		// and Publisher.nation='PRC'`,
// 	}

// 	for _, sql_str := range sql_strs {
// 		err := etcd.RefreshContext(&ctx)
// 		if err != nil {
// 			fmt.Errorf(err.Error())
// 			break
// 		}
// 		select_plan, sql_routers, err := plan.ParseAndExecute(ctx, sql_str)
// 		if err != nil {
// 			fmt.Errorf(err.Error())
// 			break
// 		}

// 		if len(sql_routers) > 0 {
// 			fmt.Println(sql_routers)
// 		}

// 		if select_plan != nil {
// 			plan.PrintPlanTreePlot(select_plan)
// 		}
// 	}
// }

func TestParseDebugLocal(t *testing.T) {
	// read partion meta info
	jsonFileDir := "/home/zqs/DDB_Project/config/partition.json"
	jsonFile, err := os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")
	byteValue, _ := ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var partitions meta.Partitions
	json.Unmarshal([]byte(byteValue), &partitions)
	fmt.Println(partitions)
	////

	// read table meta info
	jsonFileDir = "/home/zqs/DDB_Project/config/table_meta.json"
	jsonFile, err = os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")

	byteValue, _ = ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var table_metas meta.TableMetas
	json.Unmarshal([]byte(byteValue), &table_metas)
	fmt.Println(table_metas)

	///////
	// var table_metas meta.TableMetas
	// var partitions meta.Partitions
	var g_ctx cfg.Context
	// etcd.Connect_etcd() //连接etcd客户端
	utils.ParseArgs(&g_ctx)
	// init log level, log file...
	logger.LoggerInit(&g_ctx)
	// a test connection to db engine
	mysql.SQLDriverInit(&g_ctx)
	// start coordinator <worker, socket_input, socket_dispatcher>
	c := core.NewCoordinator(&g_ctx)

	ctx := meta.Context{
		TablePartitions: partitions,
		TableMetas:      table_metas,
		Peers:           c.Peers[:],
		IP:              c.Peers[c.Id].Ip,
		Port:            c.Peers[c.Id].Port,
		IsDebugLocal:    true,
	}

	a, b, _ := plan.GetFilterCondition(ctx.TablePartitions.Partitions[1], "CUSTOMER_1")
	fmt.Println(a, b)

	// parser and hanlder insert and select
	// sql_str := "insert into publisher values(200000, 'hello world');"

	// sql_str := "create table customer (ID int, NAME varchar(255), RANK_ int);"
	// sql_str := "drop table customer;"

	// stmt, _ := my_parser.ParseOneStmt("select * from a, b where a.Id = b.Id", "", "")
	// stmt, _ := my_parser.ParseOneStmt("select test.a, test2.b from test, test2 where test.a >= 2 and test2.b < 30;", "", "")

	sql_strs := []string{
		// "create table publisher (ID int, NAME varchar(255), NATION varchar(255));",
		// "create table customer (ID int, NAME varchar(255), RANK_ int);",
		// "insert into publisher values(103999, 'zzq', 'PRC');",
		// "insert into publisher values(103999, 'zzq2', 'USA');",
		// "insert into publisher values(104000, 'aa1', 'PRC');",
		// "insert into publisher values(104000, 'dss2', 'USA');",
		// "insert into customer values(20000, 'hello world', 2);",
		// "insert into customer values(20000, 'hello world', 2);",
		// "drop table customer;",
		// `create partition on |PUBLISHER| [horizontal] \
		// 	at (10.77.110.145, 10.77.110.146, 10.77.110.145, 10.77.110.146) \
		// 	where { \
		// 	 "PUBLISHER_1" : ID < 104000 and NATION = 'PRC'; \
		// 	 "PUBLISHER_2" : ID < 104000 and NATION = 'USA'; \
		// 	 "PUBLISHER_3" : ID >= 104000 and NATION = 'PRC'; \
		// 	 "PUBLISHER_4" : ID >= 104000 and NATION = 'USA' \
		// 	};`,
		// "LOAD DATA LOCAL INFILE '/tmp/data/publisher.csv' INTO TABLE publisher FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (ID, NAME, NATION);",
		// "LOAD DATA LOCAL INFILE '/tmp/data/customer.csv' INTO TABLE customer FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n' (ID, NAME, _RANK);",
		// "create table publisher (ID int, NAME varchar(255), NATION varchar(255));",
		// `create partition on |CUSTOMER| [vertical]
		// 	at (10.77.110.145, 10.77.110.146)
		// 	where {
		// 	"CUSTOMER.1" : ID, NAME;
		// 	"CUSTOMER.2" : ID, rank_
		// 	};`,
		// "select * from Customer;",
		// "select * from Publisher;",
		// "select Publisher.name from Publisher;",
		// `select Customer.name,Orders.quantity
		// from Customer,Orders
		// where Customer.id=Orders.customer_id`,
		// ` select Book.title,Book.copies,
		//   Publisher.name,Publisher.nation
		//   from Book,Publisher
		//   where Book.publisher_id=Publisher.id
		//   and Publisher.nation='USA'
		//   and Book.copies > 1000`,
		// `select Customer.name, Book.title, Publisher.name, Orders.quantity
		// from Customer, Book, Publisher, Orders
		// where
		// Customer.id=Orders.customer_id
		// and Book.id=Orders.book_id
		// and Book.publisher_id=Publisher.id
		// and Customer.id>308000
		// and Book.copies>100
		// and Orders.quantity>1
		// and Publisher.nation='PRC'
		// `,

		// 1
		// `select * from Customer`,

		// 2
		// `select Publisher.name from Publisher`,

		// 3
		// `select Book.title from Book where copies>5000`,

		// 4
		// `select customer_id, quantity from Orders where quantity < 8`,

		// 5
		// `select Book.title,Book.copies,  Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies > 1000`,

		// 6
		`select Customer.name,Orders.quantity from Customer,Orders where Customer.id=Orders.customer_id`,

		// 7
		// `select Customer.name,Customer.rank_, Orders.quantity from Customer,Orders where Customer.id=Orders.customer_id and Customer.rank_=1`, // not best

		// 8
		// `select Customer.name ,Orders.quantity, Book.title from Customer,Orders,Book where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Customer.rank_=1 and Book.copies>5000`, // not best

		// 9
		// ` select Customer.name, Book.title, Publisher.name, Orders.quantity from Customer, Book, Publisher, Orders where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Book.publisher_id=Publisher.id and Book.id>220000 and Publisher.nation='USA' and Orders.quantity>1`, // not best

		// 10
		// `select Customer.name, Book.title,Publisher.name, Orders.quantity from Customer, Book, Publisher, Orders where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Book.publisher_id=Publisher.id and Customer.id>308000 and Book.copies>100 and Orders.quantity>1 and Publisher.nation='PRC';`,
	}

	for _, sql_str := range sql_strs {
		// err := etcd.RefreshContext(&ctx)
		if err != nil {
			fmt.Errorf(err.Error())
			break
		}
		_, select_plan, sql_routers, err := plan.ParseAndExecute(ctx, sql_str)
		if err != nil {
			fmt.Errorf(err.Error())
			break
		}

		if len(sql_routers) > 0 {
			fmt.Println(sql_routers)
		}

		if select_plan != nil {
			plan.PrintPlanTreePlot(select_plan)
		}
	}
}
