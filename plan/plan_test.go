package plan_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	cfg "project/config"
	core "project/core"
	mysql "project/mysql"
	plan "project/plan"
	utils "project/utils"
	logger "project/utils/log"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func TestParseDebug(t *testing.T) {
	// read partion meta info
	jsonFileDir := "/home/bigdata/Course3-DDB/DDB_Project/config/partition.json"
	jsonFile, err := os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")
	byteValue, _ := ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var partitions plan.Partitions
	json.Unmarshal([]byte(byteValue), &partitions)
	fmt.Println(partitions)
	////

	// read table meta info
	jsonFileDir = "/home/bigdata/Course3-DDB/DDB_Project/config/table_meta.json"
	jsonFile, err = os.Open(jsonFileDir)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened users.json")

	byteValue, _ = ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	var table_metas plan.TableMetas
	json.Unmarshal([]byte(byteValue), &table_metas)
	fmt.Println(table_metas)
	///////

	var g_ctx cfg.Context
	utils.ParseArgs(&g_ctx)
	// init log level, log file...
	logger.LoggerInit(&g_ctx)
	// a test connection to db engine
	mysql.SQLDriverInit(&g_ctx)
	// start coordinator <worker, socket_input, socket_dispatcher>
	c := core.NewCoordinator(&g_ctx)

	ctx := plan.Context{
		TablePartitions: partitions,
		TableMetas:      table_metas,
	}

	// parser and hanlder insert and select
	my_parser := parser.New()
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
		// `create partition on |PUBLISHER| [horizontal]
		// 	at (10.77.110.145, 10.77.110.146, 10.77.110.145, 10.77.110.146)
		// 	where {
		// 	 "PUBLISHER.1" : ID < 104000 and NATION = 'PRC';
		// 	 "PUBLISHER.2" : ID < 104000 and NATION = 'USA';
		// 	 "PUBLISHER.3" : ID >= 104000 and NATION = 'PRC';
		// 	 "PUBLISHER.4" : ID >= 104000 and NATION = 'USA'
		// 	};`,
		// `create partition on |CUSTOMER| [vertical]
		// 	at (10.77.110.145, 10.77.110.146)
		// 	where {
		// 	"CUSTOMER.1" : ID, NAME;
		// 	"CUSTOMER.2" : ID, rank
		// 	};`,
		// "select * from Customer;",
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
		// `select *
		// from Customer`,

		// 2
		// `select Publisher.name
		// from Publisher`,

		// 3
		// `select Book.title
		// from Book
		// where copies>5000`,
		// `select customer_id, quantity from Orders
		// where quantity < 8`,
		// `select Book.title,Book.copies,  Publisher.name,Publisher.nation
		// from Book,Publisher
		// where Book.publisher_id=Publisher.id
		// and Publisher.nation='USA'
		// and Book.copies > 1000`,
		// `select Customer.name,Orders.quantity
		// from Customer,Orders
		// where Customer.id=Orders.customer_id`, // wrong
		// `select Customer.name,Customer.rank, Orders.quantity
		// from Customer,Orders
		// where Customer.id=Orders.customer_id
		// and Customer.rank=1`, // wrong projection pushdown
		// `select Customer.name ,Orders.quantity, Book.title
		// from Customer,Orders,Book
		// where Customer.id=Orders.customer_id
		// and Book.id=Orders.book_id
		// and Customer.rank=1
		// and Book.copies>5000`, // wrong

		// ` select Customer.name, Book.title, Publisher.name, Orders.quantity
		// from Customer, Book, Publisher, Orders
		// where Customer.id=Orders.customer_id
		// and Book.id=Orders.book_id
		// and Book.publisher_id=Publisher.id
		// and Book.id>220000
		// and Publisher.nation='USA'
		// and Orders.quantity>1`, // wrong

		// 10
		`select Customer.name, Book.title,
		Publisher.name, Orders.quantity
		from Customer, Book, Publisher,
		Orders
		where
		Customer.id=Orders.customer_id
		and Book.id=Orders.book_id
		and Book.publisher_id=Publisher.id
		and Customer.id>308000
		and Book.copies>100
		and Orders.quantity>1
		and Publisher.nation='PRC'`,
	}

	for _, sql_str := range sql_strs {
		sql_str = strings.ToUpper(sql_str)
		sql_str = strings.Replace(sql_str, "\t", " ", -1)
		sql_str = strings.Replace(sql_str, "\n", " ", -1)

		fmt.Println(sql_str)
		if strings.Contains(sql_str, "PARTITION") {
			sql_str = strings.Replace(sql_str, " ", "", -1)
			_, err := plan.HandlePartitionSQL(sql_str)
			if err != nil {
				fmt.Println(err)
			}
		} else {
			stmt, err := my_parser.ParseOneStmt(sql_str, "", "")
			if err != nil {
				fmt.Println(err)
			}

			// Otherwise do something with stmt
			switch x := stmt.(type) {
			case *ast.SelectStmt:
				fmt.Println("Select")
				_, _ = plan.HandleSelect(ctx, x)
			case *ast.InsertStmt:
				fmt.Println("Insert") // same as delete
				plan.HandleInsert(ctx, x)
			case *ast.CreateTableStmt:
				fmt.Println("create table") // same as delete
				plan.HandleCreateTable(ctx, x)
			case *ast.DropTableStmt:
				plan.HandleDropTable(ctx, x)
			case *ast.DeleteStmt:
				fmt.Println("delete") // same as delete
				plan.HandleDelete(ctx, x)
			default:
				// createdb, dropdb, create table, drop table, all broadcast
				plan.BroadcastSQL(ctx, c, x)
			}
		}
	}
}

// func ParseAndExecute(c *core.Coordinator, sql_str string) (*plan.PlanTreeNode, []SqlRouter, error) {
// 	var p *plan.PlanTreeNode
// 	var ret []SqlRouter
// 	var err error

// 	my_parser := parser.New()

// 	sql_str = strings.ToUpper(sql_str)
// 	sql_str = strings.Replace(sql_str, "\t", "", -1)
// 	sql_str = strings.Replace(sql_str, "\n", "", -1)

// 	fmt.Println(sql_str)
// 	if strings.Contains(sql_str, "PARTITION") {
// 		sql_str = strings.Replace(sql_str, " ", "", -1)
// 		_, err := HandlePartitionSQL(sql_str)
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	} else {
// 		stmt, err := my_parser.ParseOneStmt(sql_str, "", "")
// 		if err != nil {
// 			fmt.Println(err)
// 		}

// 		// Otherwise do something with stmt
// 		switch x := stmt.(type) {
// 		case *ast.SelectStmt:
// 			fmt.Println("Select")
// 			p, err = HandleSelect(c, x)
// 		case *ast.InsertStmt:
// 			fmt.Println("Insert") // same as delete
// 			ret, err = HandleInsert(c, x)
// 		case *ast.CreateTableStmt:
// 			fmt.Println("create table") // same as delete
// 			ret, err = HandleCreateTable(c, x)
// 		case *ast.DropTableStmt:
// 			ret, err = HandleDropTable(c, x)
// 		case *ast.DeleteStmt:
// 			fmt.Println("delete") // same as delete
// 			ret, err = HandleDelete(c, x)
// 		default:
// 			// createdb, dropdb, create table, drop table, all broadcast
// 			ret, err = BroadcastSQL(c, x)
// 		}
// 	}
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	return p, ret, err

// }
