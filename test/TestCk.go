package main

import (
	. "dashboard/config"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"os"
)

var (
	dir1,_ = os.Getwd()
	conf = flag.String("conf", fmt.Sprintf("%s/dev.json",dir1), "config path")
)

func connCk()(*sql.DB, error){
	Init(*conf)

	connConf := Configuration.Clickhouse
	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf.Host, connConf.Port, connConf.User, connConf.Pass)

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return conn,nil

}

func execOne()error{
	conn, err := connCk()
	if err!= nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec(`
		select 1+1 as a
	`)
	if err!= nil {
		return err
	}
	return nil
}

func execMany()error{
	conn, err := connCk()
	if err!= nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec(`
		set max_bytes_before_external_group_by=21474836480;
		set max_memory_usage=21474836480;
	`)
	if err!= nil {
		return err
	}
	return nil
}

func main(){
	err := execOne()
	if err != nil {
		panic(err)
	}
}
