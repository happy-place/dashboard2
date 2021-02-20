package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"reflect"
	"strings"
)

var (
	result = make([][]string,0)
)

func getInt64(value driver.Value)(int64){
	rValue := reflect.ValueOf(value)
	return rValue.Int()
}

func getString(value driver.Value) string{
	rValue := reflect.ValueOf(value)
	inter := rValue.Interface()
	arr := inter.([]uint8)
	return string(arr)
}

func getString2(value interface{}) string{
	rValue := reflect.ValueOf(value)
	inter := rValue.Interface()
	arr := inter.([]uint8)
	return string(arr)
}

func checkErr1(err error){
	if err!= nil{
		panic(err)
	}
}

func connSqlite()(*sql.DB,error){
	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				getDeps := func(node_id,node_type int64) (string, error) {
					isCompleted := false
					temp := make(map[string]string)
					cond := fmt.Sprintf(`%d-%d`,node_id,node_type)
					for ;!isCompleted; {
						sql := fmt.Sprintf(`select group_concat(parent_id || '-' || parent_type) as cond,group_concat(parent_id) as ids from edge 
								where (node_id || '-' || node_type) like '%s'`,cond)
						query, err := conn.Query(sql, nil)
						if err != nil {
							return "", err
						}
						cols := query.Columns()
						vals := make([]driver.Value, len(cols))
						query.Next(vals)

						if vals[0] == nil {
							isCompleted = true
						}else{
							cond = getString(vals[0])
							ids := getString(vals[1])
							if strings.Index(ids,",")==-1{
								temp[ids] = ids
							}else{
								for _,department_id := range strings.Split(ids,","){
									temp[department_id] = department_id
								}
							}
						}
					}

					ids := make([]string,0)
					for k,_ := range temp{
						ids = append(ids,fmt.Sprintf("%s",k))
					}

					// 写出
					values := make([]string,0)
					for department_id,_ := range temp {
						values = append(values,fmt.Sprintf(`(%d,%s)`,node_id,department_id))
					}
					sql := fmt.Sprintf(`insert into user_dep(user_id,department_id) values %s`,strings.Join(values,","))
					fmt.Println(sql)
					_, err := conn.Exec(sql, nil)
					checkErr1(err)
					return strings.Join(ids,","),nil
				}
				return conn.RegisterFunc("getDeps", getDeps, true)
			},
		})
	//conn, err := sql.Open("sqlite3_extended", "/Users/huhao/software/test.db")
	conn, err := sql.Open("sqlite3_extended", "/Users/huhao/software/idea_proj/dashboard/test.db")
	return conn, err
}

func parseRows1(rows *sql.Rows) ([][]interface{}, error) {
	var result = make([][]interface{}, 0)
	cols, err := rows.Columns() // Remember to check err afterwards
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(interface{})
	}
	for rows.Next() {
		var user_id string
		var department_id string

		err = rows.Scan(&user_id,&department_id)
		for _,department_id := range strings.Split(department_id,","){
			temp := make([]interface{}, 0)
			temp = append(temp, user_id)
			temp = append(temp, department_id)
			result = append(result, temp)
		}
	}
	return result, nil
}

func queryOne(){
	conn, err := connSqlite()
	defer conn.Close()
	checkErr1(err)
	rows, err := conn.Query(`SELECT node_id,getDeps(node_id,node_type) deps from (
			select distinct node_id,node_type from edge where node_id in (5001504,6003930) and node_type = 11
		) temp`)
	checkErr1(err)
	rows1, err := parseRows1(rows)
	fmt.Println(rows1)
}


func queryOne2(){
	conn, err := connSqlite()
	defer conn.Close()
	checkErr1(err)
	rows, err := conn.Query(`SELECT node_id,getDeps(node_id,node_type) deps from (
			select distinct node_id,node_type from edge where node_type = 11 limit 100
		) temp`)
	checkErr1(err)
	rows1, err := parseRows1(rows) // 必须遍历，否则 func 中的 insert 不会触发执行
	fmt.Printf(`len: %d\n`,len(rows1))
	fmt.Println(rows1)
}

func main(){
	//queryOne()
	queryOne2()
}
