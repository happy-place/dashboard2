package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"regexp"
)

var (
	baseDir, _  = os.Getwd()
)


func add(a int,b int) int {
	return a + b
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func testCsv(){
	dir, _ := os.Getwd()
	db, err := sql.Open("sqlite3", dir + "/userDB.db") //若数据库没有在这个项目文件下，则需要写绝对路径
	checkErr(err)

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS "student" (
	   "name" VARCHAR(64) NULL,
	   "age" INT(6) NULL,
	   "gender" INT(1)
	);
	`)
	checkErr(err)

	_, err = db.Exec("import /Users/huhao/software/idea_proj/dashboard/test/student.csv student")
	checkErr(err)

	rows, err := db.Query(`select * from student`)
	checkErr(err)
	fmt.Println(rows)
}

func parseRows(rows driver.Rows) (driver.Value, error) {
	cols := rows.Columns()
	vals := make([]driver.Value, len(cols))
	for i, _ := range cols {
		vals[i] = new(driver.Value)
	}
	rows.Next(vals)
	return vals[0], nil
}

func testExtend2(){
	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				my_date := func(msg string) (string, error) {
					fmt.Println(msg)
					query, err := conn.Query("select 1+2 as dt", nil)
					if err != nil {
						return "",err
					}
					rows, err := parseRows(query)
					fmt.Println(rows)
					return "haha",nil
				}
				return conn.RegisterFunc("my_date", my_date, true)
			},
		})

	fmt.Println("打开数据")
	var i interface{}
	conn, err := sql.Open("sqlite3_extended", baseDir+ "/foo.db")
	defer conn.Close()
	checkErr(err)
	err = conn.QueryRow(`SELECT my_date('haha')`).Scan(&i)
	checkErr(err)
	fmt.Println(i)
}


func testExtend(){
	regex := func(re, s string) (bool, error) {
		return regexp.MatchString(re, s)
	}
	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return conn.RegisterFunc("regexp2", regex, true)
			},
		})

	fmt.Println("打开数据")
	var i int
	conn, err := sql.Open("sqlite3_extended", baseDir+ "/foo.db")
	defer conn.Close()
	checkErr(err)
	err = conn.QueryRow(`SELECT regexp2("too.*", "seafood")`).Scan(&i)
	checkErr(err)
	fmt.Println(i)
}

/**
	连接、建表、插入 修改 删除 查询
 */
func testExec(){
	fmt.Println("打开数据")
	dir, _ := os.Getwd()
	db, err := sql.Open("sqlite3", dir + "/userDB.db") //若数据库没有在这个项目文件下，则需要写绝对路径
	checkErr(err)

	fmt.Println("生成数据表")
	//数据表除了在命令行提前进行创建，还可以在go程序中创建，如下：
	sql_table := `
CREATE TABLE IF NOT EXISTS "userinfo" (
   "uid" INTEGER PRIMARY KEY AUTOINCREMENT,
   "username" VARCHAR(64) NULL,
   "departname" VARCHAR(64) NULL,
   "created" TIMESTAMP default (datetime('now', 'localtime')) 
);
CREATE TABLE IF NOT EXISTS "userdeatail" (
   "uid" INT(10) NULL,
   "intro" TEXT NULL,
   "profile" TEXT NULL,
   PRIMARY KEY (uid)
);`
	db.Exec(sql_table)//执行数据表

	//插入数据
	fmt.Print("插入数据, ID=")
	stmt, err := db.Prepare("INSERT INTO userinfo(username, departname)  values(?, ?)")
	checkErr(err)
	res, err := stmt.Exec("astaxie", "研发部门")
	checkErr(err)
	id, err := res.LastInsertId()//返回新增的id号
	checkErr(err)
	fmt.Println(id)

	//更新数据
	fmt.Println("更新数据 ")
	stmt, err = db.Prepare("update userinfo set username=? where uid=?")
	checkErr(err)
	res, err = stmt.Exec("astaxieupdate", id)//将新增的id的username修改为astaxieupdate
	checkErr(err)
	affect, err := res.RowsAffected()
	checkErr(err)
	fmt.Println(affect,"条数据被更新")

	//查询数据
	fmt.Println("查询数据")
	rows, err := db.Query("SELECT * FROM userinfo")
	checkErr(err)
	for rows.Next() {
		var uid int
		var username string
		var department string
		var created string
		err = rows.Scan(&uid, &username, &department, &created)
		checkErr(err)
		fmt.Println(uid, username, department, created)
	}

	//删除数据；
	fmt.Print("删除数据,ID=")
	stmt, err = db.Prepare("delete from userinfo where uid=?")
	checkErr(err)
	res, err = stmt.Exec(id)//将想删除的id输入进去就可以删除输入的id
	checkErr(err)
	affect, err = res.RowsAffected()//几条数据受影响：返回int64类型数据
	checkErr(err)
	fmt.Println(id)
	fmt.Println(affect,"条数据被删除")

	//批量删除数据
	IDList:=[]int{3,4}
	for _,id :=range IDList{
		fmt.Println("删除数据,ID=")
		stmt, err = db.Prepare("delete from userinfo where uid=?")
		checkErr(err)
		res, err = stmt.Exec(id)//将想删除的id输入进去就可以删除输入的id
		checkErr(err)
		affect, err = res.RowsAffected()//几条数据受影响：返回int64类型数据
		checkErr(err)
		fmt.Println(id)
		fmt.Println(affect,"条数据被删除")
	}


	//查询数据:上面进行了删除新增的数据，所以并不知道当前数据有几条，再查询一次。
	fmt.Println("查询数据")
	rows, err = db.Query("SELECT * FROM userinfo")
	checkErr(err)
	for rows.Next() {
		var uid int
		var username string
		var department string
		var created string
		err = rows.Scan(&uid, &username, &department, &created)
		checkErr(err)
		fmt.Println(uid, username, department, created)
	}
	db.Close()
}

func main() {
	//testExec()
	testExtend()
	//testCsv()
	//testExtend2()
}

