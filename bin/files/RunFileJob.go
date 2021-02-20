package main

/**
效能看板需求：https://shimo.im/docs/WYrtqDKvtdQxWWxw
后端埋点参照：https://shimo.im/sheets/vBaBIDDj3dg5JlQM/NF5AK?referer=mail_collaborator_invite&parent_id=39791541&recipient=panxianao%40shimo.im

运行参数：
	env：运行环境
		idea - 测试业务流程运行是否通畅 clickhouse、 mysql
		dev - 开发环境，测试是业务
		saas - saas 线上部署运行
	debug：是否打印明细sql (true/false)
	sql: script 脚本目录
	limit：抓取 svc_tree.edge 条数，默认-1，即抓取全部，方便测试使用
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /Users/huhao/Desktop/job-file RunFileJob.go
	./RunTreeJob -env idea -sql ./script > 2020-12-14.log 2>&1 &

	实现如下逻辑
	ALTER TABLE shard.files ON CLUSTER 'shard2-repl2' DELETE WHERE guid is not null;
	INSERT INTO all.files (name,guid,file_type,file_subtype,file_loc)
	SELECT
		coalesce(s.name,d.name) AS name,
		coalesce(s.guid,d.guid) AS guid,
		coalesce(s.type,d.type) AS file_type,
		coalesce(s.sub_type,d.sub_type) AS file_subtype,
		coalesce(s.file_loc,d.file_loc) AS file_loc
	FROM
		(
			SELECT
				name,guid,type,sub_type,
				'space' AS file_loc
			FROM svc_file.file
			WHERE guid IS NOT NULL
		)s
			FULL JOIN
		(
			SELECT
				 name,guid,type,sub_type,
				 'desktop' AS file_loc
			 FROM svc_file.file_legacy
			 WHERE guid IS NOT NULL
		)d
		ON d.guid = s.guid


	SELECT name,guid,type,sub_type, 'space' AS file_loc FROM svc_file.file WHERE guid IS NOT NULL
	union all
	SELECT name,guid,type,sub_type,'desktop' AS file_loc FROM svc_file.file_legacy WHERE guid IS NOT NULL

;


*/
import (
	. "dashboard/config"
	"dashboard/model"
	"dashboard/utils"
	"database/sql"
	"os"
	"strings"
	"flag"
	"fmt"
	"time"
	_ "time/tzdata"
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
)

var (
	env string
	runDebug bool

	dir string

	hashFile       = make(map[string]interface{})

	taskChan = make(chan []interface{}, 1000000)
	collectChan = make(chan []interface{}, 1000000)

	maxConsumer = 5000
	printCnt = 10000

	signalChan = make(chan bool,1)

	conf *string

)

func parseFile(rows *sql.Rows) error{
	for rows.Next() {
		// guid,name,typ,sub_type,file_loc
		var guid,name,typ,sub_type,file_loc string
		var created_at int64
		var created_by string
		err := rows.Scan(&guid,&name,&typ,&sub_type,&created_at,&created_by,&file_loc)
		if err != nil {
			utils.LogError(err)
			return err
		}
		hashFile[guid] = nil
		collectChan <- []interface{}{guid,name,typ,sub_type,created_at,created_by,file_loc}
	}
	utils.LogInfo(`file node size: %d`,len(hashFile))
	return nil
}

func parseLegacy(rows *sql.Rows) error{
	cnt := 0
	for rows.Next() {
		// guid,name,typ,sub_type,file_loc
		var guid,name,typ,sub_type,file_loc string
		var created_at int64
		var created_by string
		err := rows.Scan(&guid,&name,&typ,&sub_type,&created_at,&created_by,&file_loc)
		if err != nil {
			utils.LogError(err)
			return err
		}
		taskChan <- []interface{}{guid,name,typ,sub_type,created_at,created_by,file_loc}
		cnt ++
	}
	utils.LogInfo(`file legacy node size: %d`,cnt)
	return nil
}

/**
	访问 svc_file 分别下载 file 和 file_legacy 到 hashFile 和 hashFileLegacy
*/
func producer(){
	mysqlScript := &model.FileMysql{}
	utils.LoadFromYaml(dir,"FileMysql", "svc_file", mysqlScript)

	connConf := Configuration.File_Mysql

	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		connConf.User, connConf.Pass, connConf.Host, connConf.Port, connConf.Db))
	if err != nil {
		utils.LogError(err)
		return
	}
	if err := conn.Ping(); err != nil {
		utils.LogError(err)
		return
	}
	defer conn.Close()

	sql := mysqlScript.FileQuery
	utils.LogInfo(sql)
	result, err := conn.Query(sql)
	if err != nil {
		utils.LogError(err)
		return
	}
	parseFile(result)

	sql = mysqlScript.LegacyQuery
	utils.LogInfo(sql)
	result, err = conn.Query(sql)
	if err != nil {
		utils.LogError(err)
		return
	}
	parseLegacy(result)
}

/**
启动消费者消费 node_type=11 溯源任务
*/
func consumer(){
	for task := range taskChan {
		// guid,name,typ,sub_type,created_at,updated_at,deleted_at,file_loc
		find(task)
	}
}

func find(task []interface{}) {
	guid := task[0]
	_,existed := hashFile[guid.(string)]
	if !existed{
		collectChan <- task
	}
}

func collect(){
	connConf := Configuration.Clickhouse
	cluserName := connConf.ClusterName

	ckScript := &model.TreeClickhouse{}
	utils.LoadFromYaml(dir,"FileClickhouse", "svc_file", ckScript)

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf.Host, connConf.Port, connConf.User, connConf.Pass)

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		utils.LogError(err)
		return
	}
	if err := conn.Ping(); err != nil {
		utils.LogError(err)
		return
	}

	defer conn.Close()

	truncateCkSql := strings.ReplaceAll(ckScript.Truncate,"{CLUSTER_NAME}",cluserName)
	utils.LogDebug(runDebug,truncateCkSql)
	_, err = conn.Exec(truncateCkSql)
	if err != nil {
		utils.LogError(err)
		return
	}

	// 批量插入
	tx, err := conn.Begin()
	if err != nil{
		utils.LogError(err)
		return
	}
	stmt, err := tx.Prepare(ckScript.Upsert)
	if err != nil {
		utils.LogError(err)
		return
	}
	defer stmt.Close()

	isOver := false
	cnt := 0
	for ;!isOver; {
		select {
			case result := <- collectChan:
				if result != nil { // close(collectChan) 发出 ""
					guid := result[0]
					name := result[1]
					typ := result[2]
					sub_type := result[3]
					created_at := result[4]
					created_by := result[5]
					file_loc := result[6]
					_, err = stmt.Exec(guid,name,typ,sub_type,created_at,created_by,-1,"null",-1,"null",file_loc)
					if err != nil {
						utils.LogError(err)
						return
					}
					cnt ++
					if cnt % printCnt == 0 {
						utils.LogInfo(fmt.Sprintf("submit %d rows",cnt))
					}
				}
			case <-time.After(time.Second * 60): // 3秒未收到数据，主动关闭，并停止处理
				utils.LogInfo("collectChan maybe need to close")
				close(collectChan)
				isOver = true
		}
	}

	err = tx.Commit()
	if err != nil {
		utils.LogError(err)
		return
	}

	utils.LogInfo(fmt.Sprintf("insert %d rows to clickhouse",cnt))

	signalChan <- true
}

func mergeFile(){
	go collect()
	for i:=0;i<maxConsumer;i++{
		go consumer()
	}
	go producer()
	<- signalChan
}

func init() {
	var runDebugStr string
	flag.StringVar(&env, "env", "prod", "运行环境：dev saas")
	flag.StringVar(&runDebugStr, "debug", "false", "是否打印debug信息")
	flag.StringVar(&dir, "sql", utils.GetScriptDir(), "script目录")
	flag.Parse()

	runDebug = runDebugStr == "true"
	if !runDebug {
		printCnt = 10000000
	}
	utils.LogInfo("input args: env=%v, runDebug=%v",env,runDebug)

	if env == "dev" || env == "saas"{
		root, _ := os.Getwd()
		conf = flag.String("conf", fmt.Sprintf(`%s/%s.json`,root,env), "config path")
		utils.LogInfo(*conf)
	}else {
		conf = flag.String("conf", "/data/config/production.json", "config path")
	}

	Init(*conf)
}

func main() {
	mergeFile()
}
