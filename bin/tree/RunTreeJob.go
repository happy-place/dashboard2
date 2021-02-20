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
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /User/huhao/Desktop/job-tree RunTreeJob.go
	./RunTreeJob -env idea -sql ./script > 2020-12-14.log 2>&1 &
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

	debugLimit int

	dir string

	hashUser = make(map[string][][]string)
	hashDep  =  make(map[string][][]string)

	taskChan = make(chan [][]string, 10000)

	taskNum = 0

	userDepsChan = make(chan string, 10000)
	userDeps = make(map[string]interface{},0)

	maxConsumer = 5000

	signalChan = make(chan bool, 1)

	closeWait = time.Second * 3

	conf *string
)

func parse(rows *sql.Rows) error{
	cnt := 0
	for rows.Next() {
		var id_type,node_id,parent_id_type,parent_id string
		err := rows.Scan(&id_type,&node_id,&parent_id_type,&parent_id)
		if err != nil {
			return err
		}
		if  strings.HasSuffix(id_type,"-11") {
			arr, existed := hashUser[id_type]
			if !existed{
				arr = make([][]string,0)
			}
			arr = append(arr, []string{node_id,parent_id_type,parent_id})
			hashUser[id_type] = arr
		} else{
			arr, existed := hashDep[id_type]
			if !existed{
				arr = make([][]string,0)
			}
			arr = append(arr, []string{node_id,parent_id_type,parent_id})
			hashDep[id_type] = arr
		}
		cnt ++
	}
	utils.LogInfo(`rows : %d`,cnt)
	taskNum = len(hashUser)
	if debugLimit != -1 {
		taskNum = debugLimit
	}
	utils.LogInfo(`user node size: %d`,len(hashUser))
	utils.LogInfo(`dep node size: %d`,len(hashDep))
	utils.LogInfo(`taskNum: %d`,taskNum)
	return nil
}


/**
	下载 svc_tree 到本地
	基于代码进行数据分类
 	node_type=11
	node_type in (9,10)
*/
func download() error{
	mysqlScript := &model.TreeMysql{}
	utils.LoadFromYaml(dir,"TreeMysql", "svc_tree", mysqlScript)

	connConf := Configuration.Tree_Mysql
	utils.LogInfo(connConf.Host)

	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		connConf.User, connConf.Pass, connConf.Host, connConf.Port, connConf.Db))
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		utils.LogError(err)
		return err
	}
	defer conn.Close()

	sqlUser := mysqlScript.Query
	utils.LogInfo(sqlUser)
	result, err := conn.Query(sqlUser)
	if err != nil {
		return err
	}
	parse(result)

	return nil
}

/*
	启动生产者生产数据 node_type=11 类型任务
*/
func producer(){
	cnt := 0
	for key := range hashUser{
		// hashUser: id_type -> node_id,parent_id_type,parent_id
		taskChan <- hashUser[key]
		cnt ++
		if cnt == debugLimit{
			break
		}
	}
	close(taskChan)
}

/**
启动消费者消费 node_type=11 溯源任务
*/
func consumer(){
	for task := range taskChan {
		// node_id,parent_id_type,parent_id
		for _,sub := range task{
			find(sub[0],sub[1],sub[2])
		}
	}
}

func find(node_id string,key string,deps string) {
	arr,existed := hashDep[key]
	if !existed {
		userDepsChan <- fmt.Sprintf(`%s-%s`,node_id,deps)
	}else{
		for _,sub := range arr{
			deps = fmt.Sprintf(`%s,%s`,sub[2],deps)
			find(node_id,sub[1],deps)
		}
	}
}

func collect(){
	isOver := false
	for ;!isOver; {
		select {
			case result := <- userDepsChan:
				if result != "" { // close(userDepsChan) 发出 ""
					temp := strings.Split(result,"-")
					for _,dep := range strings.Split(temp[1],","){
						key := fmt.Sprintf(`%s,%s`,temp[0],dep)
						userDeps[key] = nil
					}
				}
			case <-time.After(closeWait): // 3秒未收到数据，主动关闭，并停止处理
				utils.LogInfo("userDepsChan maybe need to close")
				close(userDepsChan)
				isOver = true
		}
	}
	signalChan <- true
}

/*
	结果汇总，输出到 clickhouse
*/
func upload() error{
	<- signalChan
	connConf := Configuration.Clickhouse
	cluserName := connConf.ClusterName

	ckScript := &model.TreeClickhouse{}
	utils.LoadFromYaml(dir,"TreeClickhouse", "svc_tree", ckScript)

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf.Host, connConf.Port, connConf.User, connConf.Pass)

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		return err
	}
	defer conn.Close()

	truncateCkSql := strings.ReplaceAll(ckScript.Truncate,"{CLUSTER_NAME}",cluserName)
	utils.LogDebug(runDebug,truncateCkSql)
	_, err = conn.Exec(truncateCkSql)
	if err != nil {
		return err
	}

	// 批量插入
	tx, err := conn.Begin()
	if err != nil{
		return err
	}
	stmt, err := tx.Prepare(ckScript.Upsert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for line := range userDeps {
		temp := strings.Split(line,",")
		_, err = stmt.Exec(temp[0],temp[1])
		if err != nil {
			utils.LogError(`user_id=%s %v`,temp[0],err)
			return err
		}
	}
	utils.LogInfo(fmt.Sprintf(`insert %d rows to clickhouse user_dep`,len(userDeps)))
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func parseSvcTree(){
	err := download()
	if err != nil{
		panic(err)
	}
	go collect()
	for i:=0;i<maxConsumer;i++{
		go consumer()
	}
	go producer()
	err = upload()
	if err != nil{
		panic(err)
	}
}

func init() {
	var runDebugStr string
	flag.StringVar(&env, "env", "prod", "运行环境：dev saas")
	flag.StringVar(&runDebugStr, "debug", "false", "是否打印debug信息")
	flag.StringVar(&dir, "sql", utils.GetScriptDir(), "script目录")
	flag.IntVar(&debugLimit, "limit", -1, "测试条数(-1：全部)")
	flag.Parse()

	runDebug = runDebugStr == "true"
	if runDebug {
		closeWait = time.Hour * 1
	}

	utils.LogInfo("input args: env=%v, runDebug=%v, debugLimit=%v ",env,runDebug,debugLimit)

	if env == "dev" || env == "saas"{
		root, _ := os.Getwd()
		conf = flag.String("conf", fmt.Sprintf(`%s/%s.json`,root,env), "config path")
	}else {
		conf = flag.String("conf", "/data/config/production.json", "config path")
	}

	Init(*conf)

}

func main() {
	parseSvcTree()
}
