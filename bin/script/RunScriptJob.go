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
	start\end: 起止日期，不传的话，默认是运行昨天

./RunJob -env saas -sql ./script > 2020-12-14.utils.Log 2>&1 &
go run RunScriptJob.go -env saas -debug false --start 2020-12-01 -end 2020-12-03
go run RunScriptJob.go -env saas -debug false
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /User/huhao/Desktop/job-script  RunScriptJob.go

 ck --query "insert into all.dws_enterprise_td_usage_statistic_by_global_daily FORMAT TSV" < ./2020-12-14.tsv
*/
import (
	. "dashboard/config"
	"dashboard/model"
	"dashboard/utils"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
	_ "time/tzdata"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
)

var (
	env string
	runDebug bool
	start    time.Time
	end      time.Time

	dir string

	//conf  map[string]map[string]string
	tasks []string

	memLimit int64

	writeDev bool

	// golang日期格式示例："2006-01-02 15:04:05"
	layout      = "2006-01-02"
	minusday, _ = time.ParseDuration("-24h")
	plusday, _  = time.ParseDuration("24h")

	maxRetry = 3
	sleepSec = 2 * time.Second

	fetchTree bool

	baseDir,_ = os.Getwd()

	conf *string
)


func runShell(cmd string, arg ...string) {
	var output []byte
	var handler *exec.Cmd
	handler = exec.Command(cmd,arg...)
	output, err := handler.Output()
	if err != nil {
		utils.LogError(err)
		os.Exit(1)
	}
	var result = strings.Trim(string(output), "\n")
	utils.LogInfo(result)
}

func runClickhouseJob(date string, task string) ([][]interface{}, error) {
	var result [][]interface{}
	connConf := Configuration.Clickhouse
	cluserName := connConf.ClusterName

	ckScript := &model.Clickhouse{}
	utils.LoadFromYaml(dir,"Clickhouse", task, ckScript)

	truncCkSql := strings.ReplaceAll(strings.ReplaceAll(ckScript.Truncate,
		"{CLUSTER_NAME}", cluserName), "{DATE}", date)

	upsertCkSql :=strings.ReplaceAll(ckScript.Upsert, "{DATE}", date)
	queryCkSql := strings.ReplaceAll(ckScript.Query, "{DATE}", date)

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf.Host, connConf.Port, connConf.User, connConf.Pass)

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	defer conn.Close()

	//groupby_buffer_sql := fmt.Sprintf("set max_bytes_before_external_group_by=%d", memLimit)
	//_, err = conn.Exec(groupby_buffer_sql)
	//if err != nil {
	//	return nil, err
	//}
	//utils.LogDebug(runDebug,groupby_buffer_sql)
	//
	//max_mem_sql := fmt.Sprintf("set max_memory_usage=%d", memLimit * 2)
	//_, err = conn.Exec(max_mem_sql)
	//if err != nil {
	//	return nil, err
	//}
	//utils.LogDebug(runDebug,max_mem_sql)

	utils.LogDebug(runDebug,truncCkSql)
	_, err = conn.Exec(truncCkSql)
	if err != nil {
		return nil, err
	}

	utils.LogDebug(runDebug,upsertCkSql)
	_, err = conn.Exec(upsertCkSql)
	if err != nil {
		return nil, err
	}

	// 休眠 10 秒，保证 ck 个分区数据就绪
	time.Sleep(time.Second * 10)

	utils.LogDebug(runDebug,queryCkSql)
	rows, err := conn.Query(queryCkSql)
	if err != nil {
		return nil, err
	}
	result, err = parseRows(rows)
	if err != nil {
		return nil, err
	}
	utils.LogInfo("fetch %d rows", len(result))

	if len(result) > 0 {
		utils.LogDebug(runDebug,result[0])
	}

	return result, nil
}

func parseRows(rows *sql.Rows) ([][]interface{}, error) {
	var result = make([][]interface{}, 0)
	cols, err := rows.Columns() // Remember to check err afterwards
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(interface{})
	}
	for rows.Next() {
		temp := make([]interface{}, 0)
		err = rows.Scan(vals...)
		for i, e := range vals {
			if types[i].DatabaseTypeName() == "Date" {
				temp = append(temp, string([]byte(fmt.Sprintf("%s", *e.(*interface{})))[:10]))
			} else if types[i].DatabaseTypeName() == "DateTime" {
				temp = append(temp, string([]byte(fmt.Sprintf("%s", *e.(*interface{})))[:19]))
			} else if strings.Contains(types[i].DatabaseTypeName(), "String") {
				t := fmt.Sprintf("%v", *e.(*interface{}))
				if strings.HasSuffix(t,"\\"){
					t = fmt.Sprintf(`%s\\`,strings.TrimSuffix(t,"\\"))
				}
				t = strings.ReplaceAll(t, "\"", "\\\"")
				temp = append(temp, t)
			} else {
				t := *e.(*interface{})
				if t == nil {
					temp = append(temp, "null")
				} else {
					temp = append(temp, t)
				}
			}
		}
		result = append(result, temp)
	}
	return result, nil
}

func runMysqlJob(date string, task string, argsArr [][]interface{}) error {
	connConf := Configuration.Boss_Mysql
	mysqlScript := &model.Mysql{}
	utils.LoadFromYaml(dir,"Mysql", task, mysqlScript)
	truncMysqlSql := strings.ReplaceAll(mysqlScript.Truncate, "{DATE}", date)
	upsertMysqlSql := strings.ReplaceAll(mysqlScript.Upsert, "{DATE}", date)

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

	utils.LogInfo(truncMysqlSql)
	_, err = conn.Exec(truncMysqlSql)
	if err != nil {
		return err
	}

	utils.LogDebug(runDebug,upsertMysqlSql)

	// 批量插入
	batch := 1000
	batchValues := make([]string, 0)
	temp := strings.Split(upsertMysqlSql, "(")
	template := strings.ReplaceAll("("+temp[1], "?", "\"%v\"")
	for i, args := range argsArr {
		batchValues = append(batchValues, fmt.Sprintf(template, args...))
		if (i+1)%batch == 0 || (i+1) == len(argsArr) {
			sql := temp[0] + strings.Join(batchValues, ",")
			_, err = conn.Exec(sql)
			batchValues = make([]string, 0)
			utils.LogInfo("insert job: [%d/%d]", i+1, len(argsArr))
			if err != nil {
				utils.LogError("insert to %d row", i+1)
				return err
			}
		}
	}

	return nil
}

func schedule() {
	for start.Before(end) || start.Equal(end) {
		date := start.Format(layout)
		for i, task := range tasks {
			utils.LogInfo("[%d/%d]: run the job of '%s' at %s", i+1, len(tasks), task, date)
			retry := 0
			var rows [][]interface{}
			var err error
			for retry < maxRetry {
				rows, err = runClickhouseJob(date, task)
				if err != nil {
					utils.LogError("the %d time run failed, sleep for 2 seconds. %v", retry+1, err)
					retry += 1
					if retry == maxRetry {
						os.Exit(1)
					}
					time.Sleep(sleepSec)
				} else {
					break
				}
			}

			retry = 0
			for retry < maxRetry {
				err = runMysqlJob(date, task, rows)
				if err != nil {
					utils.LogError("the %d time run failed, sleep for 2 seconds. %v", retry+1, err)
					retry += 1
					if retry == maxRetry {
						os.Exit(1)
					}
					time.Sleep(sleepSec)
				} else {
					break
				}
			}
		}

		utils.LogInfo("%s run success", date)
		start = start.Add(plusday)
	}
}

func getDateObj(date string) (time.Time, error) {
	location, _ := time.LoadLocation("Local")
	//layout="2006-01-02 15:04:05"
	dateObj, err := time.ParseInLocation(layout, date, location)
	return dateObj, err
}

func init() {
	var runDebugStr, tasksStr, startStr, endStr, writeDevStr string
	flag.StringVar(&env, "env", "prod", "运行环境：dev saas")
	flag.StringVar(&startStr, "start", time.Now().Add(minusday).Format("2006-01-02"), "起始日期")
	flag.StringVar(&endStr, "end", time.Now().Add(minusday).Format("2006-01-02"), "结束日期")
	flag.StringVar(&tasksStr, "tasks", "all", "需要执行的任务(默认执行全部)")
	flag.StringVar(&runDebugStr, "debug", "false", "是否打印debug信息")
	flag.StringVar(&dir, "sql", utils.GetScriptDir(), "script目录")
	flag.Int64Var(&memLimit, "mem_limit", 21474836480, "clickhouse内存设置")

	flag.Parse()

	utils.LogInfo("input args: env=%v, start=%v, end=%v, debug=%v, tasks=%v, write_dev=%v",
		env, startStr, endStr, runDebug, tasksStr, writeDevStr)

	if env == "dev" || env == "saas"{
		root, _ := os.Getwd()
		conf = flag.String("conf", fmt.Sprintf(`%s/%s.json`,root,env), "config path")
	}else {
		conf = flag.String("conf", "/data/config/production.json", "config path")
	}

	Init(*conf)

	if tasksStr == "all" {
		tasks = GetTasks()
	} else {
		tasks = strings.Split(strings.ReplaceAll(tasksStr, " ", ""), ",")
	}

	var err error
	start, err = getDateObj(startStr)
	if err != nil {
		utils.LogError(err)
		os.Exit(1)
	}
	end, err = getDateObj(endStr)
	if err != nil {
		utils.LogError(err)
		os.Exit(1)
	}

	runDebug = runDebugStr == "true"



}

func main() {
	schedule()
}
