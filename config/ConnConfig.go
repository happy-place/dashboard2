package config

import (
	"encoding/json"
	"github.com/jinzhu/configor"
	"github.com/sirupsen/logrus"
)

var (
	config = map[string]map[string]map[string]string{
		"idea": { // env
			"clickhouse": { // source
				"host":         "192.168.222.53",
				"port":         "9000",
				"user":         "chadmin",
				"pass":         "1vF3tO3EK2Av5",
				"db":           "all",
				"cluster_name": "shard2-repl1",
			},
			"boss_mysql": {
				"host": "rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "bigdata",
				"pass": "7b2Nu6JFEtgH6",
				"db":   "boss",
			},
			"org_mysql": {
				"host": "localhost",
				"port": "3306",
				"user": "root",
				"pass": "root",
				"db":   "svc_tree",
			},
			"tree_mysql": {
				"host": "localhost",
				"port": "3306",
				"user": "root",
				"pass": "root",
				"db":   "svc_tree",
			},
			"file_mysql": {
				"host": "rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "shimodev",
				"pass": "F7856b920fdbbf56ac",
				"db":   "svc_file",
			},
		},
		"dev": { // env
			"clickhouse": { // source
				"host":         "192.168.222.53",
				"port":         "9000",
				"user":         "chadmin",
				"pass":         "1vF3tO3EK2Av5",
				"db":           "all",
				"cluster_name": "shard2-repl1",
			},
			"boss_mysql": {
				"host": "rm-2ze81q6239y512n730o.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "bigdata",
				"pass": "7b2Nu6JFEtgH6",
				"db":   "boss",
			},
			"org_mysql": {
				"host": "rm-2ze81q6239y512n730o.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "org_ro",
				"pass": "aldashHSDa340",
				"db": "organization",
			},
			"tree_mysql": {
				"host": "pc-2ze6z9m3hp37y77om.rwlb.rds.aliyuncs.com",
				"port": "3306",
				"user": "dev_tree",
				"pass": "Lo3af578dd082535",
				"db":   "svc_tree",
			},
			"file_mysql": {
				"host": "rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "shimodev",
				"pass": "F7856b920fdbbf56ac",
				"db":   "svc_file",
			},
		},
		"saas": {
			"clickhouse": {
				"host": "clickhouse-pro.clickhouse",
				//"host": "10.111.179.169",
				"port":         "9000",
				"user":         "chadmin",
				"pass":         "6hAFCyH0Bw0JN",
				"db":           "default",
				"cluster_name": "shard2-repl2",
			},
			"boss_mysql": {
				"host": "rm-2ze06v5ed2gb1ol2l.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "boss_stats",
				"pass": "P73e485173fa6a7",
				"db":   "svc_boss_stats",
			},
			"org_mysql": {
				"host": "rm-2zezmon0g7635nkv4.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "org_db",
				"pass": "dashHSDa34004e30",
				"db": "organization",
			},
			"tree_mysql": {
				"host": "rm-2zef2h27mdnms0009.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "rotree",
				"pass": "IDS6955b171bdf7",
				"db":   "svc_tree",
			},
			"file_mysql": {
				"host": "rm-2ze78jx65s1jo7d01.mysql.rds.aliyuncs.com",
				"port": "3306",
				"user": "odps_file",
				"pass": "P0a7b736693707",
				"db":   "svc_file",
			},
		},
	}
)

type Config struct {
	Clickhouse struct {
		Host    string `json:"host"`
		Port    string `json:"port"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		Db    	string `json:"db"`
		ClusterName    	string `json:"cluster_name"`
	}

	Boss_Mysql struct {
		Host    string `json:"host"`
		Port    string `json:"port"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		Db    	string `json:"db"`
	}

	Tree_Mysql struct {
		Host    string `json:"host"`
		Port    string `json:"port"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		Db    	string `json:"db"`
	}

	File_Mysql struct {
		Host    string `json:"host"`
		Port    string `json:"port"`
		User    string `json:"user"`
		Pass    string `json:"pass"`
		Db    	string `json:"db"`
	}

	Debug  bool `env:"DEBUG" default:"true"`
}


var Configuration *Config

func Init(file string) (conf *Config, err error) {
	conf = &Config{}
	err = configor.Load(conf, file, "dev.json")
	Configuration = conf
	if conf.Debug {
		c, _ := json.Marshal(conf)
		logrus.Debugf("configurations %s", c)
	}
	return
}

