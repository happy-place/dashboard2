package main

import (
	"dashboard/utils"
	"fmt"
	"os"
)

var (
	dir, _ = os.Getwd()
)

type SiteConfig struct {
	HttpPort  int
	HttpsOn   bool
	Domain    string
	HttpsPort int
}

type NginxConfig struct {
	Port    int
	LogPath string
	Path    string
}

type Clickhouse struct {
	Truncate string
	Upsert   string
	Query    string
}

type Mysql struct {
	Truncate string
	Upsert   string
}

type Script struct {
	Clickhouse Clickhouse
	Mysql      Mysql
}

func parseSite() {
	c2 := utils.ConfigEngine{}
	path := fmt.Sprintf("%s/src/test/test.yaml", dir)
	fmt.Println(path)
	c2.Load(path)

	siteConf := SiteConfig{}
	res := c2.GetStruct("Site", &siteConf)
	fmt.Println(res)

	nginxConfig := NginxConfig{}
	res2 := c2.GetStruct("Nginx", &nginxConfig)
	fmt.Println(res2)

	siteName := c2.GetString("SiteName")
	siteAddr := c2.GetString("SiteAddr")
	fmt.Println(siteName, siteAddr)
}

func parseCk() {
	filePath := fmt.Sprintf("%s/script/%s", dir, "dws_file_7d_statistic_by_global_daily.yaml")
	//fmt.Println(filePath)
	//bytes, err := ioutil.ReadFile(filePath)
	//if err != nil {
	//	log.Fatal("yamlFile.Get err %v", err)
	//}
	//conf := new(Script)
	//yaml.Unmarshal(bytes, &conf)
	//fmt.Println(conf)

	c2 := utils.ConfigEngine{}
	fmt.Println(filePath)
	c2.Load(filePath)

	clickhouse := Clickhouse{}
	c2.GetStruct("Clickhouse", &clickhouse)
	fmt.Printf("%+v", clickhouse)

	mysql := Mysql{}
	c2.GetStruct("Mysql", &mysql)
	fmt.Printf("%+v", mysql)
}

func main() {
	//parseSite()
	parseCk()

}
