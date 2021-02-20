package model

type Clickhouse struct {
	Truncate string
	Upsert   string
	Query    string
}

type Mysql struct {
	Truncate string
	Upsert   string
}

type TreeClickhouse struct {
	Truncate string
	Upsert   string
}

type TreeMysql struct {
	Query    string
}

type FileClickhouse struct {
	Truncate string
	Upsert   string
}

type FileMysql struct {
	FileQuery    string
	LegacyQuery    string
}
