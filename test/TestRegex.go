package main

import (
	"fmt"
	"regexp"
	"strings"
)

/**
┌──────ldate─┬─team_id─┬─guid─────────────┬─name───────────────────────┬─type─┬─sub_type─┬─file_views─┬─row_number─┐
│ 2020-12-15 │ 191577  │ 5rk9dErM61CMyRqx │ 201222 Tue. Drop the "Dr." │ 2    │ -2       │          1 │         64 │
└────────────┴─────────┴──────────────────┴────────────────────────────┴──────┴──────────┴────────────┴────────────┘
*/

func replace(){
	t :="nadiand\\"
	t = strings.ReplaceAll(t, "\"", "\\\"")
	fmt.Println(t)
}

func trimSuffix(){
	t := "abc,"
	if strings.HasSuffix(t,","){
		fmt.Println(strings.TrimSuffix(t,","))
	}
}

func replace1(){
	//text := "\"world\""
	text := "\"2020-12-15\",\"191577\",\"5rk9dErM61CMyRqx\",\"201222 Tue. Drop the 'Dr.'\",\"2\",\"-2\",\"1\",\"64\""
	//pattern := "\"([^\"]*)\""

	var e interface{}
	a := "201222 Tue. Drop the 'Dr.'"
	e = &a
	t := *e.(*string)
	t = strings.ReplaceAll(t, "'", "\\'")
	fmt.Println(t)
	//matchString, err := regexp.MatchString(pattern, text)
	meta := regexp.QuoteMeta(text)
	fmt.Println(meta)
	//if err != nil {
	//	panic(err)
	//}
	//println(matchString)
}

func main() {
	//replace1()
	//replace()
	trimSuffix()
}
