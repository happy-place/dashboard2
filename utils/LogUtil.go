package utils

import (
	"fmt"
	"log"
)

func LogDebug(runDebug bool,v ...interface{}) {
	if runDebug {
		if len(v) == 1 {
			log.Printf("[DEBUG] %s\n", v...)
		} else {
			log.Printf("[DEBUG] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
		}
	}
}

func LogInfo(v ...interface{}) {
	if len(v) == 1 {
		log.Printf("[INFO] %s\n", v...)
	} else {
		log.Printf("[INFO] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
	}
}

func LogError(v ...interface{}) {
	if len(v) == 1 {
		log.Printf("[ERROR] %s\n", v...)
	} else {
		log.Printf("[ERROR] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
	}
}
