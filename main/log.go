package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/astaxie/beego/logs"
)

func converLogLevel(level string) int {
	switch strings.ToLower(level) {
	case "critical":
		return logs.LevelCritical
	case "warn":
		return logs.LevelWarn
	case "warning":
		return logs.LevelWarning
	case "info":
		return logs.LevelInfo
	case "trace":
		return logs.LevelTrace
	default: //"debug"
		return logs.LevelDebug
	}
}

func initLogger() (err error) {
	config := make(map[string]interface{})
	config["filename"] = appConfig.LogPath
	config["level"] = converLogLevel(appConfig.LogLevel)

	configStr, err := json.Marshal(config)
	if err != nil {
		fmt.Println("init logger failed, marshal err=", err)
		return
	}

	err = logs.SetLogger(logs.AdapterFile, string(configStr))
	if err != nil {
		fmt.Println("logs .SetLogger failed")
		return
	}
	return
}
