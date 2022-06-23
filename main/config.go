package main

import (
	"errors"
	"fmt"
	"logAgent/tailf"

	"github.com/astaxie/beego/config"
)

var (
	appConfig *Config
)

type Config struct {
	LogLevel    string
	LogPath     string
	ChanSize    int
	kafkaAddr   string
	CollectConf []tailf.CollectConf
}

func loadCollectConf(conf config.Configer) (err error) {
	cc := tailf.CollectConf{}
	cc.LogPath = conf.String("collect::log_path")
	if len(cc.LogPath) == 0 {
		return errors.New("invalid collect::log_path")
	}
	cc.Topic = conf.String("collect::topic")
	if len(cc.Topic) == 0 {
		return errors.New("invalid collect::topic")
	}

	appConfig.CollectConf = append(appConfig.CollectConf, cc)
	return
}

func loadConf(adapter, filename string) error {
	conf, err := config.NewConfig(adapter, filename)
	if err != nil {
		fmt.Println("new config failed")
	}

	//读配置，一般为全局可用

	appConfig = &Config{}

	appConfig.LogLevel = conf.String("logs::log_level")
	if len(appConfig.LogLevel) == 0 {
		appConfig.LogLevel = "Debug"
	}

	appConfig.LogPath = conf.String("logs::log_path")
	if len(appConfig.LogPath) == 0 {
		appConfig.LogPath = "./logs"
	}

	appConfig.ChanSize, err = conf.Int("collect::chanSize")
	if err != nil {
		// fmt.Println("invaild collect::chanSize")
		// return err
		appConfig.ChanSize = 100
	}

	appConfig.kafkaAddr = conf.String("kafka::server_addr")
	if len(appConfig.kafkaAddr) == 0 {
		err = errors.New("invalid kafka address")
		return err
	}

	err = loadCollectConf(conf)
	if err != nil {
		fmt.Println("load collect conf failed,", err)
	}
	return nil
}
