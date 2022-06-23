package main

import (
	"fmt"

	"github.com/astaxie/beego/config"
)

func main() {
	conf, err := config.NewConfig("ini", "./logcollect.conf")
	if err != nil {
		fmt.Println("new config failed, err:", err)
		return
	}

	port, err := conf.Int("server::port")
	if err != nil {
		fmt.Println("read server:port failed, err:", err)
		return
	}
	fmt.Println("Port:", port)

	log_level := conf.String("logs::log_level")
	fmt.Println("log_level:", log_level)

	log_port, err := conf.Int("logs::port")
	if err != nil {
		fmt.Println("read logs:port failed, err:", err)
		return
	}
	fmt.Println("log_Port:", log_port)

	log_path := conf.String("logs::log_path")
	fmt.Println("log_path:", log_path)
}
