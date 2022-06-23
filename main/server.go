package main

import (
	"logAgent/kafka"
	"logAgent/tailf"
	"time"

	"github.com/astaxie/beego/logs"
)

func serverRun() (err error) {
	for {
		msg := tailf.GetOneLine()
		err = sendToKafka(msg)
		if err != nil {
			logs.Error("send tp kafka err,%v", err)
			time.Sleep(time.Microsecond * 100)
			continue
		}
	}
	// return
}

func sendToKafka(msg *tailf.TextMsg) (err error) {
	// fmt.Printf("send msg:%v,  topic:%v\n", msg.Msg, msg.Topic)
	err = kafka.SendToKafka(msg.Msg, msg.Topic)
	return
}
