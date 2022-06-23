package tailf

import (
	"fmt"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/hpcloud/tail"
)

// 收集日志的config信息
type CollectConf struct {
	LogPath string
	Topic   string
}

// 跟踪文件的打印的信息的实例
// 包含*tail.Tail结构体，与该跟踪文件的配置信息
type TailObj struct {
	Tail   *tail.Tail
	Config CollectConf
}

// 管理多个跟踪文件
type TailObjMgr struct {
	TailObjs []*TailObj
	MsgChan  chan *TextMsg
}

// 使用一个channel　将打 印的信息发送给main包，再传给kafka
// 两个字段　消息　写入哪个topic
type TextMsg struct {
	Msg   string
	Topic string
}

//不能被外包使用
var (
	tailObjMgr *TailObjMgr
)

func InitTail(conf []CollectConf, chanSize int) (err error) {
	if len(conf) == 0 {
		err = fmt.Errorf("invalid config for log collect ,conf=%v", conf)
		return
	}

	tailObjMgr = &TailObjMgr{
		MsgChan: make(chan *TextMsg, chanSize),
	}
	for _, v := range conf {
		tailObj := &TailObj{
			Config: v,
		}

		tails, err := tail.TailFile(v.LogPath, tail.Config{
			ReOpen: true,
			Follow: true,
			// Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, //定位读取位置 最后
			MustExist: false, //要求文件必须存在或者暴露
			Poll:      true,
		})
		if err != nil {
			fmt.Printf("tail.TailFile %v, err:%v\n", v, err)
			continue
		}
		tailObj.Tail = tails

		tailObjMgr.TailObjs = append(tailObjMgr.TailObjs, tailObj)

		//配置好tail环境后，实现读取功能
		go readFormTail(tailObj)

	}

	return
}

func readFormTail(tailObj *TailObj) {
	// var msg *tail.Line
	flag := true
	for flag {
		line, ok := <-tailObj.Tail.Lines
		if !ok {
			logs.Warn("tail file close reopen, filename:%s\n", tailObj.Tail.Filename)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		textMsg := &TextMsg{
			Msg:   line.Text,
			Topic: tailObj.Config.Topic,
		}

		tailObjMgr.MsgChan <- textMsg

		//优雅退出
		//配置一个退出的信号，在环境变量中配置，指针形式
		// 当接收到退出信号将flag置为true
	}
}

//使用函数方式读取信息
func GetOneLine() (msg *TextMsg) {
	msg = <-tailObjMgr.MsgChan
	return
}
