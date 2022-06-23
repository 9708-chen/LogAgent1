package kafka

// import (
// 	"fmt"

// 	"github.com/Shopify/sarama"
// )

// func main() {

// 	//配置kafka环境
// 	config := sarama.NewConfig()
// 	config.Producer.RequiredAcks = sarama.WaitForAll
// 	config.Producer.Partitioner = sarama.NewRandomPartitioner
// 	config.Producer.Return.Successes = true

// 	client, err := sarama.NewSyncProducer([]string{"10.141.65.188:9092"}, config)
// 	if err != nil {
// 		fmt.Println("producer close, err:", err)
// 		return
// 	}
// 	defer client.Close()

// 	for i := 0; i < 10; i++ {
// 		msg := &sarama.ProducerMessage{}
// 		msg.Topic = "nginxLogTest"
// 		msg.Value = sarama.StringEncoder("this is a good test, my message is good~~12")

// 		pid, offset, err := client.SendMessage(msg)
// 		if err != nil {
// 			fmt.Println("send message failed,", err)
// 			return
// 		}

// 		fmt.Printf("pid:%v offset:%v\n", pid, offset)
// 	}

// }
