package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

var address = []string{"192.168.10.232:9092"}

func main() {
	syncProducer()
	asyncProducer()
}

// 同步消息
func syncProducer() {
	// 配置
	config := sarama.NewConfig()
	// 设置属性
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	producer, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Printf("new sync producer error: %s \n", err.Error())
		return
	}
	// 关闭生产者
	defer producer.Close()
	// 循环发送消息
	for i := 0; i < 10; i++ {
		// 创建消息
		value := fmt.Sprintf("sync message, index = %d", i)
		msg := &sarama.ProducerMessage{
			Topic: "topic1",                  // 主题名称
			Value: sarama.ByteEncoder(value), // 消息内容
		}
		// 发送消息
		part, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("send message error: %s \n", err.Error())
		} else {
			fmt.Printf("SUCCESS: value=%s, partition=%d, offset=%d \n", value, part, offset)
		}
		// 每隔两秒发送一条消息
		time.Sleep(2 * time.Second)
	}
}

// 异步消息
func asyncProducer() {
	// 配置
	config := sarama.NewConfig()
	// 等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应，只有上面的RequireAcks设置不是NoReponse这里才有用
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	// 设置使用的kafka版本，如果低于V0_10_0_0版本,消息中的timestrap没有作用，需要消费和生产同时配置
	// 注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息
	config.Version = sarama.V0_10_0_1

	fmt.Println("start make producer")
	//使用配置，新建一个异步生产者
	producer, err := sarama.NewAsyncProducer(address, config)
	if err != nil {
		log.Printf("new async producer error: %s \n", err.Error())
		return
	}
	defer producer.AsyncClose()

	// 循环判断哪个通道发送过来数据
	fmt.Println("start goroutine")
	go func(p sarama.AsyncProducer) {
		for {
			select {
			case suc := <-p.Successes():
				fmt.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
			case fail := <-p.Errors():
				fmt.Println("error: ", fail.Error())
			}
		}
	}(producer)

	var value string
	for i := 0; ; i++ {
		// 每隔两秒发送一条消息
		time.Sleep(2 * time.Second)

		// 创建消息
		value = fmt.Sprintf("async message, index = %d", i)
		// 注意：这里的msg必须得是新构建的变量，不然你会发现发送过去的消息内容都是一样的，因为批次发送消息的关系
		msg := &sarama.ProducerMessage{
			Topic: "topic1",
			Value: sarama.ByteEncoder(value),
		}

		// 使用通道发送
		producer.Input() <- msg
	}
}
