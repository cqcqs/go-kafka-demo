package main

import (
	"fmt"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
)

func main() {
	// 配置
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	/*config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = -2
	config.Consumer.Offsets.CommitInterval = 1 * time.Second*/
	config.Group.Return.Notifications = true

	// 创建消费者
	brokers := []string{"192.168.10.232:9092"}
	topics := []string{"topic1"}
	consumer, err := cluster.NewConsumer(brokers, "consumer-group", topics, config)
	if err != nil {
		fmt.Printf("new consumer error: %s\n", err.Error())
		return
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		for err := range consumer.Errors() {
			fmt.Printf("consumer error: %s", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Printf("consumer notification error: %v \n", ntf)
		}
	}()

	// 循环从通道中获取消息
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Printf("%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // 上报offset
			} else {
				fmt.Println("监听服务失败")
			}
		/*case err, ok := <-consumer.Errors():
			if ok {
				fmt.Printf("consumer error: %s", err.Error())
			}
		case ntf, ok := <-consumer.Notifications():
			if ok {
				fmt.Printf("consumer notification: %v \n", ntf)
			}*/
		case <-signals:
			return
		}
	}
}
