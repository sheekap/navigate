package main

import(
        "fmt"
        "github.com/Shopify/sarama"
)

var brokers = []string{"kafka.docker:9092"}
const topic string = "mobile_sdk.app_created"

func main() {
        consume()
}

func consume() {
        master, err := sarama.NewConsumer(brokers, nil)

        if err != nil {
                fmt.Println(err)
        }

        defer func() {
                if err := master.Close(); err != nil{
                        fmt.Println(err)
                }
        }()

        consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)

        if err != nil {
                fmt.Println(err)
        }

        consumerCh := make(chan string)

        go func(){
                for {
                        msg := <-consumer.Messages()
                        fmt.Println("Received Message: ", string(msg.Key), string(msg.Value))
                }
        }()

        <-consumerCh
}
