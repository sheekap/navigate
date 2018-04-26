package main

import (
	"fmt"
        // "os"

	"github.com/Shopify/sarama"
        "github.com/golang/protobuf/proto"
)

var brokers = []string{"kafka.docker:9092"}

const topic string = "mobile_sdk.app_created"

func main() {
        app := SdkAppCreated{
                BrandId: proto.Int64Value(456),
                AccountId: proto.Int64Value(123),
                Identifier: proto.StringValue("ThisIsAnIdentifier"),
                Authentication: proto.StringValue("AuthMethod"),
        }

        msg, err := proto.Marshal(app)

        if err != nil {
                fmt.Println(err)
        }

	producer(msg)
}

func producer(msg string) {
	producer, err := sarama.NewAsyncProducer(brokers, nil)

	if err != nil {
		fmt.Println("PRODUCER: UH OH")
		fmt.Println(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println("PRODUCER: UH UH OH")
			fmt.Println(err)
		}
	}()

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(msg)}
}
