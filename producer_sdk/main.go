package main

import (
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
        "github.com/golang/protobuf/proto"
)

var brokers = []string{"kafka.docker:9092"}

const topic string = "mobile_sdk.app_created"

func main() {
        app := SdkAppCreated{
                BrandId: 456,
                AccountId: 123,
                Identifier: "ThisIsAnIdentifier",
                Authentication: "AuthMethod",
        }

        msg, err := proto.Marshal(&app)

        if err != nil {
                fmt.Println(err)
        }

	producer(msg)
}

func producer(msg []byte) {
	sdkApp := SdkAppCreated{}

	if err := proto.Unmarshal(msg, &sdkApp); err != nil {
		fmt.Println(err)
	}

	var message string = "brandId: " + strconv.Itoa(int(sdkApp.BrandId)) + ", accountId: " + strconv.Itoa(int(sdkApp.AccountId)) + ", identifier: " + sdkApp.Identifier + ", authentication: " + sdkApp.Authentication

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

	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message)}
}
