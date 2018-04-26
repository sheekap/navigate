package main

import (
	"fmt"
	"time"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var brokers = []string{"kafka.docker:9092"}

const topic string = "sheeka.navigate_test"

func main() {
	message := "The time and date is: " + time.Now().String()
	producer(message)
	consume()
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

func consume() {
	master, err := sarama.NewConsumer(brokers, nil)

	if err != nil {
		fmt.Println("CONSUMER: UH OH")
		fmt.Println(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			fmt.Println("CONSUMER: UH UH OH")
			fmt.Println(err)
		}
	}()

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		fmt.Println("CONSUMER: UHHHHHH OOOOOOOOH")
		fmt.Println(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	msgCount := 0
	doneCh := make(chan struct{})

	go func() {
		for{
			select{
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

	fmt.Println("Processed, ", msgCount, "messages")
}
