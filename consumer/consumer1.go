package main

import (
	
	"fmt"
	// "log"
	// "os"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	

	c1, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "smallest",
	})

	if err != nil {
		panic(err)
	}

	c1.SubscribeTopics([]string{"SampleTopic"}, nil)
	for {
		acv,err:= c1.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", acv.TopicPartition, string(acv.Value))
			
		}else {
			fmt.Printf("Consumer error: %v (%v)\n", err, acv)
		}
	}
defer c1.Close()
}


