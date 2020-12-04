package main

import (
	
	"fmt"
	// "log"
	// "os"
	
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "smallest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"myTopic"}, nil)
	for {
		uname,err:= c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", uname.TopicPartition, string(uname.Value))
			
		}else {
			fmt.Printf("Consumer error: %v (%v)\n", err, uname)
		}
	}



defer c.Close()
}


