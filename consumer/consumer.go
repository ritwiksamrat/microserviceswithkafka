package main

import (
	
	"fmt"
	// "log"
	// "os"
	"time"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)
const (  
    username = "root"
    password = "root12345"
    hostname = "127.0.0.1:3306"
    dbname   = "realinfo"
)
func main() {
	
	db, err := dbConnection()
	if err!=nil{
		panic(err.Error())
		return
	}
	defer db.Close()
	fmt.Println("DataBase is Successfully Connected")
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

		currenttime:=time.Now()
		fmt.Println("Current Time is: ",string(currenttime.Format("15:04")))
		var op int64
		fmt.Println("You can give the time difference in hrs or mins or secs")
		fmt.Println("1. Hours")
		fmt.Println("2. Minutes")
		fmt.Println("3. Seconds")
		fmt.Println("4. Press 4 if you don't want to give the difference")
		fmt.Scanln(&op)
		switch op{
		        case 1:
						var hors int64
						fmt.Println("Enter the difference in hours")
						fmt.Scanln(&hors)
						hoursfunc(currenttime,hors)
						break
				case 2: 
						var minus int64
						fmt.Println("Enter the difference in minutes")
						fmt.Scanln(&minus)
						minutesfunc(currenttime,minus)
						break
				case 3: 
						var secons int64
						fmt.Println("Enter the difference in seconds")
						fmt.Scanln(&secons)
						secondsfunc(currenttime,secons)
						break
				default: 
						fmt.Println("You didn't give any inputs!!")
						break
			}

		}
	defer c.Close()
}

	func hoursfunc(currenttime time.Time,hors int64){
		endtime := currenttime.Add(time.Hour*time.Duration(hors) +
		time.Minute*time.Duration(00) +
		time.Second*time.Duration(00))
	    fmt.Println(string(endtime.Format("15:04")))
	}

	func minutesfunc(currenttime time.Time,minus int64){
		endtime := currenttime.Add(time.Hour*time.Duration(00) +
		time.Minute*time.Duration(minus) +
		time.Second*time.Duration(00))
	    fmt.Println(string(endtime.Format("15:04")))

	}

	func secondsfunc(currenttime time.Time,secons int64){
		endtime := currenttime.Add(time.Hour*time.Duration(00) +
		time.Minute*time.Duration(00) +
		time.Second*time.Duration(secons))
	    fmt.Println(string(endtime.Format("15:04")))
	}
