package main

import (
	
	"fmt"
	"log"
	// "os"
	"time"
	"context"
	"database/sql"
	
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)
const (  
    username = "root"
    password = "root12345"
    hostname = "127.0.0.1:3306"
    dbname   = "realinfo"
)

var actualval string
func main() {
	
	db, err := dbConnection()
	if err!=nil{
		panic(err.Error())
	}
	defer db.Close()
    log.Printf("Successfully connected to database")
	c1, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "group1",
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
			actualval=string(acv.Value)
		//	insertMemberVal(db,actualval)
			
		}else {
			fmt.Printf("Consumer error: %v (%v)\n", err, acv)
		}
	}
defer c1.Close()
}
func dsn(dbName string) string {  
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {  
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
	log.Printf("Error %s when opening DB\n", err)
	return nil, err
		}
//defer db.Close()

ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
defer cancelfunc()

db.Close()

db, err = sql.Open("mysql", dsn(dbname))
if err != nil {
	log.Printf("Error %s when opening DB", err)
	return nil, err
}

db.SetMaxOpenConns(20)
db.SetMaxIdleConns(20)
db.SetConnMaxLifetime(time.Minute * 5)

ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
defer cancelfunc()
err = db.PingContext(ctx)
if err != nil {
	log.Printf("Errors %s pinging DB", err)
	return nil, err
}
log.Printf("Connected to DB %s successfully\n", dbname)
return db, nil
}

// func insertMemberVal(db *sql.DB, actualval string) error{
   
 //   stmt, err := db.Prepare("INSERT INTO apiinfo (Value) values (?);")
//	if err != nil {
//		fmt.Print(err.Error())
//	}
//	_, err = stmt.Exec(actualval)

//	if err != nil {
//		fmt.Print(err.Error())
//    }
//    return nil
//}

	
