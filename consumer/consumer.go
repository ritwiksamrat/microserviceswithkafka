package main

import (
	"fmt"
	"log"

	// "os"
	"context"
	"database/sql"
	"time"
	"github.com/ritwiksamrat/microserviceswithkafka/consumer1"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	username = "root"
	password = "root12345"
	hostname = "127.0.0.1:3306"
	dbname   = "realinfo"
)


var keyuname string
var orgval string
func main() {
	orgval=consumer1.actualval
	fmt.Println(orgval)
	db, err := dbConnection()
	if err != nil {
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
		uname, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", uname.TopicPartition, string(uname.Value))
			keyuname = string(uname.Value)

		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, uname)
		}

		currenttime := time.Now()
		cttime := string(currenttime.Format("15:04"))
		fmt.Println("Current Time is: ", cttime)
		var op int64
		fmt.Println("You can give the time difference in hrs or mins or secs")
		fmt.Println("1. Hours")
		fmt.Println("2. Minutes")
		fmt.Println("3. Seconds")
		fmt.Println("4. Press 4 if you don't want to give the difference")
		fmt.Scanln(&op)
		switch op {
		case 1:
			var hors int64
			fmt.Println("Enter the difference in hours")
			fmt.Scanln(&hors)
			hrsfn := hoursfunc(currenttime, hors)
			fmt.Println("EndTime is: ", hrsfn)
			err := inserttablehr(db, keyuname, cttime, hrsfn)
			if err != nil {
				panic(err.Error())
			}

			break
		case 2:
			var minus int64
			fmt.Println("Enter the difference in minutes")
			fmt.Scanln(&minus)
			minsfn := minutesfunc(currenttime, minus)
			fmt.Println("EndTime is: ", minsfn)
			err := inserttablemin(db, keyuname, cttime, minsfn)
			if err != nil {
				panic(err.Error())
			}
			break
		case 3:
			var secons int64
			fmt.Println("Enter the difference in seconds")
			fmt.Scanln(&secons)
			secsfn := secondsfunc(currenttime, secons)
			fmt.Println("EndTime is: ", secsfn)
			err := inserttablesec(db, keyuname, cttime, secsfn)
			if err != nil {
				panic(err.Error())
			}

			break
		default:
			fmt.Println("You didn't give any inputs!!")
			break
		}

	}


}

func hoursfunc(currenttime time.Time, hors int64) string {
	endtime := currenttime.Add(time.Hour*time.Duration(hors) +
		time.Minute*time.Duration(00) +
		time.Second*time.Duration(00))
	return string(endtime.Format("15:04"))
	//   fmt.Println(string(endtime.Format("15:04")))
}

func minutesfunc(currenttime time.Time, minus int64) string {
	endtime := currenttime.Add(time.Hour*time.Duration(00) +
		time.Minute*time.Duration(minus) +
		time.Second*time.Duration(00))
	return string(endtime.Format("15:04"))
	//   fmt.Println(string(endtime.Format("15:04")))

}

func secondsfunc(currenttime time.Time, secons int64) string {
	endtime := currenttime.Add(time.Hour*time.Duration(00) +
		time.Minute*time.Duration(00) +
		time.Second*time.Duration(secons))
	return string(endtime.Format("15:04"))
	//    fmt.Println(string(endtime.Format("15:04")))
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

func inserttablemin(db *sql.DB, keyuname string, ct string, et string) error {

	var count int64
	count = 0
	stmt, err := db.Prepare("INSERT INTO apiinfo (subject,CurrentTime,ENDTIME,COUNTER) values (?,?,?,?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(keyuname, ct, et, count)

	if err != nil {
		fmt.Print(err.Error())
	}
	fmt.Println("Data has been gone to the database")
	return nil

}

func inserttablehr(db *sql.DB, keyuname string, ct string, et string) error {

	var count int64
	count = 0
	stmt, err := db.Prepare("INSERT INTO apiinfo (subject,CurrentTime,ENDTIME,COUNTER) values (?,?,?,?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(keyuname, ct, et, count)

	if err != nil {
		fmt.Print(err.Error())
	}
	fmt.Println("Data has been gone to the database")
	return nil

}
func inserttablesec(db *sql.DB, keyuname string, ct string, et string) error {

	var count int64
	count = 0
	stmt, err := db.Prepare("INSERT INTO apiinfo (subject,CurrentTime,ENDTIME,COUNTER) values (?,?,?,?);")
	if err != nil {
		fmt.Print(err.Error())
	}
	_, err = stmt.Exec(keyuname, ct, et, count)

	if err != nil {
		fmt.Print(err.Error())
	}
	fmt.Println("Data has been gone to the database")
	return nil

}
