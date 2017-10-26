package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	kafka "github.com/segmentio/kafka-go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"crypto/md5"
	"encoding/hex"
	"github.com/zalora/kafkapoc/model"
	"errors"
)

const (
	kafkaHost = "localhost:9092"
	kafkaTopic = "test"

	mongoDbHost = "localhost"
	mongoDbPort = "27017"
)

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", writeHandler).Methods("POST")
	r.HandleFunc("/", readHandler).Methods("GET")

	go materialize()

	fmt.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", r))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	// Reading the request body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.Write(respond(map[string]string{
			"status":  "not ok",
			"message": "Cannot read request body",
		}))
		return
	}

	// Processing


	// Write to Kafka
	key := md5.Sum(body)

	k := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaHost},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})

	defer k.Close()

	err = k.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   key[:],
			Value: body,
		},
	)
	if err != nil {
		w.Write(respond(map[string]string{
			"status":  "not ok",
			"message": err.Error(),
		}))
		return
	}

	w.Write(respond(map[string]string{
		"status": "ok",
		"key":    hex.EncodeToString(key[:]),
		"value":  string(body),
	}))
}

func materialize() {
	k := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaHost},
		Topic:     kafkaTopic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	defer k.Close()

	for {
		m, err := k.ReadMessage(context.Background())
		if err != nil {
			break
		}

		fmt.Printf(
			"[CONSUMER] message at offset %d: %s = %s\n",
			m.Offset,
			string(m.Key),
			string(m.Value),
		)

		l := &model.Log{}
		err = json.Unmarshal(m.Value, l)
		if err != nil {
			// TODO : How do we handle malformed logs in the consumer?
			fmt.Println("[CONSUMER] Cannot unmarshal log")
			continue
		}

		err = processLog(l)
		if err != nil {
			fmt.Println("[CONSUMER] Cannot unmarshal log", err.Error())
			continue
		}

	}
}

func processLog(l *model.Log) error {
	switch l.Action {
	case "POST_CREATE":
		return processPostCreationLog(l)
	default:
		return errors.New("Invalid log action: " + l.Action)
	}
}

func processPostCreationLog(l *model.Log) error {
	session, err := mgo.Dial(fmt.Sprintf("%s:%s", mongoDbHost, mongoDbPort))
	if err != nil {
		return err
	}
	defer session.Close()

	data := &model.PostCreationLogData{}
	err = json.Unmarshal(l.Data, data)

	data.Post.Type = data.Type

	// Post will be created based on the log data. For now, just insert the post data in the DB

	c := session.DB("feed").C("posts")
	err = c.Insert(data.Post)
	if err != nil {
		return err
	}
	return nil
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	session, err := mgo.Dial(fmt.Sprintf("%s:%s", mongoDbHost, mongoDbPort))
	if err != nil {
		w.Write(respond(map[string]string{
			"status":  "not ok",
			"message": "Cannot connect to DB",
		}))
		return
	}
	defer session.Close()

	// Get the posts collection
	c := session.DB("feed").C("posts")

	posts := []*model.Post{}
	c.Find(bson.M{}).All(&posts)

	w.Write(respond(map[string]interface{}{
		"status":  "ok",
		"message": posts,
	}))
}

func respond(o interface{}) []byte {
	json, err := json.Marshal(o)
	if err != nil {
		return []byte("{\"status\":\"not ok\",\"message\":\"unable to marshal json\"}")
	}

	return []byte(json)
}
