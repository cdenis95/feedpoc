package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	kafka "github.com/segmentio/kafka-go"
)

var kafkaHost = "localhost:9092"
var kafkaTopic = "test"

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", writeHandler).Methods("POST")

	go readHandler()

	fmt.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", r))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	var key string
	if val, ok := r.URL.Query()["key"]; ok {
		key = val[0]
	} else {
		w.Write(respond(map[string]string{
			"status":  "not ok",
			"message": "parameter `key` is missing",
		}))
		return
	}

	var value string
	if val, ok := r.URL.Query()["value"]; ok {
		value = val[0]
	} else {
		w.Write(respond(map[string]string{
			"status":  "not ok",
			"message": "parameter `value` is missing",
		}))
		return
	}

	k := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaHost},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})

	defer k.Close()

	k.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		},
	)

	w.Write(respond(map[string]string{
		"status": "ok",
		"key":    key,
		"value":  value,
	}))
}

func readHandler() {
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
	}
}

func respond(o map[string]string) []byte {
	json, err := json.Marshal(o)
	if err != nil {
		return []byte("{\"status\":\"not ok\",\"message\":\"unable to marshal json\"}")
	}

	return []byte(json)
}
