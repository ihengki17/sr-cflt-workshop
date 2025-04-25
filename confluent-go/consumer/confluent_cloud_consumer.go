/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This is a simple example demonstrating how to produce a message to
// a topic, and then reading it back again using a consumer. The topic
// belongs to a Apache Kafka cluster from Confluent Cloud. For more
// information about Confluent Cloud, please visit:
//
// https://www.confluent.io/confluent-cloud/

package main

import (
	"fmt"
	"os"
	"time"
	"log"

	"github.com/joho/godotenv"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

// In order to set the constants below, you are going to need
// to log in into your Confluent Cloud account. If you choose
// to do this via the Confluent Cloud CLI, follow these steps.

// 1) Log into Confluent Cloud:
//    $ ccloud login
//
// 2) List the environments from your account:
//    $ ccloud environment list
//
// 3) From the list displayed, select one environment:
//    $ ccloud environment use <ENVIRONMENT_ID>
//
// To retrieve the information about the bootstrap servers,
// you need to execute the following commands:
//
// 1) List the Apache Kafka clusters from the environment:
//    $ ccloud kafka cluster list
//
// 2) From the list displayed, describe your cluster:
//    $ ccloud kafka cluster describe <CLUSTER_ID>
//
// Finally, to create a new API key to be used in this program,
// you need to execute the following command:
//
// 1) Create a new API key in Confluent Cloud:
//    $ ccloud api-key create

func main() {

	path:="../local.env"
	err := godotenv.Load(path)
    if err != nil {
        log.Fatal("Error loading .env file")
    }

    bootstrapServers:= os.Getenv("BOOTSTRAP_SERVER")
	ccloudAPIKey := os.Getenv("CC_API_KEY")
	ccloudAPISecret := os.Getenv("CC_API_SECRET")
	schemaRegistryAPIEndpoint := os.Getenv("SR_ENDPOINT")
	schemaRegistryAPIKey      := os.Getenv("SR_API_KEY")
	schemaRegistryAPISecret   := os.Getenv("SR_API_SECRET")

	topic := "go-test-topic2"

	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithBasicAuthentication(
		schemaRegistryAPIEndpoint,
		schemaRegistryAPIKey,
		schemaRegistryAPISecret))

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	deser, err := avro.NewGenericDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("Failed to create deserializer: %s\n", err)
		os.Exit(1)
	}


	// Now consumes the record and print its value...
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"sasl.mechanisms":    "PLAIN",
		"security.protocol":  "SASL_SSL",
		"sasl.username":      ccloudAPIKey,
		"sasl.password":      ccloudAPISecret,
		"session.timeout.ms": 6000,
		"group.id":           "my-group",
		"auto.offset.reset":  "earliest"})

	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %s", err))
	}
	defer consumer.Close()

	topics := []string{topic}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topics: %s", err))
	}

	for {
		message, err := consumer.ReadMessage(100 * time.Millisecond)
		if err == nil {
			received := User{}
			err := deser.DeserializeInto(*message.TopicPartition.Topic, message.Value, &received)
			if err != nil {
				fmt.Printf("Failed to deserialize payload: %s\n", err)
			} else {
				fmt.Printf("consumed from topic %s [%d] at offset %v: %+v",
					*message.TopicPartition.Topic,
					message.TopicPartition.Partition, message.TopicPartition.Offset,
					received)
			}
		}
	}

}


// User is a simple record example
type User struct {
	Name           string `json:"name"`
	PhoneNumber string  `json:"phone_number"`
	CustomerID  string `json:"customer_id"`
	Times int64 `json:"times"`
}
