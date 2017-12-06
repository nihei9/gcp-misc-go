package main

import (
	"fmt"
	"log"
	"test/gcp-pubsub/client/gcp/pubsub"
)

const (
	credentialsPath  = "********"
	projectID        = "********"
	topicName        = "********"
	subscriptionName = "********"
)

func main() {
	c, err := pubsub.New(pubsub.PubSubInput{
		CredentialsPath: credentialsPath,
		ProjectID:       projectID,
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.Publish(topicName, &pubsub.Message{
		Data:       []byte("Hello world."),
		Attributes: map[string]string{"Type": "text"},
	})
	if err != nil {
		log.Fatal(err)
	}

	err = c.Subscribe(subscriptionName, messageProcessor)
	if err != nil {
		log.Fatal(err)
	}
}

func messageProcessor(message *pubsub.Message) int {
	fmt.Printf("Got message; %s", string(message.Data))
	return 0
}
