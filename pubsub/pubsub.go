package pubsub

import (
	"fmt"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

// PubSubInput
type PubSubInput struct {
	CredentialsPath string
	ProjectID       string
}

// validate
func (in PubSubInput) validate() error {
	messages := []string{}

	if in.CredentialsPath == "" {
		messages = append(messages, "CredentialsPath is missing")
	}
	if in.ProjectID == "" {
		messages = append(messages, "ProjectID is missing")
	}

	if len(messages) > 0 {
		errs := ""
		for _, msg := range messages {
			if errs != "" {
				errs += " / "
			}
			errs += msg
		}
		return fmt.Errorf("validation error; %s", errs)
	}

	return nil
}

// Message
type Message struct {
	Data       []byte
	Attributes map[string]string
}

// ClientIF
type ClientIF interface {
	Publish(topic string, message *Message) (string, error)
	Subscribe(subscriptionName string, ch chan *Message) error
}

// Client implements ClientIF
type Client struct {
	client *pubsub.Client
	topics map[string]*pubsub.Topic
}

// New
func New(in PubSubInput) (ClientIF, error) {
	err := in.validate()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, in.ProjectID, option.WithCredentialsFile(in.CredentialsPath))
	if err != nil {
		return nil, fmt.Errorf("Could not create pubsub Client; error: %v", err)
	}

	return &Client{
		client: client,
	}, nil
}

// Publish
func (c *Client) Publish(topicName string, message *Message) (string, error) {
	topic := c.client.Topic(topicName)
	if topic == nil {
		return "", fmt.Errorf("Could not get topic; topic: %s", topicName)
	}
	ctx := context.Background()
	result := topic.Publish(ctx, &pubsub.Message{
		Data:       message.Data,
		Attributes: message.Attributes,
	})
	id, err := result.Get(ctx)
	if err != nil {
		return "", err
	}

	return id, nil
}

// Subscribe
func (c *Client) Subscribe(subscriptionName string, ch chan *Message) error {
	sub := c.client.Subscription(subscriptionName)
	ctx := context.Background()
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		ch <- &Message{
			Data:       msg.Data,
			Attributes: msg.Attributes,
		}
		msg.Ack()
	})
	if err != nil {
		return err
	}

	return nil
}
