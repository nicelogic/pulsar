package pulsarclient

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/nicelogic/config"
)
type Client struct {
	Client   pulsar.Client
}
type clientConfig struct {
	Url                string
	Operation_timeout  int
	Connection_timeout int
}

func (client *Client) Init(configFilePath string) (err error) {
	clientConfig := clientConfig{}
	err = config.Init(configFilePath, &clientConfig)
	if err != nil {
		log.Println("config init err: ", err)
		return err
	}
	log.Printf("%#v\n", clientConfig)

	client.Client, err = pulsar.NewClient(pulsar.ClientOptions{
		URL: clientConfig.Url,

		OperationTimeout:  time.Duration(clientConfig.Operation_timeout) * time.Second,
		ConnectionTimeout: time.Duration(clientConfig.Connection_timeout) * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	return err
}

func (client *Client) Receive(ctx context.Context, consumer pulsar.Consumer, msg any)(err error){
	pulsarMsg, err := consumer.Receive(context.Background())
	if err != nil {
		log.Printf("consumer receive err: %v\n", err)
		return err
	}
	consumer.Ack(pulsarMsg)
	payload := pulsarMsg.Payload()
	log.Printf("Received message msgId: %#v -- content: '%s'\n",
		pulsarMsg.ID(), string(payload))
	err = json.Unmarshal(payload, msg)
	if err != nil{
		log.Printf("json unmarshal err: %v\n", err)
		return err
	}
	return err
}
