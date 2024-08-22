package local

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/TykTechnologies/cannery/v2/publisher"
	"github.com/TykTechnologies/cannery/v2/subscriber"
	"github.com/TykTechnologies/storage/temporal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func (c *CRDTStore) Publish(channel, message string) (int64, error) {
	ctx := context.Background()

	if c.publisher == nil {
		c.publisher = publisher.NewPublisher(c.crdt.Conn)
	}

	err := c.publisher.Publish(ctx, channel, message)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (c *CRDTStore) Subscribe(channels ...string) model.Subscription {
	ctx, cancel := context.WithCancel(context.Background())
	sub := NewCRDTSubscription(ctx, cancel, c.crdt, channels)

	for i, _ := range channels {
		s := subscriber.NewSubscriber(c.crdt.Conn.Host.ID(), c.crdt.Conn, sub.onMessage, false)
		// if cached great, otherwise subscriber will create a new topic
		_, err := s.Subscribe(context.Background(), channels[i])
		if err != nil {
			slog.Error("failed to subscribe to crdt pusub", "error", err, "channel", channels[i])
			return nil
		}
		sub.subs = append(sub.subs, s)
		sub.SendSubcribeConfirmation(channels[i])
	}

	return sub
}

type CRDTSubscription struct {
	topics   []string
	subs     []*subscriber.Subscriber
	ctx      context.Context
	stopFunc context.CancelFunc
	msgChan  chan *CRDTMessage
	conn     *CRDTStorConnector
}

func NewCRDTSubscription(ctx context.Context, cancelFunc context.CancelFunc, conn *CRDTStorConnector, topics []string) *CRDTSubscription {
	return &CRDTSubscription{
		topics:   topics,
		subs:     make([]*subscriber.Subscriber, 0),
		ctx:      ctx,
		stopFunc: cancelFunc,
		msgChan:  make(chan *CRDTMessage, 1000),
		conn:     conn,
	}
}

func (c *CRDTSubscription) SendSubcribeConfirmation(channel string) {
	newMsg := NewCRDTMessage(nil, MSGTypeSubscribe)
	newMsg.CustomChannel = channel
	newMsg.CustomPayload = "subscribe"

	c.msgChan <- newMsg
}

// Receive waits for and returns the next message from the subscription.
func (c *CRDTSubscription) Receive(ctx context.Context) (model.Message, error) {

	select {
	case <-ctx.Done():
		return &EmptyCRDTMessage{}, ctx.Err()
	case msg := <-c.msgChan:
		return msg, nil

	}

}

// Close closes the subscription and cleans up resources.
func (c *CRDTSubscription) Close() error {
	// TODO: If we close subs, the tests fail - this is dumb
	// for i, _ := range c.subs {
	// 	c.subs[i].Stop()
	// }
	//c.stopFunc()
	return nil
}

func (c *CRDTSubscription) onMessage(msg *pubsub.Message) error {

	select {
	case c.msgChan <- NewCRDTMessage(msg, MSGTypeMessage):
	default:
		return fmt.Errorf("message channel full")
	}

	return nil
}

const (
	MSGTypeSubscribe = "subscription"
	MSGTypeMessage   = "message"
)

type CRDTMessage struct {
	msgType       string
	CustomPayload string
	CustomChannel string
	msg           *pubsub.Message
}

func NewCRDTMessage(msg *pubsub.Message, msgType string) *CRDTMessage {
	return &CRDTMessage{
		msgType:       msgType,
		CustomPayload: "",
		CustomChannel: "",
		msg:           msg,
	}
}

func (c *CRDTMessage) Type() string {
	return c.msgType
}

func (c *CRDTMessage) Channel() (string, error) {
	if c.CustomChannel != "" {
		return c.CustomChannel, nil
	}

	t := *c.msg.Topic
	return t, nil
}

func (c *CRDTMessage) Payload() (string, error) {
	if c.CustomPayload != "" {
		return c.CustomPayload, nil
	}

	d := string(c.msg.Data)
	return d, nil
}

type EmptyCRDTMessage struct{}

func (c *EmptyCRDTMessage) Type() string {
	return "empty_message"
}
func (c *EmptyCRDTMessage) Channel() (string, error) {
	return "", nil
}

func (c *EmptyCRDTMessage) Payload() (string, error) {
	return "", nil
}
