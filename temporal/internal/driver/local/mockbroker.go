package local

import (
	"context"
	"errors"
	"sync"

	"github.com/TykTechnologies/storage/temporal/model"
)

// MockBroker is a mock implementation of the Broker interface
type MockBroker struct {
	subscriptions map[string][]chan model.Message
	mu            sync.RWMutex
}

// NewMockBroker creates a new MockBroker
func NewMockBroker() *MockBroker {
	return &MockBroker{
		subscriptions: make(map[string][]chan model.Message),
	}
}

// Publish sends a message to all subscribers of the specified channel
func (mb *MockBroker) Publish(channel, message string) (int64, error) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	subscribers, ok := mb.subscriptions[channel]
	if !ok {
		return 0, nil
	}

	msg := &MockMessage{
		messageType: "message",
		channel:     channel,
		payload:     message,
	}

	for _, ch := range subscribers {
		select {
		case ch <- msg:
		default:
			// If the channel is full, we skip this subscriber
		}
	}

	return int64(len(subscribers)), nil
}

// Subscribe creates a new subscription for the specified channels
func (mb *MockBroker) Subscribe(channels ...string) model.Subscription {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	msgChan := make(chan model.Message, 100)
	sub := &MockSubscription{
		broker:   mb,
		channels: channels,
		msgChan:  msgChan,
	}

	for _, channel := range channels {
		mb.subscriptions[channel] = append(mb.subscriptions[channel], msgChan)
		// Send subscription confirmation message
		msgChan <- &MockMessage{
			messageType: "subscription",
			channel:     channel,
			payload:     "subscribe",
		}
	}

	return sub
}

// MockSubscription is a mock implementation of the Subscription interface
type MockSubscription struct {
	broker   *MockBroker
	channels []string
	msgChan  chan model.Message
	closed   bool
	mu       sync.Mutex
}

// Receive waits for and returns the next message from the subscription
func (ms *MockSubscription) Receive(ctx context.Context) (model.Message, error) {
	select {
	case msg := <-ms.msgChan:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close closes the subscription and cleans up resources
func (ms *MockSubscription) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return errors.New("subscription already closed")
	}

	ms.closed = true

	ms.broker.mu.Lock()
	defer ms.broker.mu.Unlock()

	for _, channel := range ms.channels {
		subscribers := ms.broker.subscriptions[channel]
		for i, ch := range subscribers {
			if ch == ms.msgChan {
				ms.broker.subscriptions[channel] = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
	}

	return nil
}

// MockMessage is a mock implementation of the Message interface
type MockMessage struct {
	messageType string
	channel     string
	payload     string
}

// Type returns the message type
func (mm *MockMessage) Type() string {
	return mm.messageType
}

// Channel returns the channel the message was received on
func (mm *MockMessage) Channel() (string, error) {
	if mm.messageType == "message" || mm.messageType == "subscription" {
		return mm.channel, nil
	}
	return "", errors.New("invalid message type")
}

// Payload returns the message payload
func (mm *MockMessage) Payload() (string, error) {
	if mm.messageType == "message" || mm.messageType == "subscription" {
		return mm.payload, nil
	}
	return "", errors.New("invalid message type")
}
