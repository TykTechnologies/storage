package local

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewMockBroker(t *testing.T) {
	broker := NewMockBroker()
	if broker == nil {
		t.Fatal("NewMockBroker() returned nil")
	}
	if broker.subscriptions == nil {
		t.Error("NewMockBroker() did not initialize subscriptions map")
	}
}

func TestMockBroker_Publish(t *testing.T) {
	broker := NewMockBroker()
	channel := "testChannel"
	message := "testMessage"

	// Test publishing to a channel with no subscribers
	count, err := broker.Publish(channel, message)
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}
	if count != 0 {
		t.Errorf("Publish() to empty channel returned count = %d, want 0", count)
	}

	// Add a subscriber
	sub := broker.Subscribe(channel)
	defer sub.Close()

	// Test publishing to a channel with a subscriber
	count, err = broker.Publish(channel, message)
	if err != nil {
		t.Errorf("Publish() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Publish() returned count = %d, want 1", count)
	}

	// Verify the message was received
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receivedMsg1, err := sub.Receive(ctx)
	if err != nil {
		t.Errorf("Receive() error = %v", err)
	}
	if receivedMsg1.Type() != "subscription" {
		t.Errorf("Received message type = %s, want 'subscription'", receivedMsg1.Type())
	}
	receivedChannel, _ := receivedMsg1.Channel()
	if receivedChannel != channel {
		t.Errorf("Received message channel = %s, want %s", receivedChannel, channel)
	}
	receivedPayload, _ := receivedMsg1.Payload()
	if receivedPayload != "subscribe" {
		t.Errorf("Received message payload = %s, want %s", receivedPayload, "subscribe")
	}

	receivedMsg2, err := sub.Receive(ctx)
	if err != nil {
		t.Errorf("Receive() error = %v", err)
	}
	if receivedMsg2.Type() != "message" {
		t.Errorf("Received message type = %s, want 'message'", receivedMsg2.Type())
	}
	receivedChannel2, _ := receivedMsg2.Channel()
	if receivedChannel2 != channel {
		t.Errorf("Received message channel = %s, want %s", receivedChannel2, channel)
	}
	receivedPayload2, _ := receivedMsg2.Payload()
	if receivedPayload2 != message {
		t.Errorf("Received message payload = %s, want %s", receivedPayload2, message)
	}
}

func TestMockBroker_Subscribe(t *testing.T) {
	broker := NewMockBroker()
	channels := []string{"channel1", "channel2"}

	sub := broker.Subscribe(channels...)

	// Verify subscription confirmation messages
	for _, channel := range channels {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := sub.Receive(ctx)
		cancel()
		if err != nil {
			t.Errorf("Receive() error = %v", err)
		}
		if msg.Type() != "subscription" {
			t.Errorf("Subscription message type = %s, want 'subscription'", msg.Type())
		}
		msgChannel, _ := msg.Channel()
		if msgChannel != channel {
			t.Errorf("Subscription message channel = %s, want %s", msgChannel, channel)
		}
		payload, _ := msg.Payload()
		if payload != "subscribe" {
			t.Errorf("Subscription message payload = %s, want 'subscribe'", payload)
		}
	}

	// Verify subscriptions were added
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	for _, channel := range channels {
		if len(broker.subscriptions[channel]) != 1 {
			t.Errorf("Channel %s has %d subscribers, want 1", channel, len(broker.subscriptions[channel]))
		}
	}
}

func TestMockSubscription_Close(t *testing.T) {
	broker := NewMockBroker()
	channel := "testChannel"
	sub := broker.Subscribe(channel)

	// Close the subscription
	err := sub.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify the subscription was removed
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	if len(broker.subscriptions[channel]) != 0 {
		t.Errorf("Channel %s has %d subscribers after close, want 0", channel, len(broker.subscriptions[channel]))
	}

	// Try to close again
	err = sub.Close()
	if err == nil {
		t.Error("Close() on already closed subscription should return an error")
	}
}

func TestMockMessage(t *testing.T) {
	tests := []struct {
		name        string
		messageType string
		channel     string
		payload     string
		wantErr     bool
	}{
		{"Valid message", "message", "testChannel", "testPayload", false},
		{"Valid subscription", "subscription", "testChannel", "subscribe", false},
		{"Invalid type", "invalid", "testChannel", "testPayload", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &MockMessage{
				messageType: tt.messageType,
				channel:     tt.channel,
				payload:     tt.payload,
			}

			if msg.Type() != tt.messageType {
				t.Errorf("Type() = %v, want %v", msg.Type(), tt.messageType)
			}

			channel, err := msg.Channel()
			if (err != nil) != tt.wantErr {
				t.Errorf("Channel() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && channel != tt.channel {
				t.Errorf("Channel() = %v, want %v", channel, tt.channel)
			}

			payload, err := msg.Payload()
			if (err != nil) != tt.wantErr {
				t.Errorf("Payload() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && payload != tt.payload {
				t.Errorf("Payload() = %v, want %v", payload, tt.payload)
			}
		})
	}
}

func TestMockBroker_Concurrency(t *testing.T) {
	broker := NewMockBroker()
	channels := []string{"channel1", "channel2", "channel3"}
	messageCount := 100
	subscriberCount := 5

	var wg sync.WaitGroup
	wg.Add(subscriberCount + 1) // +1 for the publisher

	// Start subscribers
	for i := 0; i < subscriberCount; i++ {
		go func() {
			defer wg.Done()
			sub := broker.Subscribe(channels...)
			defer sub.Close()

			// Receive subscription confirmations
			for range channels {
				sub.Receive(context.Background())
			}

			// Receive messages
			for j := 0; j < messageCount; j++ {
				_, err := sub.Receive(context.Background())
				if err != nil {
					t.Errorf("Receive() error = %v", err)
				}
			}
		}()
	}

	// Start publisher
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			for _, channel := range channels {
				_, err := broker.Publish(channel, "test message")
				if err != nil {
					t.Errorf("Publish() error = %v", err)
				}
			}
		}
	}()

	wg.Wait()
}
