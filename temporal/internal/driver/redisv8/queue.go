package redisv8

import (
	"context"
	"fmt"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/go-redis/redis/v8"
)

// subscribeAdapter is an adapter for redis.PubSub to satisfy model.Subscription interface.
// Receive() method returns a model.Message instead of an interface{}.
type subscriptionAdapter struct {
	pubSub *redis.PubSub
}

// messageAdapter is an adapter for redis.Message to satisfy model.Message interface.
// Channel() and Payload() methods return the channel and payload of the message.
type messageAdapter struct {
	msg *redis.Message
}

func newSubscriptionAdapter(pubSub *redis.PubSub) *subscriptionAdapter {
	return &subscriptionAdapter{pubSub: pubSub}
}

func newMessageAdapter(msg *redis.Message) *messageAdapter {
	return &messageAdapter{msg: msg}
}

func (m *messageAdapter) Type() {

}

func (m *messageAdapter) Channel() string {
	return m.msg.Channel
}

func (m *messageAdapter) Payload() string {
	return m.msg.Payload
}

func (r *subscriptionAdapter) Receive(ctx context.Context) (model.Message, error) {
	msg, err := r.pubSub.Receive(ctx)
	if err != nil {
		return nil, err
	}

	switch m := msg.(type) {
	case *redis.Message:
		msg := newMessageAdapter(m)
		return msg, nil
	case *redis.Subscription, *redis.Pong:
		// TBD: should we return a message for these?
		return nil, nil
	case error:
		return nil, fmt.Errorf("redis subscription error: %w", m)
	default:
		return nil, fmt.Errorf("redis subscription error: unknown message type %T", m)
	}
}

func (r *subscriptionAdapter) Close() error {
	return r.pubSub.Close()
}

func (r *RedisV8) Publish(ctx context.Context, channel, message string) (int64, error) {
	return r.client.Publish(ctx, channel, message).Result()
}

// Subscribe initializes a subscription to one or more channels.
func (r *RedisV8) Subscribe(ctx context.Context, channels ...string) (model.Subscription, error) {
	sub := r.client.Subscribe(ctx, channels...)
	for range channels {
		// The first message is always a confirmation of the subscription.
		// We're ensuring the subscription is established before returning.
		// This way, the caller can be sure that the subscription is ready to receive messages before publishing.
		_, err := sub.Receive(ctx)
		if err != nil {
			return nil, err
		}
	}

	adapterSub := newSubscriptionAdapter(sub)
	return adapterSub, nil
}
