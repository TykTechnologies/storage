package redisv8

import (
	"context"

	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/go-redis/redis/v8"
)

// subscribeAdapter is an adapter for redis.PubSub to satisfy model.Subscription interface.
// Receive() method returns a model.Message instead of an interface{}.
type subscriptionAdapter struct {
	pubSub *redis.PubSub
}

// messageAdapter is an adapter to satisfy model.Message interface.
// Channel() and Payload() methods return the channel and payload of the message.
// Type() method returns the type of the message.
type messageAdapter struct {
	msg interface{}
}

func newSubscriptionAdapter(pubSub *redis.PubSub) *subscriptionAdapter {
	return &subscriptionAdapter{pubSub: pubSub}
}

func newMessageAdapter(msg interface{}) *messageAdapter {
	return &messageAdapter{msg: msg}
}

func (m *messageAdapter) Type() string {
	switch m.msg.(type) {
	case *redis.Message:
		return model.MessageTypeMessage
	case *redis.Subscription:
		return model.MessageTypeSubscription
	case *redis.Pong:
		return model.MessageTypePong
	case error:
		return model.MessageTypeError
	default:
		return model.ErrUnknownMessageType.Error()
	}
}

func (m *messageAdapter) Channel() (string, error) {
	switch msg := m.msg.(type) {
	case *redis.Message:
		return msg.Channel, nil
	case *redis.Subscription:
		return msg.Channel, nil
	case *redis.Pong:
		return "", nil
	case error:
		return "", msg
	default:
		return "", model.ErrUnknownMessageType
	}
}

func (m *messageAdapter) Payload() (string, error) {
	switch msg := m.msg.(type) {
	case *redis.Message:
		return msg.Payload, nil
	case *redis.Subscription:
		return msg.Kind, nil
	case *redis.Pong:
		return msg.Payload, nil
	case error:
		return "", msg
	default:
		return "", model.ErrUnknownMessageType
	}
}

func (r *subscriptionAdapter) Receive(ctx context.Context) (model.Message, error) {
	msg, err := r.pubSub.Receive(ctx)
	if err != nil {
		return nil, err
	}

	return newMessageAdapter(msg), nil
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

	adapterSub := newSubscriptionAdapter(sub)

	return adapterSub, nil
}
