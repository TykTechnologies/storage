package redisv8

import (
	"context"

	"github.com/TykTechnologies/storage/temporal/model"
)

func (r *RedisV8) Publish(ctx context.Context, channel, message string) (int64, error) {
	return r.client.Publish(ctx, channel, message).Result()
}

// Subscribe initializes a subscription to one or more channels.
func (r *RedisV8) Subscribe(ctx context.Context, channels ...string) (model.Subscription, error) {
	sub := r.client.Subscribe(ctx, channels...)
	for range channels {
		// The first message is always a confirmation of the subscription.
		// We're ensuring the subscription is established before returning.
		// That way, the caller can be sure that the subscription is ready to receive messages before publishing.
		_, err := sub.Receive(ctx)
		if err != nil {
			return nil, err
		}
	}

	return sub, nil
}
