package local

import (
	"context"

	"github.com/TykTechnologies/storage/temporal/model"
)

// ===== QUEUE =====

// Publish sends a message to the specified channel.
// It returns the number of clients that received the message.
func (api *API) Publish(ctx context.Context, channel, message string) (int64, error) {
	// We're ignoring the context here as the Broker interface doesn't use it.
	// In a real implementation, you might want to respect context cancellation.
	err := api.Connector.Ping(ctx)
	if err != nil {
		return 0, err
	}

	return api.Broker.Publish(channel, message)
}

// Subscribe initializes a subscription to one or more channels.
// It returns a Subscription interface that allows receiving messages and closing the subscription.
func (api *API) Subscribe(ctx context.Context, channels ...string) model.Subscription {
	// We're ignoring the context here as the Broker interface doesn't use it.
	// In a real implementation, you might want to respect context cancellation.
	err := api.Connector.Ping(ctx)
	if err != nil {
		return nil
	}
	return api.Broker.Subscribe(channels...)
}
