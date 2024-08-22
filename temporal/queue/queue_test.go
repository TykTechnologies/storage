package queue

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/stretchr/testify/assert"
)

func TestQueue_Publish(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	testCases := []struct {
		name        string
		channel     string
		message     string
		expectedErr error
		wantResult  int64
		setup       func(q model.Queue) ([]model.Subscription, error)
	}{
		{
			name:       "Publish to a channel",
			channel:    "test_channel1",
			message:    "Hello, World!",
			wantResult: 1,
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "test_channel1")
				_, err := sub1.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:       "Publish to a channel without subscribers",
			channel:    "non_subscribers_channel",
			message:    "Hello, World!",
			wantResult: 0,
		},
		{
			name:       "Publish with empty message",
			channel:    "test_channel2",
			message:    "",
			wantResult: 1,
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "test_channel2")

				_, err := sub1.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:       "Publish to multiple subscribers",
			channel:    "multi_subscriber_channel",
			message:    "Multi-subscriber message",
			wantResult: 2, // Assuming 2 subscribers for this test
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "multi_subscriber_channel")

				sub2 := q.Subscribe(context.Background(), "multi_subscriber_channel")

				_, err := sub1.Receive(context.Background())
				if err != nil {
					return nil, err
				}
				_, err = sub2.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:       "Publish with long message",
			channel:    "test_channel3",
			message:    strings.Repeat("long_message_", 1000), // Adjust length as needed
			wantResult: 1,
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "test_channel3")

				_, err := sub1.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:       "Publish with special characters in message",
			channel:    "test_channel4",
			message:    "Special!@#$%^&*()_+",
			wantResult: 1,
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "test_channel4")

				_, err := sub1.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:       "Publish to channel with special characters",
			channel:    "special_@#$%^_channel",
			message:    "Hello, World!",
			wantResult: 1,
			setup: func(q model.Queue) ([]model.Subscription, error) {
				sub1 := q.Subscribe(context.Background(), "special_@#$%^_channel")

				_, err := sub1.Receive(context.Background())
				return []model.Subscription{sub1}, err
			},
		},
		{
			name:        "Publish with connection failure",
			channel:     "test_channel5",
			message:     "Message with connection failure",
			expectedErr: temperr.ClosedConnection,
			wantResult:  0,
		},
	}

	ctx := context.Background()

	for _, connector := range connectors {
		queue, err := NewQueue(connector)
		assert.Nil(t, err)

		for _, tc := range testCases {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				if connector.Type() == model.CRDTType {
					if tc.wantResult > 1 {
						t.Skip("CRDT does not have knowledge of subscriber count")
					}

					if tc.expectedErr == temperr.ClosedConnection {
						t.Skip("CRDT does not mimic connection failure")
					}

					if tc.name == "Publish to a channel without subscribers" {
						t.Skip("CRDT does not have knowledge of subscriber count")
					}
				}

				if tc.expectedErr != nil {
					err = connector.Disconnect(context.Background())
					assert.Nil(t, err)
				}

				if tc.setup != nil {
					subs, err := tc.setup(queue)
					assert.Nil(t, err)
					for _, sub := range subs {
						defer sub.Close()
					}
				}
				result, err := queue.Publish(ctx, tc.channel, tc.message)

				if tc.expectedErr != nil {
					assert.NotNil(t, err)
					assert.Equal(t, tc.expectedErr, err)
					return
				}

				assert.Nil(t, err)
				assert.Equal(t, tc.wantResult, result)
			})
		}
	}
}

func TestQueue_Subscribe(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	testCases := []struct {
		name        string
		channels    []string
		expectedErr func(err error) bool
		setup       func(q model.Queue, channels []string, msg string) error
		expectedMsg string
	}{
		{
			name:     "Subscribe to a single channel",
			channels: []string{"test_channel1"},
			setup: func(q model.Queue, channels []string, msg string) error {
				for _, channel := range channels {
					_, err := q.Publish(context.Background(), channel, msg)
					return err
				}

				return nil
			},
			expectedMsg: "test",
		},
		{
			name:     "Subscribe to multiple channels",
			channels: []string{"test_channel3", "test_channel4"},
			setup: func(q model.Queue, channels []string, msg string) error {
				for _, channel := range channels {
					_, err := q.Publish(context.Background(), channel, msg)
					if err != nil {
						return err
					}
				}

				return nil
			},
			expectedMsg: "test",
		},
		{
			name:     "Subscribe to a non-existent channel",
			channels: []string{"non_existent_channel"},
			expectedErr: func(err error) bool {
				var netErr net.Error
				return errors.As(err, &netErr) && netErr.Timeout()
			},
			setup:       nil,
			expectedMsg: "",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	for _, connector := range connectors {
		queue, err := NewQueue(connector)
		assert.Nil(t, err)

		for _, tc := range testCases {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				sub := queue.Subscribe(ctx, tc.channels...)
				assert.NotNil(t, sub)
				defer sub.Close()

				for _, ch := range tc.channels {
					msg, err := sub.Receive(ctx)

					assert.Nil(t, err)
					actualChannel, err := msg.Channel()
					assert.Nil(t, err)
					assert.Equal(t, ch, actualChannel)

					actualPayload, err := msg.Payload()
					assert.Nil(t, err)
					assert.Equal(t, "subscribe", actualPayload)

					actualType := msg.Type()
					assert.Equal(t, model.MessageTypeSubscription, actualType)
				}

				if tc.setup != nil {
					err = tc.setup(queue, tc.channels, tc.expectedMsg)
					assert.Nil(t, err)

					for _, ch := range tc.channels {
						msg, err := sub.Receive(ctx)
						if tc.expectedErr != nil {
							assert.True(t, tc.expectedErr(err))
							return
						}

						assert.Nil(t, err)
						actualChannel, err := msg.Channel()
						assert.Nil(t, err)
						assert.Equal(t, ch, actualChannel)

						actualPayload, err := msg.Payload()
						assert.Nil(t, err)
						assert.Equal(t, tc.expectedMsg, actualPayload)

						actualType := msg.Type()
						assert.Equal(t, model.MessageTypeMessage, actualType)
					}
				}
			})
		}
	}
}

func TestQueue_NewQueue(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	for _, connector := range connectors {
		t.Run(connector.Type(), func(t *testing.T) {
			queue, err := NewQueue(connector)
			assert.Nil(t, err)
			assert.NotNil(t, queue)
		})
	}

	_, err := NewQueue(&testutil.StubConnector{})
	assert.NotNil(t, err)
}

func TestQueue_Ctx(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	for _, connector := range connectors {
		t.Run(connector.Type(), func(t *testing.T) {
			queue, err := NewQueue(connector)
			assert.Nil(t, err)
			assert.NotNil(t, queue)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

			didReceive := make(chan bool, 1)
			go func(context.Context) {
				defer func() {
					close(didReceive)
				}()

				sub := queue.Subscribe(ctx, "test_channel")
				defer sub.Close()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						msg, err := sub.Receive(ctx)
						if msg.Type() == model.MessageTypeSubscription {
							continue
						} else {
							assert.NotNil(t, err)
							didReceive <- true
							return
						}
					}
				}
			}(ctx)

			_, err = queue.Publish(context.Background(), "test_channel", "test")
			assert.Nil(t, err)

			// cancel the context now that the goroutine is running
			cancel()

			// wait for the goroutine to exit
			<-didReceive
		})
	}
}
