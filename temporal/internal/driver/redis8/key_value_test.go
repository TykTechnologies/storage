package redis8

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// newTestRedis returns a Redis8 instance connected to a miniredis server.
func newTestRedis(t *testing.T) (*Redis8, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}

	return NewRedis8(mr.Addr(), "", 0), func() {
		mr.Close()
	}
}

func TestRedis8_Set(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		value      string
		expiration time.Duration
		wantErr    bool
	}{
		{"Set valid key-value", "key1", "value1", 10 * time.Second, false},
		{"Set with empty key", "", "value2", 10 * time.Second, true},
		{"Set with empty value", "key3", "", 10 * time.Second, false},
		{"Set with no expiration", "key4", "value4", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client, cleanup := newTestRedis(t)
			defer cleanup()

			err := client.Set(ctx, tt.key, tt.value, tt.expiration)
			if (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				gotValue, err := client.Client.Get(ctx, tt.key).Result()
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if gotValue != tt.value {
					t.Errorf("Get() = %v, want %v", gotValue, tt.value)
				}

				if tt.expiration > 0 {
					ttl, err := client.Client.TTL(ctx, tt.key).Result()
					if err != nil {
						t.Errorf("TTL() error = %v", err)
					}
					if ttl > tt.expiration || ttl <= 0 {
						t.Errorf("TTL() = %v, want less than or equal to %v", ttl, tt.expiration)
					}
				}
			}
		})
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *Redis8)
		key     string
		want    string
		wantErr bool
	}{
		{
			name:    "Get non-existing key",
			key:     "key1",
			want:    "",
			wantErr: false,
		},
		{
			name: "Get existing key",
			setup: func(rdb *Redis8) {
				err := rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "key2",
			want:    "value2",
			wantErr: false,
		},
		{
			name:    "Get key when server is closed",
			key:     "key3",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)

			if tt.wantErr {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			got, err := client.Get(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *Redis8)
		key     string
		wantErr bool
	}{
		{
			name: "Delete existing key",
			setup: func(rdb *Redis8) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "key1",
			wantErr: false,
		},
		{
			name:    "Delete non-existing key",
			key:     "key2",
			wantErr: false,
		},
		{
			name:    "Delete key when server is closed",
			key:     "key3",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)

			if tt.wantErr {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			err := client.Delete(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIncrement(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *Redis8)
		key     string
		want    int64
		wantErr bool
	}{
		{
			name: "Increment existing key",
			setup: func(rdb *Redis8) {
				err := rdb.Set(context.Background(), "counter", "5", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "counter",
			want:    6,
			wantErr: false,
		},
		{
			name:    "Increment non-existing key",
			key:     "counter",
			want:    1,
			wantErr: false,
		},
		{
			name:    "Increment key when server is closed",
			key:     "counter",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)

			if tt.wantErr {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			got, err := client.Increment(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Increment() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Increment() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecrement(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *Redis8)
		key     string
		want    int64
		wantErr bool
	}{
		{
			name: "Decrement existing key",
			setup: func(rdb *Redis8) {
				err := rdb.Set(context.Background(), "counter", "5", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "counter",
			want:    4,
			wantErr: false,
		},
		{
			name:    "Decrement non-existing key",
			key:     "counter",
			want:    -1,
			wantErr: false,
		},
		{
			name:    "Decrement key when server is closed",
			key:     "counter",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)

			if tt.wantErr {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			got, err := client.Decrement(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decrement() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Decrement() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExists(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *Redis8)
		key     string
		want    bool
		wantErr bool
	}{
		{
			name: "Check existing key",
			setup: func(rdb *Redis8) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "key1",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Check non-existing key",
			key:     "key2",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Check key when server is closed",
			key:     "key3",
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)

			if tt.wantErr {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			exists, err := client.Exists(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exists() error = %v, wantErr %v", err, tt.wantErr)
			}
			if exists != tt.want {
				t.Errorf("Exists() got = %v, want %v", exists, tt.want)
			}
		})
	}
}
