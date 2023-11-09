//go:build redis
// +build redis

package redisv9

import (
	"context"
	"testing"
	"time"

	"github.com/TykTechnologies/storage/temporal/internal/types"
	"github.com/TykTechnologies/storage/temporal/utils"
)

func newTestRedis(t *testing.T) (*RedisV9, func()) {
	t.Helper()

	opts := &types.ClientOpts{
		Redis: &types.RedisOptions{
			Addrs: []string{"localhost:6379"},
		},
	}

	r8 := NewRedisV9(opts)

	ctx := context.Background()

	_, err := r8.client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("an error '%v' occurred when connecting to Redis server", err)
	}

	return r8, func() {
		_, err = r8.client.FlushDB(ctx).Result()
		if err != nil {
			t.Fatalf("an error '%v' occurred when flushing the database", err)
		}

		err = r8.client.Close()
		if err != nil {
			t.Fatalf("an error '%v' occurred when closing the connection", err)
		}
	}
}

func TestRedisV9_Set(t *testing.T) {
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
				gotValue, err := client.client.Get(ctx, tt.key).Result()
				if err != nil {
					t.Errorf("Get() error = %v", err)
				}
				if gotValue != tt.value {
					t.Errorf("Get() = %v, want %v", gotValue, tt.value)
				}

				if tt.expiration > 0 {
					ttl, err := client.client.TTL(ctx, tt.key).Result()
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

func TestRedisV9_Get(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
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
			setup: func(rdb *RedisV9) {
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

func TestRedisV9_Delete(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		key     string
		wantErr bool
	}{
		{
			name: "Delete existing key",
			setup: func(rdb *RedisV9) {
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

func TestRedisV9_Increment(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		key     string
		want    int64
		wantErr bool
	}{
		{
			name: "Increment existing key",
			setup: func(rdb *RedisV9) {
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

func TestRedisV9_Decrement(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		key     string
		want    int64
		wantErr bool
	}{
		{
			name: "Decrement existing key",
			setup: func(rdb *RedisV9) {
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

func TestRedisV9_Exists(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		key     string
		want    bool
		wantErr bool
	}{
		{
			name: "Check existing key",
			setup: func(rdb *RedisV9) {
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

func TestRedisV9_Expire(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(rdb *RedisV9)
		key        string
		expiration time.Duration
		wantErr    bool
	}{
		{
			name: "Expire existing key",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:        "key1",
			expiration: 10 * time.Second,
			wantErr:    false,
		},
		{
			name:       "Expire non-existing key",
			key:        "key2",
			expiration: 10 * time.Second,
			wantErr:    false,
		},
		{
			name:       "Expire key when server is closed",
			key:        "key3",
			expiration: 10 * time.Second,
			wantErr:    true,
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

			err := client.Expire(context.Background(), tt.key, tt.expiration)
			if (err != nil) != tt.wantErr {
				t.Errorf("Expire() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && tt.setup != nil {
				ttl, err := client.client.TTL(context.Background(), tt.key).Result()
				if err != nil {
					t.Errorf("TTL() error = %v", err)
				}
				if ttl > tt.expiration || ttl <= 0 {
					t.Errorf("TTL() = %v, want less than or equal to %v", ttl, tt.expiration)
				}
			}
		})
	}
}

func TestRedisV9_TTL(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		key     string
		want    int64
		wantErr bool
	}{
		{
			name: "TTL existing key",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 10*time.Second)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			key:     "key1",
			want:    10,
			wantErr: false,
		},
		{
			name:    "TTL non-existing key",
			key:     "key2",
			want:    0,
			wantErr: false,
		},
		{
			name:    "TTL key when server is closed",
			key:     "key3",
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

			got, err := client.TTL(context.Background(), tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("TTL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("TTL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisV9_DeleteKeys(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		keys    []string
		want    int64
		wantErr bool
	}{
		{
			name: "DeleteKeys existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:    []string{"key1", "key2"},
			want:    2,
			wantErr: false,
		},
		{
			name:    "DeleteKeys non-existing keys",
			keys:    []string{"key3", "key4"},
			want:    0,
			wantErr: false,
		},
		{
			name:    "DeleteKeys keys when server is closed",
			keys:    []string{"key5", "key6"},
			want:    0,
			wantErr: true,
		},
		{
			name: "DeleteKeys existing and non-existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key7", "value7", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:    []string{"key7", "key8"},
			want:    1,
			wantErr: false,
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

			got, err := client.DeleteKeys(context.Background(), tt.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("DeleteKeys() got = %v, want %v", got, tt.want)
			}

			if !tt.wantErr {
				for _, key := range tt.keys {
					exists, err := client.Exists(context.Background(), key)
					if err != nil {
						t.Errorf("Exists() error = %v", err)
					}
					if exists {
						t.Errorf("Exists() = %v, want %v", exists, false)
					}
				}
			}
		})
	}
}

func TestRedisV9_DeleteScanMatch(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		pattern string
		want    int64
		wantErr bool
	}{
		{
			name: "DeleteScanMatch existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    2,
			wantErr: false,
		},
		{
			name:    "DeleteScanMatch non-existing keys",
			pattern: "key*",
			want:    0,
			wantErr: false,
		},
		{
			name:    "DeleteScanMatch keys when server is closed",
			pattern: "key*",
			want:    0,
			wantErr: true,
		},
		{
			name: "DeleteScanMatch existing and non-existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key3", "value3", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    1,
			wantErr: false,
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

			got, err := client.DeleteScanMatch(context.Background(), tt.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteScanMatch() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("DeleteScanMatch() got = %v, want %v", got, tt.want)
			}

			if !tt.wantErr {
				keys, err := client.Keys(context.Background(), tt.pattern)
				if err != nil {
					t.Errorf("Keys() error = %v", err)
				}
				if len(keys) > 0 {
					t.Errorf("Keys() = %v, want %v", keys, []string{})
				}
			}
		})
	}
}

func TestRedisV9_Keys(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		pattern string
		want    []string
		wantErr bool
	}{
		{
			name: "Keys existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    []string{"key2", "key1"},
			wantErr: false,
		},
		{
			name:    "Keys non-existing keys",
			pattern: "key*",
			want:    []string{},
			wantErr: false,
		},
		{
			name:    "Keys keys when server is closed",
			pattern: "key*",
			want:    []string{},
			wantErr: true,
		},
		{
			name: "Keys existing and non-existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key3", "value3", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    []string{"key3"},
			wantErr: false,
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

			got, err := client.Keys(context.Background(), tt.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("Keys() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(got) != len(tt.want) {
				t.Errorf("Keys() got = %v, want %v", got, tt.want)
			}

			equal := utils.CompareUnorderedSlices(got, tt.want)
			if !equal {
				t.Errorf("Keys() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisV9_GetMulti(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(rdb *RedisV9)
		keys    []string
		want    []interface{}
		wantErr bool
	}{
		{
			name: "GetMulti existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:    []string{"key1", "key2"},
			want:    []interface{}{"value1", "value2"},
			wantErr: false,
		},
		{
			name:    "GetMulti non-existing keys",
			keys:    []string{"key3", "key4"},
			want:    []interface{}{nil, nil},
			wantErr: false,
		},
		{
			name:    "GetMulti keys when server is closed",
			keys:    []string{"key5", "key6"},
			want:    []interface{}{},
			wantErr: true,
		},
		{
			name: "GetMulti existing and non-existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key7", "value7", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			keys:    []string{"key7", "key8"},
			want:    []interface{}{"value7", nil},
			wantErr: false,
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

			got, err := client.GetMulti(context.Background(), tt.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMulti() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(got) != len(tt.want) {
				t.Errorf("GetMulti() got = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("GetMulti() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestRedisV9_GetKeysAndValuesWithFilter(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(rdb *RedisV9)
		pattern        string
		want           map[string]interface{}
		wantErr        bool
		shutdownServer bool
	}{
		{
			name: "GetKeysAndValuesWithFilter existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key1", "value1", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				err = rdb.Set(context.Background(), "key2", "value2", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    map[string]interface{}{"key1": "value1", "key2": "value2"},
			wantErr: false,
		},
		{
			name:    "GetKeysAndValuesWithFilter non-existing keys",
			pattern: "key*",
			want:    map[string]interface{}{},
			wantErr: true, // MGET fails when no keys are provided
		},
		{
			name: "GetKeysAndValuesWithFilter existing and non-existing keys",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key3", "value3", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "key*",
			want:    map[string]interface{}{"key3": "value3"},
			wantErr: false,
		},
		{
			name: "GetKeysAndValuesWithFilter existing and non-existing keys with empty pattern",
			setup: func(rdb *RedisV9) {
				err := rdb.Set(context.Background(), "key4", "value4", 0)
				if err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			},
			pattern: "",
			want:    map[string]interface{}{},
			wantErr: true, // MGET fails when pattern is empty
		},
		{
			name:           "Keys function returns error",
			pattern:        "key*",
			want:           map[string]interface{}{},
			wantErr:        true,
			shutdownServer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newTestRedis(t)
			if tt.shutdownServer {
				cleanup()
			} else {
				defer cleanup()
			}

			if tt.setup != nil {
				tt.setup(client)
			}

			got, err := client.GetKeysAndValuesWithFilter(context.Background(), tt.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetKeysAndValuesWithFilter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(got) != len(tt.want) {
				t.Errorf("GetKeysAndValuesWithFilter() got = %v, want %v", got, tt.want)
			}
			for k := range got {
				if got[k] != tt.want[k] {
					t.Errorf("GetKeysAndValuesWithFilter() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
