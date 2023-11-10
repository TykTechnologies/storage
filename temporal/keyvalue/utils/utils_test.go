package utils

import (
	"testing"

	"github.com/TykTechnologies/storage/temporal/keyvalue/internal/types"
)

func TestGetRedisAddrs(t *testing.T) {
	tests := []struct {
		name string
		opts types.RedisOptions
		want []string
	}{
		{
			name: "With Addrs",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
		},
		{
			name: "With Hosts map",
			opts: types.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379", "127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6380"},
		},
		{
			name: "With Host and Port",
			opts: types.RedisOptions{
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.1",
					Port: 6379,
				},
			},
			want: []string{"127.0.0.1:6379"},
		},
		{
			name: "With empty options",
			opts: types.RedisOptions{},
			want: []string{},
		},
		{
			name: "With Addrs and Hosts map",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				Hosts: map[string]string{"127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Hosts map
		},
		{
			name: "With Addrs and Host/Port",
			opts: types.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.2",
					Port: 6380,
				},
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Host/Port
		},
		{
			name: "With Hosts map and Host/Port",
			opts: types.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379"},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "127.0.0.2",
					Port: 6380,
				},
			},
			want: []string{"127.0.0.1:6379"}, // Hosts map takes priority over Host/Port
		},
		{
			name: "With all empty values",
			opts: types.RedisOptions{
				Hosts: map[string]string{},
				BaseStorageOptions: types.BaseStorageOptions{
					Host: "",
					Port: 0,
				},
			},
			want: []string{},
		},
		{
			name: "With Port only",
			opts: types.RedisOptions{
				BaseStorageOptions: types.BaseStorageOptions{
					Port: 6379,
				},
			},
			want: []string{":6379"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetRedisAddrs(&tt.opts)
			equals := CompareUnorderedSlices(got, tt.want)
			if !equals {
				t.Errorf("getRedisAddrs() = %v, want %v", got, tt.want)
			}
		})
	}
}
