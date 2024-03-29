package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/temporal/model"
)

func TestGetRedisAddrs(t *testing.T) {
	tests := []struct {
		name string
		opts model.RedisOptions
		want []string
	}{
		{
			name: "With Addrs",
			opts: model.RedisOptions{
				Addrs: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6379"},
		},
		{
			name: "With Hosts map",
			opts: model.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379", "127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379", "127.0.0.2:6380"},
		},
		{
			name: "With Host and Port",
			opts: model.RedisOptions{
				Host: "127.0.0.1",
				Port: 6379,
			},
			want: []string{"127.0.0.1:6379"},
		},
		{
			name: "With empty options",
			opts: model.RedisOptions{},
			want: []string{},
		},
		{
			name: "With Addrs and Hosts map",
			opts: model.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				Hosts: map[string]string{"127.0.0.2": "6380"},
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Hosts map
		},
		{
			name: "With Addrs and Host/Port",
			opts: model.RedisOptions{
				Addrs: []string{"127.0.0.1:6379"},
				Host:  "127.0.0.2",
				Port:  6380,
			},
			want: []string{"127.0.0.1:6379"}, // Addrs takes priority over Host/Port
		},
		{
			name: "With Hosts map and Host/Port",
			opts: model.RedisOptions{
				Hosts: map[string]string{"127.0.0.1": "6379"},
				Host:  "127.0.0.2",
				Port:  6380,
			},
			want: []string{"127.0.0.1:6379"}, // Hosts map takes priority over Host/Port
		},
		{
			name: "With all empty values",
			opts: model.RedisOptions{
				Hosts: map[string]string{},
				Host:  "",
				Port:  0,
			},
			want: []string{},
		},
		{
			name: "With Port only",
			opts: model.RedisOptions{
				Port: 6379,
			},
			want: []string{":6379"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetRedisAddrs(&tt.opts)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
