package temporal

import (
	"testing"

	"github.com/TykTechnologies/storage/temporal/connector"
	"github.com/TykTechnologies/storage/temporal/connector/types"
)

func TestNewKeyValue(t *testing.T) {
	tests := []struct {
		name      string
		setupConn func() (types.Connector, error)
		wantErr   bool
	}{
		{
			name: "Redis8",
			setupConn: func() (types.Connector, error) {
				return connector.NewConnector(types.RedisV8Type, types.WithRedisConfig(&types.RedisOptions{
					Addrs: []string{"localhost:6379"},
				}))
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := tt.setupConn()
			if err != nil {
				t.Errorf("setupConn() error = %v", err)
				return
			}
			_, err = NewKeyValue(conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKeyValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
