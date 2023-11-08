package temporal

import (
	"testing"

	"github.com/TykTechnologies/storage/temporal/internal/types"
)

func TestNewKeyValue(t *testing.T) {
	tests := []struct {
		name    string
		opts    *types.ClientOpts
		wantErr bool
	}{
		{
			name: "Redis8",
			opts: &types.ClientOpts{
				Type: Redis8,
				Addr: "localhost:6379",
			},
			wantErr: false,
		},
		{
			name: "Invalid",
			opts: &types.ClientOpts{
				Type: "Invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKeyValue(tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKeyValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
