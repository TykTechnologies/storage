package ratelimiter

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/flusher"
	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/TykTechnologies/storage/temporal/temperr"
	"github.com/stretchr/testify/assert"
)

func TestRedisCluster_SetRollingWindow(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name          string
		keyName       string
		per           int64
		valueOverride string
		pipeline      bool
		expectedErr   error
		expectedLen   int
	}{
		{
			name:          "valid_rolling_window",
			keyName:       "key1",
			per:           60,
			valueOverride: "value1",
			pipeline:      false,
			expectedErr:   nil,
			expectedLen:   0,
		},
		{
			name:          "empty_key_name",
			keyName:       "",
			per:           60,
			valueOverride: "value2",
			pipeline:      false,
			expectedErr:   temperr.KeyEmpty,
			expectedLen:   0,
		},
		{
			name:          "negative_period",
			keyName:       "key2",
			per:           -10,
			valueOverride: "value3",
			pipeline:      false,
			expectedErr:   temperr.InvalidPeriod,
			expectedLen:   0,
		},
		{
			name:          "pipeline_enabled",
			keyName:       "key_pipeline",
			per:           60,
			valueOverride: "pipeline_value",
			pipeline:      true,
			expectedErr:   nil,
			expectedLen:   0,
		},
		{
			name:          "valueOverride",
			keyName:       "key_value_override",
			per:           60,
			valueOverride: "-1",
			pipeline:      false,
			expectedErr:   nil,
			expectedLen:   0,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				rateLimiter, err := NewRateLimit(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				result, err := rateLimiter.SetRollingWindow(ctx, tc.keyName, tc.per, tc.valueOverride, tc.pipeline)

				assert.Equal(t, tc.expectedErr, err)

				if err == nil {
					assert.Equal(t, tc.expectedLen, len(result))
					// Executing SetRollingWindow again should return expectedLen + 1 if err == nil
					result, err = rateLimiter.SetRollingWindow(ctx, tc.keyName, tc.per, tc.valueOverride, tc.pipeline)
					assert.NoError(t, err)
					assert.Equal(t, tc.expectedLen+1, len(result))
				}
			})
		}
	}
}

func TestRedisCluster_GetRollingWindow(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name        string
		keyName     string
		per         int64
		pipeline    bool
		expectedErr error
		expectedLen int
		preTest     func(ctx context.Context, rateLimiter model.RateLimit)
	}{
		{
			name:        "empty_sorted_set",
			keyName:     "key_empty",
			per:         60,
			pipeline:    false,
			expectedErr: nil,
			expectedLen: 0,
		},
		{
			name:        "non_empty_sorted_set",
			keyName:     "key_non_empty",
			per:         60,
			pipeline:    false,
			expectedErr: nil,
			expectedLen: 2,
			preTest: func(ctx context.Context, rateLimiter model.RateLimit) {
				_, err := rateLimiter.SetRollingWindow(ctx, "key_non_empty", 60, "value1", false)
				assert.Nil(t, err)
				_, err = rateLimiter.SetRollingWindow(ctx, "key_non_empty", 60, "value2", false)
				assert.Nil(t, err)
			},
		},
		{
			name:        "pipeline_enabled",
			keyName:     "key_pipeline",
			per:         60,
			pipeline:    true,
			expectedErr: nil,
			expectedLen: 0,
		},
		{
			name:        "negative_period",
			keyName:     "key_negative_period",
			per:         -10,
			pipeline:    false,
			expectedErr: temperr.InvalidPeriod,
			expectedLen: 0,
		},
		{
			name:        "empty_key_name",
			keyName:     "",
			per:         60,
			pipeline:    false,
			expectedErr: temperr.KeyEmpty,
			expectedLen: 0,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				rateLimiter, err := NewRateLimit(connector)
				assert.Nil(t, err)

				if tc.preTest != nil {
					tc.preTest(ctx, rateLimiter)
				}

				result, err := rateLimiter.GetRollingWindow(ctx, tc.keyName, tc.per, tc.pipeline)

				assert.Equal(t, tc.expectedErr, err)
				if err == nil {
					assert.Equal(t, tc.expectedLen, len(result))
				}
			})
		}
	}
}
