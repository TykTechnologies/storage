package poolstats_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TykTechnologies/storage/poolstats"
)

func TestFieldSet_Has(t *testing.T) {
	tcs := []struct {
		name string
		set  poolstats.FieldSet
		has  []poolstats.FieldSet
		not  []poolstats.FieldSet
	}{
		{
			name: "empty_set_reports_nothing",
			set:  0,
			not: []poolstats.FieldSet{
				poolstats.FieldMaxOpen, poolstats.FieldOpen, poolstats.FieldInUse,
				poolstats.FieldIdle, poolstats.FieldWaitCount, poolstats.FieldWaitDuration,
				poolstats.FieldCheckOutFailures,
			},
		},
		{
			name: "partial_set",
			set:  poolstats.FieldOpen | poolstats.FieldInUse | poolstats.FieldIdle,
			has:  []poolstats.FieldSet{poolstats.FieldOpen, poolstats.FieldInUse, poolstats.FieldIdle},
			not: []poolstats.FieldSet{
				poolstats.FieldMaxOpen, poolstats.FieldWaitCount, poolstats.FieldWaitDuration,
				poolstats.FieldCheckOutFailures,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, f := range tc.has {
				assert.True(t, tc.set.Has(f))
			}

			for _, f := range tc.not {
				assert.False(t, tc.set.Has(f))
			}
		})
	}
}

func TestFieldBitsAreDistinct(t *testing.T) {
	fields := []poolstats.FieldSet{
		poolstats.FieldMaxOpen, poolstats.FieldOpen, poolstats.FieldInUse,
		poolstats.FieldIdle, poolstats.FieldWaitCount, poolstats.FieldWaitDuration,
		poolstats.FieldCheckOutFailures,
	}

	seen := poolstats.FieldSet(0)

	for _, f := range fields {
		assert.NotZero(t, f)
		assert.Zero(t, seen&f, "field bits must not overlap")
		seen |= f
	}
}
