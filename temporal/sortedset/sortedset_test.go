package sortedset

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/flusher"
	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestSortedSet_AddScoredMember(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	testCases := []struct {
		name       string
		key        string
		member     string
		score      float64
		wantResult int64
		wantErr    bool
	}{
		{
			name:       "Add new member",
			key:        "test_sorted_set",
			member:     "member1",
			score:      10.0,
			wantResult: 1,
			wantErr:    false,
		},
		{
			name:       "Add existing member with same score",
			key:        "test_sorted_set",
			member:     "member1",
			score:      10.0,
			wantResult: 0,
			wantErr:    false,
		},
		{
			name:       "Add existing member with different score",
			key:        "test_sorted_set",
			member:     "member1",
			score:      20.0,
			wantResult: 0,
			wantErr:    false,
		},
		{
			name:       "Add member to different key",
			key:        "test_sorted_set_2",
			member:     "member2",
			score:      15.0,
			wantResult: 1,
			wantErr:    false,
		},
		{
			name:       "Add member with high score",
			key:        "test_sorted_set",
			member:     "member3",
			score:      1e6,
			wantResult: 1,
			wantErr:    false,
		},
		{
			name:       "Add member with negative score",
			key:        "test_sorted_set",
			member:     "member4",
			score:      -100.0,
			wantResult: 1,
			wantErr:    false,
		},
	}

	ctx := context.Background()

	for _, connector := range connectors {
		flusher, err := flusher.NewFlusher(connector)
		assert.Nil(t, err)

		defer assert.Nil(t, flusher.FlushAll(ctx))

		for _, tc := range testCases {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				sortedSet, err := NewSortedSet(connector)
				assert.Nil(t, err)

				val, err := sortedSet.AddScoredMember(ctx, tc.key, tc.member, tc.score)
				if tc.wantErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
				}

				assert.Equal(t, tc.wantResult, val)
			})
		}
	}
}

func TestSortedSet_GetMembersByScoreRange(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	testKey := "test_sorted_set"
	testMembers := []struct {
		member string
		score  float64
	}{
		{"member1", 10.0},
		{"member2", 20.0},
		{"member3", 30.0},
	}

	testCases := []struct {
		name            string
		min             string
		max             string
		expectedErr     bool
		expectedMembers []interface{}
		expectedScores  []float64
	}{
		{
			name:            "Partial Overlap",
			min:             "15",
			max:             "25",
			expectedErr:     false,
			expectedMembers: []interface{}{"member2"},
			expectedScores:  []float64{20.0},
		},
		{
			name:            "Empty Range",
			min:             "50",
			max:             "60",
			expectedErr:     false,
			expectedMembers: []interface{}{},
			expectedScores:  []float64{},
		},
		{
			name:            "Large Score Values",
			min:             "-1e9",
			max:             "1e9",
			expectedErr:     false,
			expectedMembers: []interface{}{"member1", "member2", "member3"},
			expectedScores:  []float64{10.0, 20.0, 30.0},
		},
		{
			name:            "Exact Score Match",
			min:             "20",
			max:             "20",
			expectedErr:     false,
			expectedMembers: []interface{}{"member2"},
			expectedScores:  []float64{20.0},
		},
		{
			name:            "Exclusive Range",
			min:             "21",
			max:             "29",
			expectedErr:     false,
			expectedMembers: []interface{}{},
			expectedScores:  []float64{},
		},
		{
			name:            "Negative to Positive Range",
			min:             "-50",
			max:             "50",
			expectedErr:     false,
			expectedMembers: []interface{}{"member1", "member2", "member3"},
			expectedScores:  []float64{10.0, 20.0, 30.0},
		},
		{
			name:            "Inverted Range",
			min:             "30",
			max:             "10",
			expectedErr:     false,
			expectedMembers: []interface{}{},
			expectedScores:  []float64{},
		},
		{
			name:        "Invalid Score Format",
			min:         "not-a-number",
			max:         "100",
			expectedErr: true,
		},
		{
			name:            "Boundary Values",
			min:             "10",
			max:             "30",
			expectedErr:     false,
			expectedMembers: []interface{}{"member1", "member2", "member3"},
			expectedScores:  []float64{10.0, 20.0, 30.0},
		},
	}

	ctx := context.Background()

	for _, connector := range connectors {
		sortedSet, err := NewSortedSet(connector)
		assert.Nil(t, err)

		flusher, err := flusher.NewFlusher(connector)
		assert.Nil(t, err)

		defer assert.Nil(t, flusher.FlushAll(ctx))

		for _, member := range testMembers {
			_, err := sortedSet.AddScoredMember(ctx, testKey, member.member, member.score)
			assert.Nil(t, err)
		}

		for _, tc := range testCases {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				members, scores, err := sortedSet.GetMembersByScoreRange(ctx, testKey, tc.min, tc.max)

				if tc.expectedErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)

					assert.Equal(t, tc.expectedMembers, members)
					assert.Equal(t, tc.expectedScores, scores)
				}
			})
		}
	}
}

func TestSortedSet_RemoveMembersByScoreRange(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	testKey := "test_sorted_set"
	testMembers := []struct {
		member string
		score  float64
	}{
		{"member1", 10.0},
		{"member2", 20.0},
		{"member3", 30.0},
	}

	testCases := []struct {
		name            string
		min             string
		max             string
		expectedErr     bool
		expectedRemoved int64
		setup           func(sortedSet SortedSet) error
	}{
		{
			name:            "Remove Specific Range",
			min:             "15",
			max:             "25",
			expectedErr:     false,
			expectedRemoved: 1,
		},
		{
			name:            "Remove Non-Existent Range",
			min:             "40",
			max:             "50",
			expectedErr:     false,
			expectedRemoved: 0,
		},
		{
			name:        "Remove With Invalid Range",
			min:         "not-a-number",
			max:         "100",
			expectedErr: true,
		},
		{
			name:            "Remove All Members",
			min:             "-inf",
			max:             "+inf",
			expectedErr:     false,
			expectedRemoved: 2,
		},

		{
			name:            "Remove From Empty Set",
			min:             "0",
			max:             "10",
			expectedErr:     false,
			expectedRemoved: 0,
		},
	}

	ctx := context.Background()

	for _, connector := range connectors {
		sortedSet, err := NewSortedSet(connector)
		assert.Nil(t, err)

		flusher, err := flusher.NewFlusher(connector)
		assert.Nil(t, err)

		defer assert.Nil(t, flusher.FlushAll(ctx))

		for _, member := range testMembers {
			_, err := sortedSet.AddScoredMember(ctx, testKey, member.member, member.score)
			assert.Nil(t, err)
		}

		for _, tc := range testCases {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				removed, err := sortedSet.RemoveMembersByScoreRange(ctx, testKey, tc.min, tc.max)

				if tc.expectedErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
					assert.Equal(t, tc.expectedRemoved, removed)
				}
			})
		}
	}
}
