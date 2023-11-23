package set

import (
	"context"
	"testing"

	"github.com/TykTechnologies/storage/temporal/flusher"
	"github.com/TykTechnologies/storage/temporal/internal/testutil"
	"github.com/TykTechnologies/storage/temporal/model"
	"github.com/stretchr/testify/assert"
)

func TestSet_AddMember(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name            string
		key             string
		member          string
		setup           func(db Set)
		expectedMembers []string
		expectedErr     error
	}{
		{
			name:   "new_set",
			key:    "key",
			member: "member",
			expectedMembers: []string{
				"member",
			},
			expectedErr: nil,
		},
		{
			name:   "existing_set",
			key:    "key",
			member: "member2",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member")
				assert.Nil(t, err)
			},
			expectedMembers: []string{
				"member", "member2",
			},
			expectedErr: nil,
		},
		{
			name:            "empty_key",
			key:             "",
			member:          "member",
			expectedMembers: []string{},
			expectedErr:     model.ErrKeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				set, err := NewSet(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(set)
				}

				err = set.AddMember(ctx, tc.key, tc.member)
				assert.Equal(t, tc.expectedErr, err)

				if tc.expectedErr == nil {
					currentMembers, err := set.Members(ctx, tc.key)
					assert.Nil(t, err)
					assert.ElementsMatch(t, tc.expectedMembers, currentMembers)
				}
			})
		}
	}
}

func TestSet_Members(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name            string
		key             string
		setup           func(db Set)
		expectedMembers []string
		expectedErr     error
	}{
		{
			name:            "new_set",
			key:             "key",
			expectedMembers: []string{},
			expectedErr:     nil,
		},
		{
			name: "existing_set",
			key:  "key",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member")
				assert.Nil(t, err)
			},
			expectedMembers: []string{
				"member",
			},
			expectedErr: nil,
		},
		{
			name:            "empty_key",
			key:             "",
			expectedMembers: []string{},
			expectedErr:     model.ErrKeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				set, err := NewSet(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(set)
				}

				currentMembers, err := set.Members(ctx, tc.key)
				assert.Equal(t, tc.expectedErr, err)
				assert.ElementsMatch(t, tc.expectedMembers, currentMembers)
			})
		}
	}
}

func TestSet_IsMember(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name             string
		key              string
		member           string
		setup            func(db Set)
		expectedIsMember bool
		expectedErr      error
	}{
		{
			name:             "new_set",
			key:              "key",
			member:           "member",
			expectedIsMember: false,
			expectedErr:      nil,
		},
		{
			name:   "existing_set_is_member",
			key:    "key",
			member: "member2",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member1")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member2")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member3")
				assert.Nil(t, err)
			},
			expectedIsMember: true,
			expectedErr:      nil,
		},

		{
			name:   "existing_set_is_not_member",
			key:    "key",
			member: "member4",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member1")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member2")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member3")
				assert.Nil(t, err)
			},
			expectedIsMember: false,
			expectedErr:      nil,
		},
		{
			name:             "empty_key",
			key:              "",
			member:           "member",
			expectedIsMember: false,
			expectedErr:      model.ErrKeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				set, err := NewSet(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(set)
				}

				isMember, err := set.IsMember(ctx, tc.key, tc.member)
				assert.Equal(t, tc.expectedErr, err)
				assert.Equal(t, tc.expectedIsMember, isMember)
			})
		}
	}
}

func TestSet_RemoveMember(t *testing.T) {
	connectors := testutil.TestConnectors(t)
	defer testutil.CloseConnectors(t, connectors)

	tcs := []struct {
		name            string
		key             string
		member          string
		setup           func(db Set)
		expectedMembers []string
		expectedErr     error
	}{
		{
			name:            "new_set",
			key:             "key",
			member:          "member",
			expectedMembers: []string{},
			expectedErr:     nil,
		},
		{
			name:   "existing_set_remove_member",
			key:    "key",
			member: "member2",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member1")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member2")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member3")
				assert.Nil(t, err)
			},
			expectedMembers: []string{
				"member1", "member3",
			},
			expectedErr: nil,
		},

		{
			name:   "existing_set_remove_not_member",
			key:    "key",
			member: "member4",
			setup: func(db Set) {
				err := db.AddMember(context.Background(), "key", "member1")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member2")
				assert.Nil(t, err)

				err = db.AddMember(context.Background(), "key", "member3")
				assert.Nil(t, err)
			},
			expectedMembers: []string{
				"member1", "member2", "member3",
			},
			expectedErr: nil,
		},
		{
			name:            "empty_key",
			key:             "",
			member:          "member",
			expectedMembers: []string{},
			expectedErr:     model.ErrKeyEmpty,
		},
	}

	for _, connector := range connectors {
		for _, tc := range tcs {
			t.Run(connector.Type()+"_"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				set, err := NewSet(connector)
				assert.Nil(t, err)

				flusher, err := flusher.NewFlusher(connector)
				assert.Nil(t, err)
				defer assert.Nil(t, flusher.FlushAll(ctx))

				if tc.setup != nil {
					tc.setup(set)
				}

				err = set.RemoveMember(ctx, tc.key, tc.member)
				assert.Equal(t, tc.expectedErr, err)

				if tc.expectedErr == nil && tc.expectedMembers != nil {
					actualMembers, err := set.Members(ctx, tc.key)
					assert.Nil(t, err)
					assert.ElementsMatch(t, tc.expectedMembers, actualMembers)
				}
			})
		}
	}
}
