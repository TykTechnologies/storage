package local

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/TykTechnologies/storage/temporal/temperr"
)

type SortedSetEntry struct {
	Score  float64
	Member string
}

type SortedSet []SortedSetEntry

func (s SortedSet) Len() int      { return len(s) }
func (s SortedSet) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortedSet) Less(i, j int) bool {
	if s[i].Score == s[j].Score {
		return s[i].Member < s[j].Member
	}
	return s[i].Score < s[j].Score
}

func NewSortedSetObject() *Object {
	return &Object{
		Type:  TypeSortedSet,
		Value: make(SortedSet, 0),
		NoExp: true,
	}
}

func (api *API) AddScoredMember(ctx context.Context, key, member string, score float64) (int64, error) {
	var added int64
	o, err := api.Store.Get(key)
	if err != nil {
		o = NewSortedSetObject()
	}

	if o == nil {
		o = NewSortedSetObject()
	}

	if o.Type != TypeSortedSet {
		return 0, fmt.Errorf("key is not a sorted set")
	}

	sortedSet := o.Value.(SortedSet)

	index := -1
	for j, entry := range sortedSet {
		if entry.Member == member {
			index = j
			break
		}
	}

	if index == -1 {
		sortedSet = append(sortedSet, SortedSetEntry{Score: score, Member: member})
		added++
	} else {
		sortedSet[index].Score = score
	}

	sort.Sort(sortedSet)
	o.Value = sortedSet
	api.Store.Set(key, o)
	return added, nil
}

func (api *API) GetMembersByScoreRange(ctx context.Context, key, minScore, maxScore string) ([]interface{}, []float64, error) {
	o, err := api.Store.Get(key)
	if err != nil {
		return nil, nil, err
	}

	if o == nil {
		return []interface{}{}, []float64{}, nil
	}

	if o.Deleted || o.IsExpired() {
		return []interface{}{}, []float64{}, nil
	}

	if o.Type != TypeSortedSet {
		return nil, nil, temperr.KeyMisstype
	}

	sortedSet := o.Value.(SortedSet)

	from, fromInclusive, err := parseScore(minScore)
	if err != nil {
		return []interface{}{}, []float64{}, err
	}

	to, toInclusive, err := parseScore(maxScore)
	if err != nil {
		return []interface{}{}, []float64{}, err
	}

	var members = make([]interface{}, 0)
	var scores = make([]float64, 0)

	for _, entry := range sortedSet {
		if (fromInclusive && entry.Score >= from || !fromInclusive && entry.Score > from) &&
			(toInclusive && entry.Score <= to || !toInclusive && entry.Score < to) {
			members = append(members, entry.Member)
			scores = append(scores, entry.Score)
		}
		if entry.Score > to {
			break
		}
	}

	return members, scores, nil
}

func (api *API) RemoveMembersByScoreRange(ctx context.Context, key, minScore, maxScore string) (int64, error) {
	o, err := api.Store.Get(key)
	var removed int64
	if err != nil {
		return 0, err
	}

	if o == nil {
		return 0, nil
	}

	if o.Type != TypeSortedSet {
		return 0, temperr.KeyMisstype
	}

	if o.Deleted || o.IsExpired() {
		return 0, nil
	}

	sortedSet := o.Value.(SortedSet)

	from, fromInclusive, err := parseScore(minScore)
	if err != nil {
		return 0, err
	}

	to, toInclusive, err := parseScore(maxScore)
	if err != nil {
		return 0, err
	}

	var newSet SortedSet

	for _, entry := range sortedSet {
		if (fromInclusive && entry.Score >= from || !fromInclusive && entry.Score > from) &&
			(toInclusive && entry.Score <= to || !toInclusive && entry.Score < to) {
			// Skip this entry (effectively removing it)
			removed++
			continue
		}
		newSet = append(newSet, entry)
	}

	o.Value = newSet
	err = api.Store.Set(key, o)
	if err != nil {
		return 0, err
	}

	return removed, nil
}

func parseScore(score string) (float64, bool, error) {
	inclusive := true
	if strings.HasPrefix(score, "(") {
		inclusive = false
		score = score[1:]
	}

	if score == "-inf" {
		return math.Inf(-1), inclusive, nil
	}
	if score == "+inf" {
		return math.Inf(1), inclusive, nil
	}

	value, err := strconv.ParseFloat(score, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid score: %s", score)
	}

	return value, inclusive, nil
}
