package local

import (
	"context"
	"encoding/json"
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

func (s *SortedSetEntry) MarshalJSON() ([]byte, error) {
	asStr := fmt.Sprintf("%f:%s", s.Score, s.Member)
	b, err := json.Marshal(asStr)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func NewSortedSetObject() *Object {
	return &Object{
		Type:  TypeSortedSet,
		Value: make(SortedSet, 0),
		NoExp: true,
	}
}

func (api *API) getSortedSetValue(o *Object) (SortedSet, error) {
	if o.Value == nil {
		return nil, nil
	}

	switch o.Value.(type) {
	case SortedSet:
		return o.Value.(SortedSet), nil
	case []interface{}:
		sortedSet := make(SortedSet, 0)
		arr := o.Value.([]interface{})
		for i, _ := range arr {
			vs, ok := arr[i].(string)
			if !ok {
				return nil, fmt.Errorf("invalid set member (tp): %T", arr[i])
			}

			parts := strings.Split(vs, ":")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid set member (parts): %T", vs)
			}

			score, err := strconv.ParseFloat(parts[0], 64)
			if err != nil {
				return nil, fmt.Errorf("invalid set member (parse): %T", vs)
			}

			sortedSet = append(sortedSet, SortedSetEntry{
				Score:  score,
				Member: parts[1],
			})
		}
		return sortedSet, nil

	default:
		return nil, fmt.Errorf("invalid sorted set")

	}
}

func (api *API) AddScoredMember(ctx context.Context, key, member string, score float64) (int64, error) {
	var added int64
	o, err := api.Store.Get(key)
	if err != nil {
		o = NewSortedSetObject()
		api.addToKeyIndex(key)
	}

	if o == nil {
		o = NewSortedSetObject()
		api.addToKeyIndex(key)
	}

	if o.Type != TypeSortedSet {
		return 0, fmt.Errorf("key is not a sorted set")
	}

	sortedSet, err := api.getSortedSetValue(o)
	if err != nil {
		return 0, err
	}

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
	err = api.Store.Set(key, o)
	if err != nil {
		return 0, err
	}
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

	sortedSet, err := api.getSortedSetValue(o)
	if err != nil {
		return nil, nil, err
	}

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

	sortedSet, err := api.getSortedSetValue(o)
	if err != nil {
		return 0, err
	}

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
