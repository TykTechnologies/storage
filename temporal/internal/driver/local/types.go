package local

import (
	"time"

	"github.com/TykTechnologies/storage/temporal/model"
)

type Object struct {
	Exp       time.Time
	Type      string
	NoExp     bool
	Deleted   bool
	DeletedAt time.Time
	Value     interface{}
}

func (o *Object) IsExpired() bool {
	if o.NoExp {
		return false
	}

	return time.Now().After(o.Exp)
}

func (o *Object) SetExpire(d time.Duration) {
	o.Exp = time.Now().Add(d)
	o.NoExp = false
}

func (o *Object) SetExpireAt(t time.Time) {
	o.Exp = t
	o.NoExp = false
}

type KVStore interface {
	Get(key string) (*Object, error)
	Set(key string, value interface{}) error
	Delete(key string) error
	FlushAll() error
}

type Broker interface {
	Publish(channel, message string) (int64, error)
	Subscribe(channels ...string) model.Subscription
}
