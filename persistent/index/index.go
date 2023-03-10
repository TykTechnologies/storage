package index

import "github.com/TykTechnologies/storage/persistent/dbm"

type Index struct {
	Name       string
	Background bool
	Keys       []dbm.DBM
	IsTTLIndex bool
	TTL        int
}
