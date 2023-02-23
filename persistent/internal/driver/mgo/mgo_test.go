package mgo

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type dummyDBObject struct {
	Id    id.OID `bson:"_id,omitempty"`
	Name  string `bson:"name"`
	Email string `bson:"email"`
}

func (d dummyDBObject) GetObjectID() id.OID {
	return d.Id
}

func (d *dummyDBObject) SetObjectID(id id.OID) {
	d.Id = id
}

func (d dummyDBObject) TableName() string {
	if os.Getenv("INVALID_TABLENAME") != "" {
		return ""
	}
	return "dummy"
}

// prepareEnvironment returns a new mgo driver connection and a dummy object to test
func prepareEnvironment(t *testing.T) (*mgoDriver, *dummyDBObject) {
	t.Helper()
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017/test",
		UseSSL:           false,
	})
	if err != nil {
		t.Fatal(err)
	}
	// create a new dummy object
	object := &dummyDBObject{
		Name:  "test",
		Email: "test@test.com",
	}

	return mgo, object
}

func TestInsert(t *testing.T) {
	mgo, object := prepareEnvironment(t)

	// insert the object into the database
	err := mgo.Insert(context.Background(), object)
	assert.Nil(t, err)
	// delete the object from the database
	defer mgo.Delete(context.Background(), object)

	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestDelete(t *testing.T) {
	mgo, object := prepareEnvironment(t)

	// insert the object into the database
	err := mgo.Insert(context.Background(), object)
	assert.Nil(t, err)
	// check if the object was inserted
	sess := mgo.session.Copy()
	defer sess.Close()
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())

	// delete the object from the database
	err = mgo.Delete(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was deleted
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.NotNil(t, err)
	assert.True(t, mgo.IsErrNoRows(err))
}

func TestUpdate(t *testing.T) {
	mgo, object := prepareEnvironment(t)
	// insert the object into the database
	err := mgo.Insert(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was inserted
	sess := mgo.session.Copy()
	col := sess.DB("").C(object.TableName())

	defer func() {
		sess.Close()

		err = mgo.Delete(context.Background(), object)
		if err != nil {
			t.Fatal("Error deleting object", err)
		}
	}()

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())

	// update the object
	object.Name = "test2"
	object.Email = "test2@test2.com"
	err = mgo.Update(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was updated
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestIsErrNoRows(t *testing.T) {
	mgoDriver := mgoDriver{}

	assert.True(t, mgoDriver.IsErrNoRows(mgo.ErrNotFound))
	assert.False(t, mgoDriver.IsErrNoRows(nil))
	assert.False(t, mgoDriver.IsErrNoRows(mgo.ErrCursor))
}

func Test_mgoDriver_Count(t *testing.T) {
	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{
			name: "0 objects",
			want: 0,
		},
		{
			name: "1 object",
			want: 1,
		},
		{
			name: "2 objects",
			want: 2,
		},
		{
			name: "10 objects",
			want: 10,
		},
		{
			name:    "failing because of invalid table name",
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgo, object := prepareEnvironment(t)
			// Making sure dummy collection is empty before running tests
			sess := mgo.session.Copy()
			defer sess.Close()
			dropCollection(sess, object, t)

			for i := 0; i < tt.want; i++ {
				object = &dummyDBObject{
					Name:  "test" + strconv.Itoa(i),
					Email: "test@test.com",
				}
				err := mgo.Insert(context.Background(), object)
				assert.Nil(t, err)
			}
			// Making sure dummy collection is empty after running tests
			defer dropCollection(sess, object, t)

			if tt.wantErr {
				os.Setenv("INVALID_TABLENAME", "true")
				defer os.Unsetenv("INVALID_TABLENAME")
			}
			got, err := mgo.Count(context.Background(), object)
			if (err != nil) != tt.wantErr {
				t.Errorf("mgoDriver.Count() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("mgoDriver.Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func dropCollection(sess *mgo.Session, object *dummyDBObject, t *testing.T) {
	col := sess.DB("").C(object.TableName())
	err := col.DropCollection()
	if err != nil {
		// If no object has been inserted yet, the collection does not exist
		if err.Error() != "ns not found" {
			t.Fatal("Error dropping collection", err)
		}
	}
}

func Test_mgoDriver_Query(t *testing.T) {
	type args struct {
		result interface{}
		query  model.DBM
	}

	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Bob", Email: "bob@example.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Peter", Email: "peter@test.com", Id: id.OID(bson.NewObjectId().Hex())},
	}

	tests := []struct {
		name           string
		args           args
		expectedResult interface{}
		wantErr        bool
	}{
		{
			name: "4 objects",
			args: args{
				result: &[]dummyDBObject{},
				query:  model.DBM{},
			},
			expectedResult: &dummyData,
		},
		{
			name: "4 objects with limit 2",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_limit": 2,
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[1]},
		},
		{
			name: "4 objects with limit 2 and offset 2",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_limit":  2,
					"_offset": 2,
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[2], dummyData[3]},
		},
		{
			name: "4 objects with limit 2 and offset 2 and sort by name",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_limit":  2,
					"_offset": 2,
					"_sort":   "name",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[1], dummyData[0]},
		},
		{
			name: "filter by email ending with tyk.com",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"email": model.DBM{
						"$regex": "tyk.com$",
					},
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[1], dummyData[3]},
		},
		{
			name: "filter by email ending with tyk.com and sort by name",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"email": model.DBM{
						"$regex": "tyk.com$",
					},
					"_sort": "name",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[3], dummyData[1]},
		},
		{
			name: "filter by name starting with A",
			args: args{
				result: &dummyDBObject{},
				query: model.DBM{
					"name": model.DBM{
						"$regex": "^A",
					},
				},
			},
			expectedResult: &dummyData[3],
		},
		{
			name: "filter by name starting with J and sort by name",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"name": model.DBM{
						"$regex": "^J",
					},
					"_sort": "name",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[1], dummyData[0]},
		},
		{
			name: "invalid db name",
			args: args{
				result: &[]dummyDBObject{},
				query:  model.DBM{},
			},
			wantErr:        true,
			expectedResult: &[]dummyDBObject{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgo, object := prepareEnvironment(t)
			// Making sure dummy collection is empty before running tests
			sess := mgo.session.Copy()
			defer sess.Close()
			dropCollection(sess, object, t)

			// Making sure dummy collection is empty after running tests
			defer dropCollection(sess, object, t)

			for _, obj := range dummyData {
				err := mgo.Insert(context.Background(), &obj)
				assert.Nil(t, err)
			}

			if tt.wantErr {
				os.Setenv("INVALID_TABLENAME", "true")
				defer os.Unsetenv("INVALID_TABLENAME")
			}

			if err := mgo.Query(context.Background(), object, tt.args.result, tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("mgoDriver.Query() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedResult, tt.args.result)
		})
	}
}

func TestBuildQuery(t *testing.T) {
	d := &mgoDriver{}
	collection := &mgo.Collection{}

	tests := []struct {
		name     string
		query    model.DBM
		expected bson.M
	}{
		{
			name:     "Empty Query",
			query:    model.DBM{},
			expected: bson.M{},
		},
		{
			name:     "Simple Query with One Key-Value Pair",
			query:    model.DBM{"name": "John"},
			expected: bson.M{"name": "John"},
		},
		{
			name:     "Query with Multiple Key-Value Pairs",
			query:    model.DBM{"name": "John", "age": 30},
			expected: bson.M{"name": "John", "age": 30},
		},
		{
			name: "Query with Nested Query",
			query: model.DBM{
				"name": model.DBM{
					"$ne": "Bob",
				},
			},
			expected: bson.M{
				"name": bson.M{
					"$ne": "Bob",
				},
			},
		},
		{
			name: "Query with Nested Query Containing $i",
			query: model.DBM{
				"name": model.DBM{
					"$i": "john",
				},
			},
			expected: bson.M{
				"name": &bson.RegEx{
					Pattern: "^john$",
					Options: "i",
				},
			},
		},
		{
			name: "Query with Nested Query Containing $text",
			query: model.DBM{
				"name": model.DBM{
					"$text": "John",
				},
			},
			expected: bson.M{
				"name": bson.M{
					"$regex": bson.RegEx{
						Pattern: "John",
						Options: "i",
					},
				},
			},
		},
		{
			name:     "Query with _id",
			query:    model.DBM{"_id": bson.ObjectIdHex("6068ff6b2242597b683cef38")},
			expected: bson.M{"_id": bson.ObjectIdHex("6068ff6b2242597b683cef38")},
		},
		{
			name:     "Query with $or",
			query:    model.DBM{"$or": []model.DBM{{"name": "John"}, {"name": "Bob"}}},
			expected: bson.M{"$or": []bson.M{{"name": "John"}, {"name": "Bob"}}},
		},
		{
			name: "Query with slice",
			query: model.DBM{
				"name": []string{"Alice", "Bob"},
			},
			expected: bson.M{
				"name": bson.M{"$in": []string{"Alice", "Bob"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := d.buildQuery(test.query, collection)
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("mgoDriver.buildQuery() = %v, want %v", result, test.expected)
			}
		})
	}
}

func TestHandleStoreError(t *testing.T) {
	mgo, _ := prepareEnvironment(t)

	tests := []struct {
		name          string
		inputErr      error
		wantErr       error
		wantReconnect bool
	}{
		{
			name:     "Nil input error",
			inputErr: nil,
			wantErr:  nil,
		},
		{
			name:          "Known connection error",
			inputErr:      errors.New("no reachable servers"),
			wantErr:       nil,
			wantReconnect: true,
		},
		{
			name:     "Unknown connection error",
			inputErr: errors.New("unknown error"),
			wantErr:  nil,
		},
		{
			name:          "i/o timeout",
			inputErr:      errors.New("i/o timeout"),
			wantErr:       nil,
			wantReconnect: true,
		},
		{
			name:          "failing when reconnecting",
			inputErr:      errors.New("reset by peer"),
			wantErr:       errors.New("reset by peer"),
			wantReconnect: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sess := mgo.session
			defer sess.Close()

			if test.wantErr != nil {
				invalidMgo := *mgo
				invalidMgo.options = model.ClientOpts{
					ConnectionString:  "mongodb://host:port/invalid",
					ConnectionTimeout: 1,
				}
				err := invalidMgo.HandleStoreError(test.inputErr)
				if err == nil {
					t.Errorf("expected error to be returned when mgo is nil")
				}
				return
			}

			gotErr := mgo.HandleStoreError(test.inputErr)

			if test.wantReconnect {
				if sess == mgo.session {
					t.Errorf("session was not reconnected when it should have been")
				}
			} else {
				if sess != mgo.session {
					t.Errorf("session was reconnected when it shouldn't have been")
				}
			}

			if !errors.Is(gotErr, test.wantErr) {
				t.Errorf("got error %v, want error %v", gotErr, test.wantErr)
			}
		})
	}
}
