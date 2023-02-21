package mgo

import (
	"context"
	"os"
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
	// defer mgo.Delete(context.Background(), object)

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

func TestMgoDriver_Query(t *testing.T) {
	// Setup
	mgo, object := prepareEnvironment(t)
	sess := mgo.session.Copy()

	defer sess.Close()

	dropCollection(sess, object, t)

	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Bob", Email: "bob@example.com", Id: id.OID(bson.NewObjectId().Hex())},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.OID(bson.NewObjectId().Hex())},
	}

	for _, obj := range dummyData {
		err := mgo.Insert(context.Background(), &obj)
		assert.Nil(t, err)
	}

	defer dropCollection(sess, object, t)

	tests := []struct {
		name          string
		query         bson.M
		sort          string
		limit         int
		offset        int
		expectedCount int
		expected      []dummyDBObject
	}{
		{
			name:          "query with no filters",
			query:         bson.M{},
			limit:         0,
			offset:        0,
			expectedCount: 4,
			expected:      dummyData,
		},
		{
			name:          "query with limit and offset",
			query:         bson.M{},
			limit:         2,
			offset:        1,
			expectedCount: 2,
			expected:      []dummyDBObject{dummyData[1], dummyData[2]},
		},
		{
			name:          "query with name starting with J",
			query:         bson.M{"name": bson.M{"$regex": "^J"}},
			limit:         0,
			offset:        0,
			expectedCount: 2,
			expected:      []dummyDBObject{dummyData[0], dummyData[1]},
		},
		{
			name: "query with emails ending with example.com",
			query: bson.M{
				"email": bson.M{
					"$regex": "example.com$",
				},
			},
			limit:         0,
			offset:        0,
			expectedCount: 2,
			expected:      []dummyDBObject{dummyData[0], dummyData[2]},
		},
		{
			name: "query with emails ending with example.com and limit 1, offset 1",
			query: bson.M{
				"email": bson.M{
					"$regex": "example.com$",
				},
			},
			limit:         1,
			offset:        1,
			expectedCount: 1,
			expected:      []dummyDBObject{dummyData[2]},
		},
		{
			name: "query with emails ending with example.com and limit 1, offset 2",
			query: bson.M{
				"email": bson.M{
					"$regex": "example.com$",
				},
			},
			limit:         1,
			offset:        2,
			expectedCount: 0,
			expected:      []dummyDBObject{},
		},
		{
			name: "query with emails starting with j, sorted by name",
			query: bson.M{
				"email": bson.M{
					"$regex": "^j",
				},
			},
			sort:          "name",
			limit:         0,
			offset:        0,
			expectedCount: 2,
			expected:      []dummyDBObject{dummyData[1], dummyData[0]},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Query the database
			var result []dummyDBObject
			err := mgo.Query(context.Background(), object.TableName(), &result, model.DBM{
				"query":  test.query,
				"sort":   test.sort,
				"limit":  test.limit,
				"offset": test.offset,
			})
			assert.Nil(t, err)

			// Check the number of documents returned
			assert.Equal(t, test.expectedCount, len(result))

			// Check that the returned documents match the expected ones
			for i, doc := range result {
				assert.Equal(t, test.expected[i].Name, doc.Name)
				assert.Equal(t, test.expected[i].Email, doc.Email)
			}
		})
	}
}
