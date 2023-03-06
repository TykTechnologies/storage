package mgo

import (
	"context"
	"errors"
	"fmt"
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
	Id                id.ObjectId       `bson:"_id,omitempty"`
	Name              string            `bson:"name"`
	Email             string            `bson:"email"`
	Country           dummyCountryField `bson:"country"`
	Age               int               `bson:"age"`
	invalidCollection bool
}

type dummyCountryField struct {
	CountryName string `bson:"country_name"`
	Continent   string `bson:"continent"`
}

func (d dummyDBObject) GetObjectID() id.ObjectId {
	return d.Id
}

func (d *dummyDBObject) SetObjectID(id id.ObjectId) {
	d.Id = id
}

func (d dummyDBObject) TableName() string {
	if d.invalidCollection {
		return ""
	}
	return "dummy"
}

// prepareEnvironment returns a new mgo driver connection and a dummy object to test
func prepareEnvironment(t *testing.T) (*mgoDriver, *dummyDBObject, *mgo.Session) {
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
		Name:    "test",
		Email:   "test@test.com",
		Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
		Age:     10,
	}

	// create a session
	sess := mgo.session.Copy()
	dropCollection(sess, object, t)

	return mgo, object, sess
}

func cleanEnvironment(t *testing.T, object *dummyDBObject, sess *mgo.Session) {
	t.Helper()

	object.invalidCollection = false
	dropCollection(sess, object, t)
	sess.Close()
}

func TestInsert(t *testing.T) {
	mgo, object, sess := prepareEnvironment(t)
	defer cleanEnvironment(t, object, sess)

	// insert the object into the database
	err := mgo.Insert(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was inserted
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.Country, result.Country)
	assert.Equal(t, object.Age, result.Age)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestDelete(t *testing.T) {
	mgo, object, sess := prepareEnvironment(t)
	defer cleanEnvironment(t, object, sess)

	// insert the object into the database
	err := mgo.Insert(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was inserted
	col := sess.DB("").C(object.TableName())

	var result dummyDBObject
	err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.Country, result.Country)
	assert.Equal(t, object.Age, result.Age)
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
	tests := []struct {
		name      string
		wantErr   bool
		newObject *dummyDBObject
		queries   []model.DBM
	}{
		{
			name: "update object",
			newObject: &dummyDBObject{
				Name:    "test2",
				Email:   "test2@test.com",
				Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
				Age:     20,
			},
		},
		{
			name: "update object with query",
			newObject: &dummyDBObject{
				Name:    "test",
				Email:   "test@test.com",
				Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
				Age:     100,
			},
			queries: []model.DBM{
				{"$set": model.DBM{"age": 100}},
			},
		},
		{
			name:      "multiple queries for one object",
			newObject: &dummyDBObject{},
			queries: []model.DBM{
				{
					"name": "test2",
				},
				{
					"email": "test@test.com",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgo, object, sess := prepareEnvironment(t)
			defer cleanEnvironment(t, object, sess)
			// insert the object into the database
			err := mgo.Insert(context.Background(), object)
			assert.Nil(t, err)

			tt.newObject.Id = object.GetObjectID()
			// updating the object
			err = mgo.Update(context.Background(), tt.newObject, tt.queries...)
			if tt.wantErr {
				assert.NotNil(t, err)
				return
			}
			assert.Nil(t, err)

			// check if the object was updated
			col := sess.DB("").C(object.TableName())

			var result dummyDBObject
			err = col.Find(bson.M{"_id": object.GetObjectID()}).One(&result)
			assert.Nil(t, err)

			assert.Equal(t, tt.newObject.Name, result.Name)
			assert.Equal(t, tt.newObject.Email, result.Email)
			assert.Equal(t, tt.newObject.Country, result.Country)
			assert.Equal(t, tt.newObject.Age, result.Age)
			assert.Equal(t, tt.newObject.GetObjectID(), result.GetObjectID())
		})
	}
}

func TestUpdateMany(t *testing.T) {
	tests := []struct {
		name            string
		wantErr         bool
		newObjects      []id.DBObject
		queries         []model.DBM
		expectedObjects []*dummyDBObject
	}{
		{
			name: "update object",
			newObjects: []id.DBObject{
				&dummyDBObject{
					Name:    "test2",
					Email:   "test2@test.com",
					Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
					Age:     20,
				},
			},
			expectedObjects: []*dummyDBObject{
				{
					Name:    "test2",
					Email:   "test2@test.com",
					Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
					Age:     20,
				},
			},
		},
		{
			name: "update object with query",
			newObjects: []id.DBObject{
				&dummyDBObject{},
			},
			queries: []model.DBM{
				{"$set": model.DBM{"age": 100}},
			},
			expectedObjects: []*dummyDBObject{
				{
					Name:    "test",
					Email:   "test@test.com",
					Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
					Age:     100,
				},
			},
		},
		{
			name:       "different length of objects and queries",
			newObjects: []id.DBObject{},
			queries: []model.DBM{
				{
					"name": "test2",
				},
				{
					"email": "test@test.com",
				},
			},
			wantErr: true,
		},
		{
			name: "multiple objects and multiple queries",
			newObjects: []id.DBObject{
				&dummyDBObject{},
				&dummyDBObject{},
			},
			queries: []model.DBM{
				{
					"$set": model.DBM{"name": "test1"},
				},
				{
					"$set": model.DBM{"name": "test2"},
				},
			},
			expectedObjects: []*dummyDBObject{
				{
					Name:    "test1",
					Email:   "test@test.com",
					Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
					Age:     10,
				},
				{
					Name:    "test2",
					Email:   "test@test.com",
					Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
					Age:     10,
				},
			},
		},
		{
			name:            "no rows provided",
			newObjects:      []id.DBObject{},
			queries:         []model.DBM{},
			expectedObjects: []*dummyDBObject{},
			wantErr:         true,
		},
		{
			name: "queries with different collections",
			newObjects: []id.DBObject{
				&dummyDBObject{},
				&dummyDBObject{},
			},
			queries: []model.DBM{
				{
					"$set": model.DBM{"name": "test1"},
				},
				{
					"$set":        model.DBM{"name": "test2"},
					"_collection": "invalid_collection",
				},
			},
			expectedObjects: []*dummyDBObject{
				{},
				{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgo, object, sess := prepareEnvironment(t)
			defer cleanEnvironment(t, object, sess)

			// insert the objects into the database
			ids := make([]id.ObjectId, len(tt.newObjects))

			assert.Equal(t, len(tt.newObjects), len(tt.expectedObjects))

			for i := range tt.newObjects {
				newId := id.NewObjectID()
				object.SetObjectID(newId)
				tt.expectedObjects[i].SetObjectID(newId)
				tt.newObjects[i].SetObjectID(newId)
				ids[i] = newId

				err := mgo.Insert(context.Background(), object)
				assert.Nil(t, err)
			}

			err := mgo.UpdateMany(context.Background(), tt.newObjects, tt.queries...)
			if tt.wantErr {
				assert.NotNil(t, err)
				return
			}
			assert.Nil(t, err)

			// check if the object was updated
			col := sess.DB("").C(object.TableName())
			for index, id := range ids {
				var result dummyDBObject
				err = col.Find(bson.M{"_id": id}).One(&result)
				assert.Nil(t, err)

				assert.Equal(t, tt.expectedObjects[index].Name, result.Name)
				assert.Equal(t, tt.expectedObjects[index].Email, result.Email)
				assert.Equal(t, tt.expectedObjects[index].Country, result.Country)
				assert.Equal(t, tt.expectedObjects[index].Age, result.Age)
				assert.Equal(t, tt.expectedObjects[index].GetObjectID(), result.GetObjectID())
			}
		})
	}

	t.Run("updating unexisting object", func(t *testing.T) {
		mgo, object, sess := prepareEnvironment(t)
		defer cleanEnvironment(t, object, sess)

		// insert the object into the database
		err := mgo.Insert(context.Background(), object)
		assert.Nil(t, err)

		// update a random object
		object.SetObjectID(id.NewObjectID())
		err = mgo.UpdateMany(context.Background(), []id.DBObject{object})
		assert.NotNil(t, err)
	})

	t.Run("updating unexisting objects", func(t *testing.T) {
		mgo, object, sess := prepareEnvironment(t)
		defer cleanEnvironment(t, object, sess)

		// insert the objects into the database
		objects := make([]id.DBObject, 3)

		for i := 0; i < 3; i++ {
			newId := id.NewObjectID()
			object.SetObjectID(newId)

			err := mgo.Insert(context.Background(), object)
			assert.Nil(t, err)

			object.SetObjectID(id.NewObjectID())
			objects[i] = object
		}

		// update a random object
		object.SetObjectID(id.NewObjectID())
		queries := []model.DBM{
			{}, {}, {},
		}
		err := mgo.UpdateMany(context.Background(), objects, queries...)
		fmt.Println("Error: ", err)
		assert.NotNil(t, err)
	})
}

func TestIsErrNoRows(t *testing.T) {
	mgoDriver := mgoDriver{}

	assert.True(t, mgoDriver.IsErrNoRows(mgo.ErrNotFound))
	assert.False(t, mgoDriver.IsErrNoRows(nil))
	assert.False(t, mgoDriver.IsErrNoRows(mgo.ErrCursor))
}

func TestCount(t *testing.T) {
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
			mgo, object, sess := prepareEnvironment(t)
			defer cleanEnvironment(t, object, sess)

			for i := 0; i < tt.want; i++ {
				object = &dummyDBObject{
					Name:  "test" + strconv.Itoa(i),
					Email: "test@test.com",
					Country: dummyCountryField{
						CountryName: "TestCountry" + strconv.Itoa(i),
						Continent:   "TestContinent",
					},
					Age: i,
				}

				err := mgo.Insert(context.Background(), object)
				assert.Nil(t, err)
			}

			object.invalidCollection = tt.wantErr

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

func TestQuery(t *testing.T) {
	type args struct {
		result interface{}
		query  model.DBM
	}

	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8},
		{Name: "Bob", Email: "bob@example.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45},
		{Name: "Peter", Email: "peter@test.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12},
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
			name: "filter by country name",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"country.country_name": "TestCountry",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[3]},
		},
		{
			name: "filter by country name and sort by name",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"country.country_name": "TestCountry",
					"_sort":                "name",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[3], dummyData[0]},
		},

		{
			name: "filter by id",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_id": dummyData[0].GetObjectID(),
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0]},
		},
		{
			name: "filter by slice of ids",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_id": model.DBM{
						"$in": []id.ObjectId{dummyData[0].GetObjectID(), dummyData[1].GetObjectID()},
					},
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[1]},
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
			mgo, object, sess := prepareEnvironment(t)
			defer cleanEnvironment(t, object, sess)

			for _, obj := range dummyData {
				err := mgo.Insert(context.Background(), &obj)
				assert.Nil(t, err)
			}

			object.invalidCollection = tt.wantErr

			if err := mgo.Query(context.Background(), object, tt.args.result, tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("mgoDriver.Query() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedResult, tt.args.result)
		})
	}
}

func TestDeleteWhere(t *testing.T) {
	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8},
		{Name: "Bob", Email: "bob@example.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45},
		{Name: "Peter", Email: "peter@test.com", Id: id.ObjectId(bson.NewObjectId()), Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12},
	}

	tests := []struct {
		name              string
		query             model.DBM
		expectedNewValues []dummyDBObject
		errorExpected     error
	}{
		{
			name:              "delete all",
			query:             model.DBM{},
			expectedNewValues: []dummyDBObject(nil),
		},
		{
			name: "delete by email ending with tyk.com",
			query: model.DBM{
				"email": model.DBM{
					"$regex": "tyk.com$",
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by name starting with A",
			query: model.DBM{
				"name": model.DBM{
					"$regex": "^A",
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by country name",
			query: model.DBM{
				"country.country_name": "TestCountry",
			},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by id",
			query: model.DBM{
				"_id": dummyData[0].GetObjectID(),
			},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age",
			query: model.DBM{
				"age": 10,
			},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age and country name",
			query: model.DBM{
				"age":                  10,
				"country.country_name": "TestCountry",
			},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j",
			query: model.DBM{
				"email": model.DBM{
					"$regex": "^j",
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j and age lower than 10",
			query: model.DBM{
				"email": model.DBM{
					"$regex": "^j",
				},
				"age": model.DBM{
					"$lt": 10,
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete invalid value",
			query: model.DBM{
				"email": model.DBM{
					"$regex": "^x",
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     mgo.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgo, object, sess := prepareEnvironment(t)
			defer cleanEnvironment(t, object, sess)

			for _, obj := range dummyData {
				err := mgo.Insert(context.Background(), &obj)
				assert.Nil(t, err)
			}

			err := mgo.DeleteWhere(context.Background(), object, tt.query)
			if err != nil {
				if tt.errorExpected != nil {
					assert.True(t, errors.Is(err, tt.errorExpected))
					return
				}
				t.Errorf("DeleteWhere() error = %v", err)
				return
			}

			var result []dummyDBObject
			err = mgo.Query(context.Background(), object, &result, model.DBM{})
			if err != nil {
				t.Errorf("Query() error = %v", err)
				return
			}

			if !reflect.DeepEqual(result, tt.expectedNewValues) {
				t.Errorf("Expected output %v, but got %v", tt.expectedNewValues, result)
			}
		})
	}
}

func TestHandleStoreError(t *testing.T) {
	mgo, _, _ := prepareEnvironment(t)

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
				err := invalidMgo.handleStoreError(test.inputErr)
				if err == nil {
					t.Errorf("expected error to be returned when mgo is nil")
				}
				return
			}

			gotErr := mgo.handleStoreError(test.inputErr)

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

func Test_getColName(t *testing.T) {
	type args struct {
		query model.DBM
		row   id.DBObject
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "get collection name from query",
			args: args{
				query: model.DBM{
					"_collection": "test",
				},
				row: nil,
			},
			want:    "test",
			wantErr: false,
		},
		{
			name: "get collection name from row",
			args: args{
				query: nil,
				row:   &dummyDBObject{},
			},
			want:    "dummy",
			wantErr: false,
		},
		{
			name: "no collection name",
			args: args{
				query: nil,
				row:   nil,
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getColName(tt.args.query, tt.args.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("getColName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getColName() = %v, want %v", got, tt.want)
			}
		})
	}
}
