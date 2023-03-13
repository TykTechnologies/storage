package mgo

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/TykTechnologies/storage/persistent/dbm"
	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/index"
	"github.com/TykTechnologies/storage/persistent/internal/helper"
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
func prepareEnvironment(t *testing.T) (*mgoDriver, *dummyDBObject) {
	t.Helper()
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&model.ClientOpts{
		ConnectionString: "mongodb://localhost:27017,localhost:27018,tyk-mongo:27019/test",
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

	return mgo, object
}

func cleanDB(t *testing.T) {
	t.Helper()
	d, _ := prepareEnvironment(t)
	helper.ErrPrint(d.session.DB("").DropDatabase())
}

func dropCollection(t *testing.T, driver *mgoDriver, object *dummyDBObject) {
	t.Helper()
	object.invalidCollection = false
	err := driver.Drop(context.Background(), object)
	// if the collection does not exist, avoid failing the test
	if err != nil && !strings.Contains(err.Error(), "ns not found") {
		t.Fatal(err)
	}
}

func TestInsert(t *testing.T) {
	defer cleanDB(t)
	driver, object := prepareEnvironment(t)
	defer dropCollection(t, driver, object)

	// insert the object into the database
	err := driver.Insert(context.Background(), object)
	assert.Nil(t, err)

	// check if the object was inserted

	var result dummyDBObject
	err = driver.Query(context.Background(), object, &result, dbm.DBM{"_id": object.GetObjectID()})
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.Country, result.Country)
	assert.Equal(t, object.Age, result.Age)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestDelete(t *testing.T) {
	defer cleanDB(t)

	driver, object := prepareEnvironment(t)
	ctx := context.Background()

	t.Run("deleting a existing object", func(t *testing.T) {
		// insert the object into the database
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		// delete the collection
		defer dropCollection(t, driver, object)

		// validates that the object was inserted
		var result dummyDBObject
		err = driver.Query(ctx, object, &result, dbm.DBM{"_id": object.GetObjectID()})
		assert.Nil(t, err)
		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())

		// delete the object from the database
		err = driver.Delete(ctx, object)
		assert.Nil(t, err)

		// check if the object was deleted
		err = driver.Query(ctx, object, &result, dbm.DBM{"_id": object.GetObjectID()})
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})

	t.Run("deleting a non existent object", func(t *testing.T) {
		// delete the object from the database
		err := driver.Delete(ctx, object)
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})
}

func TestUpdate(t *testing.T) {
	defer cleanDB(t)

	driver, object := prepareEnvironment(t)
	ctx := context.Background()
	defer dropCollection(t, driver, object)
	t.Run("Updating an existing obj", func(t *testing.T) {
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)

		object.Name = "test2"
		object.Email = "test2@test2.com"
		object.Age = 20
		err = driver.Update(ctx, object)
		assert.Nil(t, err)

		// check if the object was updated
		result := &dummyDBObject{}
		result.SetObjectID(object.GetObjectID())
		err = driver.Query(ctx, object, result, dbm.DBM{"_id": result.GetObjectID()})
		assert.Nil(t, err)

		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())
	})

	t.Run("Updating a non existing obj", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()

		object.SetObjectID(id.NewObjectID())

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})

	t.Run("Updating an object without _id", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.False(t, driver.IsErrNoRows(err))
	})
}

func TestBulkUpdate(t *testing.T) {
	defer cleanDB(t)

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10,
		},
		{
			Name: "Jane", Email: "jane@tyk.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8,
		},
		{
			Name: "Bob", Email: "bob@example.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45,
		},
		{
			Name: "Peter", Email: "peter@test.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12,
		},
	}

	tcs := []struct {
		testName          string
		query             []dbm.DBM
		givenObjects      []id.DBObject
		expectedNewValues []id.DBObject
		errorExpected     error
	}{
		{
			testName:          "update only one - without modifying values",
			givenObjects:      []id.DBObject{&dummyData[0]},
			expectedNewValues: []id.DBObject{&dummyData[0]},
			errorExpected:     mgo.ErrNotFound,
		},
		{
			testName: "update only one - modifying values",
			givenObjects: []id.DBObject{&dummyDBObject{
				Name: "Test", Email: "test@test.com", Id: dummyData[0].Id,
				Country: dummyData[0].Country, Age: dummyData[0].Age,
			}},
			expectedNewValues: []id.DBObject{&dummyDBObject{
				Name: "Test", Email: "test@test.com", Id: dummyData[0].Id,
				Country: dummyData[0].Country, Age: dummyData[0].Age,
			}},
		},
		{
			testName: "update two - without query",
			givenObjects: []id.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					Id:      dummyData[0].Id,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					Id:      dummyData[1].Id,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []id.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					Id:      dummyData[0].Id,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					Id:      dummyData[1].Id,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
		},
		{
			testName: "update two - filter with query",
			givenObjects: []id.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					Id:      dummyData[0].Id,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					Id:      dummyData[1].Id,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []id.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					Id:      dummyData[0].Id,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					Id:      dummyData[1].Id,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			query: []dbm.DBM{{"_id": dummyData[0].GetObjectID()}, {"name": "Jane"}},
		},
		{
			testName:      "update error - empty rows",
			givenObjects:  []id.DBObject{},
			errorExpected: errors.New(model.ErrorEmptyRow),
		},
		{
			testName: "update error - different params len",
			givenObjects: []id.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					Id:      dummyData[0].Id,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					Id:      dummyData[1].Id,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []id.DBObject{
				&dummyData[0],
				&dummyData[1],
			},
			query:         []dbm.DBM{{"testName": "Jane"}},
			errorExpected: errors.New(model.ErrorRowQueryDiffLenght),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			driver, object := prepareEnvironment(t)
			ctx := context.Background()
			defer dropCollection(t, driver, object)

			for _, obj := range dummyData {
				err := driver.Insert(ctx, &obj)
				assert.Nil(t, err)
			}

			err := driver.BulkUpdate(ctx, tc.givenObjects, tc.query...)
			assert.Equal(t, tc.errorExpected, err)

			var result []dummyDBObject
			err = driver.Query(context.Background(), object, &result, dbm.DBM{})
			assert.Nil(t, err)

			for i, expected := range tc.expectedNewValues {
				assert.EqualValues(t, expected, &result[i])
			}
		})
	}
}

func TestUpdateAll(t *testing.T) {
	defer cleanDB(t)

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     10,
		},
		{
			Name:  "Jane",
			Email: "jane@tyk.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"},
			Age:     8,
		},
		{
			Name:    "Bob",
			Email:   "bob@example.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"},
			Age:     25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     45,
		},
		{
			Name:    "Peter",
			Email:   "peter@test.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"},
			Age:     12,
		},
	}

	tcs := []struct {
		testName          string
		givenQuery        dbm.DBM
		givenUpdate       dbm.DBM
		givenObject       id.DBObject
		expectedNewValues func() []*dummyDBObject
		errorExpected     error
	}{
		{
			testName:    "unset all age",
			givenQuery:  dbm.DBM{},
			givenObject: &dummyDBObject{},
			givenUpdate: dbm.DBM{"$unset": dbm.DBM{"age": 0}},
			expectedNewValues: func() []*dummyDBObject {
				var newDummies []*dummyDBObject

				for i := range dummyData {
					dummy := dummyData[i]
					dummy.Age = 0

					newDummies = append(newDummies, &dummy)
				}

				return newDummies
			},
		},
		{
			testName:    "set all age to 50",
			givenQuery:  dbm.DBM{},
			givenObject: &dummyDBObject{},
			givenUpdate: dbm.DBM{"$set": dbm.DBM{"age": 50}},
			expectedNewValues: func() []*dummyDBObject {
				var newDummies []*dummyDBObject

				for i := range dummyData {
					dummy := dummyData[i]
					dummy.Age = 50

					newDummies = append(newDummies, &dummy)
				}

				return newDummies
			},
		},
		{
			testName: "increment age by those with tyk.com email by 10",
			givenQuery: dbm.DBM{
				"email": dbm.DBM{
					"$regex": "tyk.com$",
				},
			},
			givenObject: &dummyDBObject{},
			givenUpdate: dbm.DBM{"$inc": dbm.DBM{"age": 10}},
			expectedNewValues: func() []*dummyDBObject {
				var newDummies []*dummyDBObject

				for i := range dummyData {
					dummy := dummyData[i]
					newDummies = append(newDummies, &dummy)
				}

				newDummies[1].Age = 18
				newDummies[3].Age = 55
				return newDummies
			},
		},
		{
			testName: "set nested Country.CountryName value of John",
			givenQuery: dbm.DBM{
				"name": "John",
			},
			givenObject: &dummyDBObject{},
			givenUpdate: dbm.DBM{"$set": dbm.DBM{"country.country_name": "test"}},
			expectedNewValues: func() []*dummyDBObject {
				var newDummies []*dummyDBObject

				for i := range dummyData {
					dummy := dummyData[i]
					newDummies = append(newDummies, &dummy)
				}
				newDummies[0].Country = dummyCountryField{
					CountryName: "test",
					Continent:   "TestContinent",
				}
				return newDummies
			},
		},
		{
			testName: "no document query should return all the same",
			givenQuery: dbm.DBM{
				"random": "query",
			},
			givenObject:   &dummyDBObject{},
			errorExpected: mgo.ErrNotFound,
			givenUpdate:   dbm.DBM{"$set": dbm.DBM{"country.country_name": "test"}},
			expectedNewValues: func() []*dummyDBObject {
				var newDummies []*dummyDBObject

				for i := range dummyData {
					dummy := dummyData[i]
					newDummies = append(newDummies, &dummy)
				}
				return newDummies
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			driver, object := prepareEnvironment(t)
			ctx := context.Background()
			defer helper.ErrPrint(driver.Drop(ctx, object))

			for _, obj := range dummyData {
				err := driver.Insert(ctx, &obj)
				assert.Nil(t, err)
			}

			err := driver.UpdateAll(ctx, tc.givenObject, tc.givenQuery, tc.givenUpdate)
			assert.Equal(t, tc.errorExpected, err)

			var result []dummyDBObject
			err = driver.Query(ctx, tc.givenObject, &result, dbm.DBM{})
			assert.Nil(t, err)

			for i, expected := range tc.expectedNewValues() {
				assert.EqualValues(t, expected, &result[i])
			}
		})
	}
}

func TestIsErrNoRows(t *testing.T) {
	defer cleanDB(t)

	mgoDriver := mgoDriver{}

	assert.True(t, mgoDriver.IsErrNoRows(mgo.ErrNotFound))
	assert.False(t, mgoDriver.IsErrNoRows(nil))
	assert.False(t, mgoDriver.IsErrNoRows(mgo.ErrCursor))
}

func TestCount(t *testing.T) {
	defer cleanDB(t)

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     10,
		},
		{
			Name:  "Jane",
			Email: "jane@tyk.com", Id: id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"},
			Age:     8,
		},
		{
			Name:    "Bob",
			Email:   "bob@example.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"},
			Age:     25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     45,
		},
		{
			Name:    "Peter",
			Email:   "peter@test.com",
			Id:      id.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"},
			Age:     12,
		},
	}

	ctx := context.Background()

	tcs := []struct {
		name        string
		prepareTc   func(*testing.T) (*mgoDriver, *dummyDBObject)
		givenFilter []dbm.DBM
		want        int
		wantErr     error
	}{
		{
			name:      "0 objects",
			want:      0,
			prepareTc: prepareEnvironment,
		},
		{
			name: "1 object",
			want: 1,
			prepareTc: func(t *testing.T) (*mgoDriver, *dummyDBObject) {
				t.Helper()

				driver, object := prepareEnvironment(t)

				err := driver.Insert(ctx, object)
				assert.Nil(t, err)

				return driver, object
			},
		},
		{
			name: "5 objects",
			want: 5,
			prepareTc: func(t *testing.T) (*mgoDriver, *dummyDBObject) {
				t.Helper()

				driver, object := prepareEnvironment(t)

				for _, obj := range dummyData {
					err := driver.Insert(ctx, &obj)
					assert.Nil(t, err)
				}

				return driver, object
			},
		},
		{
			name:        "count with filter",
			want:        2,
			givenFilter: []dbm.DBM{{"country.country_name": "TestCountry"}},
			prepareTc: func(t *testing.T) (*mgoDriver, *dummyDBObject) {
				t.Helper()

				driver, object := prepareEnvironment(t)

				for _, obj := range dummyData {
					err := driver.Insert(ctx, &obj)
					assert.Nil(t, err)
				}

				return driver, object
			},
		},
		{
			name:        "count with filter, multiple options",
			want:        1,
			givenFilter: []dbm.DBM{{"country.country_name": "TestCountry", "email": "john@example.com"}},
			prepareTc: func(t *testing.T) (*mgoDriver, *dummyDBObject) {
				t.Helper()

				driver, object := prepareEnvironment(t)

				for _, obj := range dummyData {
					err := driver.Insert(ctx, &obj)
					assert.Nil(t, err)
				}

				return driver, object
			},
		},
		{
			name:        "count with multiple filters",
			want:        0,
			wantErr:     errors.New(model.ErrorMultipleDBM),
			givenFilter: []dbm.DBM{{"country.country_name": "TestCountry"}, {"testName": "test"}},
			prepareTc:   prepareEnvironment,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			driver, object := tc.prepareTc(t)
			defer cleanDB(t)

			got, err := driver.Count(ctx, object, tc.givenFilter...)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestQuery(t *testing.T) {
	defer cleanDB(t)

	type args struct {
		result interface{}
		query  dbm.DBM
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
				query:  nil,
			},
			expectedResult: &dummyData,
		},
		{
			name: "4 objects with limit 2",
			args: args{
				result: &[]dummyDBObject{},
				query: dbm.DBM{
					"_limit": 2,
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[1]},
		},
		{
			name: "4 objects with limit 2 and offset 2",
			args: args{
				result: &[]dummyDBObject{},
				query: dbm.DBM{
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
				query: dbm.DBM{
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
				query: dbm.DBM{
					"email": dbm.DBM{
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
				query: dbm.DBM{
					"email": dbm.DBM{
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
				query: dbm.DBM{
					"name": dbm.DBM{
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
				query: dbm.DBM{
					"name": dbm.DBM{
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
				query: dbm.DBM{
					"country.country_name": "TestCountry",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[3]},
		},
		{
			name: "filter by country name and sort by name",
			args: args{
				result: &[]dummyDBObject{},
				query: dbm.DBM{
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
				query: dbm.DBM{
					"_id": dummyData[0].GetObjectID(),
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0]},
		},
		{
			name: "filter by slice of ids",
			args: args{
				result: &[]dummyDBObject{},
				query: dbm.DBM{
					"_id": dbm.DBM{
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
				query:  dbm.DBM{},
			},
			wantErr:        true,
			expectedResult: &[]dummyDBObject{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, object := prepareEnvironment(t)
			ctx := context.Background()
			defer dropCollection(t, driver, object)

			for _, obj := range dummyData {
				err := driver.Insert(ctx, &obj)
				assert.Nil(t, err)
			}

			object.invalidCollection = tt.wantErr

			if err := driver.Query(ctx, object, tt.args.result, tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("mongoDriver.Query() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedResult, tt.args.result)
		})
	}
}

func TestDeleteWithQuery(t *testing.T) {
	defer cleanDB(t)

	driver, obj := prepareEnvironment(t)
	driver.Drop(context.Background(), obj)

	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8},
		{Name: "Bob", Email: "bob@example.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45},
		{Name: "Peter", Email: "peter@test.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12},
	}

	tests := []struct {
		name              string
		query             []dbm.DBM
		expectedNewValues []dummyDBObject
		errorExpected     error
	}{
		{
			name:              "empty query",
			query:             []dbm.DBM{},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     errors.New("not found"),
		},
		{
			name: "delete by email ending with tyk.com",
			query: []dbm.DBM{
				{
					"email": dbm.DBM{
						"$regex": "tyk.com$",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by name starting with A",
			query: []dbm.DBM{
				{
					"name": dbm.DBM{
						"$regex": "^A",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by country name",
			query: []dbm.DBM{{
				"country.country_name": "TestCountry",
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by id",
			query: []dbm.DBM{{
				"_id": dummyData[0].GetObjectID(),
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age",
			query: []dbm.DBM{{
				"age": 10,
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age and country name",
			query: []dbm.DBM{{
				"age":                  10,
				"country.country_name": "TestCountry",
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j",
			query: []dbm.DBM{
				{
					"email": dbm.DBM{
						"$regex": "^j",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j and age lower than 10",
			query: []dbm.DBM{{
				"email": dbm.DBM{
					"$regex": "^j",
				},
				"age": dbm.DBM{
					"$lt": 10,
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete invalid value",
			query: []dbm.DBM{{
				"email": dbm.DBM{
					"$regex": "^x",
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     mgo.ErrNotFound,
		},
		{
			name: "delete invalid value",
			query: []dbm.DBM{{
				"email": dbm.DBM{
					"$regex": "^x",
				},
			}, {
				"email": dbm.DBM{
					"$regex": "^x",
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     errors.New(model.ErrorMultipleQueryForSingleRow),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, object := prepareEnvironment(t)
			defer dropCollection(t, driver, object)

			ctx := context.Background()

			for _, obj := range dummyData {
				err := driver.Insert(ctx, &obj)
				assert.Nil(t, err)
			}

			object.SetObjectID(id.NewObjectID())
			err := driver.Delete(ctx, object, tt.query...)
			assert.Equal(t, tt.errorExpected, err)
			if tt.errorExpected == nil {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, tt.errorExpected, err)
			}

			var result []dummyDBObject
			err = driver.Query(ctx, object, &result, dbm.DBM{})
			assert.Nil(t, err)

			assert.EqualValues(t, tt.expectedNewValues, result)
		})
	}
}

func TestHandleStoreError(t *testing.T) {
	defer cleanDB(t)

	driver, _ := prepareEnvironment(t)

	tests := []struct {
		name     string
		inputErr error

		wantReconnect bool
	}{
		{
			name:     "Nil input error",
			inputErr: nil,
		},
		{
			name:          "Known connection error",
			inputErr:      errors.New("no reachable servers"),
			wantReconnect: true,
		},
		{
			name:     "Unknown connection error",
			inputErr: errors.New("unknown error"),
		},
		{
			name:          "i/o timeout",
			inputErr:      errors.New("i/o timeout"),
			wantReconnect: true,
		},
		{
			name:          "failing when reconnecting",
			inputErr:      errors.New("reset by peer"),
			wantReconnect: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sess := driver.session
			defer sess.Close()

			if test.inputErr != nil {
				invalidMgo := *driver
				invalidMgo.options = model.ClientOpts{
					ConnectionString:  "mongodb://host:port/invalid",
					ConnectionTimeout: 1,
				}
				err := invalidMgo.handleStoreError(test.inputErr)
				if err == nil {
					t.Errorf("expected error to be returned when driver is nil")
				}
				return
			}

			gotErr := driver.handleStoreError(test.inputErr)

			if test.wantReconnect {
				if sess == driver.session {
					t.Errorf("session was not reconnected when it should have been")
				}
			} else {
				if sess != driver.session {
					t.Errorf("session was reconnected when it shouldn't have been")
				}
			}

			if !errors.Is(gotErr, test.inputErr) {
				t.Errorf("got error %v, want error %v", gotErr, test.inputErr)
			}
		})
	}
}

func TestIndexes(t *testing.T) {
	defer cleanDB(t)

	tcs := []struct {
		testName          string
		givenIndex        index.Index
		expectedCreateErr error
		expectedIndexes   []index.Index
		expectedGetError  error
	}{
		{
			testName:          "no index case",
			givenIndex:        index.Index{},
			expectedCreateErr: errors.New(model.ErrorIndexEmpty),
		},
		{
			testName: "simple index case",
			givenIndex: index.Index{
				Name: "test",
				Keys: []dbm.DBM{{"apiid": 1}},
			},
			expectedIndexes: []index.Index{
				{
					Name: "_id_",
					Keys: []dbm.DBM{{"_id": int32(1)}},
				},
				{
					Name: "test",
					Keys: []dbm.DBM{{"apiid": int32(1)}},
				},
			},
		},
		{
			testName: "simple index without name",
			givenIndex: index.Index{
				Name: "",
				Keys: []dbm.DBM{{"apiid": 1}},
			},
			expectedIndexes: []index.Index{
				{
					Name: "_id_",
					Keys: []dbm.DBM{{"_id": int32(1)}},
				},
				{
					Name: "apiid_1",
					Keys: []dbm.DBM{{"apiid": int32(1)}},
				},
			},
		},
		{
			testName: "composed index case",
			givenIndex: index.Index{
				Name: "logBrowser",
				Keys: []dbm.DBM{{"timestamp": -1}, {"apiid": 1}, {"orgid": 1}},
			},
			expectedIndexes: []index.Index{
				{
					Name: "_id_",
					Keys: []dbm.DBM{{"_id": int32(1)}},
				},
				{
					Name: "logBrowser",
					Keys: []dbm.DBM{{"timestamp": int32(-1)}, {"apiid": int32(1)}, {"orgid": int32(1)}},
				},
			},
		},
		{
			testName: "simple index with TTL case",
			givenIndex: index.Index{
				Name:       "test",
				Keys:       []dbm.DBM{{"apiid": 1}},
				IsTTLIndex: true,
				TTL:        1,
			},
			expectedIndexes: []index.Index{
				{
					Name: "_id_",
					Keys: []dbm.DBM{{"_id": int32(1)}},
				},
				{
					Name:       "test",
					Keys:       []dbm.DBM{{"apiid": int32(1)}},
					TTL:        1,
					IsTTLIndex: true,
				},
			},
		},
		{
			testName: "compound index with TTL case",
			givenIndex: index.Index{
				Name:       "test",
				Keys:       []dbm.DBM{{"apiid": 1}, {"orgid": -1}},
				IsTTLIndex: true,
				TTL:        1,
			},
			expectedCreateErr: errors.New(model.ErrorIndexComposedTTL),
			expectedIndexes:   []index.Index{},
		},
		{
			// cover https://www.mongodb.com/docs/drivers/go/v1.8/fundamentals/indexes/#geospatial-indexes
			testName: "compound case with string value",
			givenIndex: index.Index{
				Name: "test",
				Keys: []dbm.DBM{{"location.geo": "2dsphere"}},
			},
			expectedIndexes: []index.Index{
				{
					Name: "_id_",
					Keys: []dbm.DBM{{"_id": int32(1)}},
				},
				{
					Name: "test",
					Keys: []dbm.DBM{{"location.geo": "2dsphere"}},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.testName, func(t *testing.T) {
			ctx := context.Background()
			driver, obj := prepareEnvironment(t)
			defer helper.ErrPrint(driver.Drop(ctx, obj))

			err := driver.CreateIndex(context.Background(), obj, tc.givenIndex)
			assert.Equal(t, tc.expectedCreateErr, err)
			if err != nil {
				return
			}

			actualIndexes, err := driver.GetIndexes(context.Background(), obj)
			assert.Equal(t, tc.expectedCreateErr, err)

			assert.Len(t, actualIndexes, len(tc.expectedIndexes))
			assert.EqualValues(t, tc.expectedIndexes, actualIndexes)
		})
	}
}

func TestPing(t *testing.T) {
	defer cleanDB(t)

	t.Run("ping ok", func(t *testing.T) {
		driver, _ := prepareEnvironment(t)
		err := driver.Ping(context.Background())
		assert.Nil(t, err)
	})
	t.Run("ping sess closed", func(t *testing.T) {
		driver, _ := prepareEnvironment(t)
		driver.Close()
		err := driver.Ping(context.Background())
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(model.ErrorSessionClosed), err)
	})
	t.Run("ping internal sess closed", func(t *testing.T) {
		driver, _ := prepareEnvironment(t)
		driver.session.Close()
		err := driver.Ping(context.Background())
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(model.ErrorSessionClosed+" from panic"), err)
	})
}

func TestHasTable(t *testing.T) {
	defer cleanDB(t)

	t.Run("HasTable sess closed", func(t *testing.T) {
		driver, _ := prepareEnvironment(t)
		driver.Close()
		// Test when session is nil
		result, err := driver.HasTable(context.Background(), "dummy")
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(model.ErrorSessionClosed), err)
		assert.False(t, result)
	})
	t.Run("HasTable internat sess closed", func(t *testing.T) {
		// Test when session is closed
		driver, _ := prepareEnvironment(t)
		driver.session.Close() // mock a closed session
		result, err := driver.HasTable(context.Background(), "dummy")
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(model.ErrorSessionClosed+" from panic"), err)
		assert.False(t, result)
	})

	t.Run("HasTable ok", func(t *testing.T) {
		// Test when collection exists
		driver, object := prepareEnvironment(t)
		err := driver.Insert(context.Background(), object)
		assert.Nil(t, err)
		defer dropCollection(t, driver, object)
		result, err := driver.HasTable(context.Background(), "dummy")
		assert.Nil(t, err)
		assert.True(t, result)
	})

	t.Run("HasTable when collection does not exist", func(t *testing.T) {
		// Test when collection does not exist
		driver, _ := prepareEnvironment(t)
		result, err := driver.HasTable(context.Background(), "dummy")
		assert.Nil(t, err)
		assert.False(t, result)
	})
}

func TestAutoMigrate(t *testing.T) {
	t.Run("AutoMigrate 1 object with no opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		defer dropCollection(t, driver, obj)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []id.DBObject{obj}

		err = driver.AutoMigrate(context.Background(), objs)
		assert.Nil(t, err)

		cols, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		assert.Len(t, cols, 1)
		assert.Equal(t, "dummy", cols[0])
	})

	t.Run("AutoMigrate 1 object with opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		defer dropCollection(t, driver, obj)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []id.DBObject{obj}
		opt := dbm.DBM{
			"capped":   true,
			"maxBytes": 1234,
		}

		err = driver.AutoMigrate(context.Background(), objs, opt)
		assert.Nil(t, err)

		cols, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		assert.Len(t, cols, 1)
		assert.Equal(t, "dummy", cols[0])

		stats := bson.M{}
		err = driver.db.Run(bson.M{"collStats": "dummy"}, &stats)
		assert.Nil(t, err)

		assert.True(t, stats["capped"].(bool))
	})

	t.Run("AutoMigrate 1 object with multiple opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []id.DBObject{obj}
		opt := dbm.DBM{
			"capped":   true,
			"maxBytes": 1234,
		}
		opt2 := dbm.DBM{
			"size": 1234,
		}

		err = driver.AutoMigrate(context.Background(), objs, opt, opt2)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), model.ErrorRowOptDiffLenght)
	})
}
