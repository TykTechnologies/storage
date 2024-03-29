//go:build mongo4.4 || mongo4.2 || mongo4.0 || mongo3.6 || mongo3.4 || mongo3.2 || mongo3.0 || mongo2.6
// +build mongo4.4 mongo4.2 mongo4.0 mongo3.6 mongo3.4 mongo3.2 mongo3.0 mongo2.6

package mgo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/TykTechnologies/storage/persistent/utils"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/TykTechnologies/storage/persistent/internal/helper"
	"github.com/TykTechnologies/storage/persistent/internal/types"
)

type dummyDBObject struct {
	ID                model.ObjectID    `bson:"_id,omitempty"`
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

func (d *dummyDBObject) GetObjectID() model.ObjectID {
	return d.ID
}

func (d *dummyDBObject) SetObjectID(id model.ObjectID) {
	d.ID = id
}

func (d *dummyDBObject) TableName() string {
	if d.invalidCollection {
		return ""
	}
	return "dummy"
}

// prepareEnvironment returns a new mgo driver connection and a dummy object to test
func prepareEnvironment(t *testing.T) (*mgoDriver, *dummyDBObject) {
	t.Helper()
	// create a new mgo driver connection
	mgo, err := NewMgoDriver(&types.ClientOpts{
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
	helper.ErrPrint(d.DropDatabase(context.Background()))
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

	ctx := context.Background()

	t.Run("inserting one object", func(t *testing.T) {
		defer dropCollection(t, driver, object)
		// insert the object into the database
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)

		// check if the object was inserted

		var result dummyDBObject
		err = driver.Query(context.Background(), object, &result, model.DBM{"_id": object.GetObjectID()})
		assert.Nil(t, err)

		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.Country, result.Country)
		assert.Equal(t, object.Age, result.Age)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())
	})
	t.Run("inserting multiple objects", func(t *testing.T) {
		objects := []model.DBObject{}

		for i := 0; i < 3; i++ {
			objects = append(objects, &dummyDBObject{
				Name:  "test" + strconv.Itoa(i),
				Email: "email@email.com",
				ID:    model.NewObjectID(),
			})
		}

		// insert the objects into the database
		err := driver.Insert(ctx, objects...)
		assert.Nil(t, err)
		// delete the collection
		defer dropCollection(t, driver, object)

		// check if the objects were inserted
		var result []dummyDBObject
		err = driver.Query(ctx, object, &result, model.DBM{})
		assert.Nil(t, err)
		assert.Len(t, result, len(objects))
		for i, obj := range objects {
			assert.Equal(t, obj.GetObjectID(), result[i].GetObjectID())
		}
	})

	t.Run("inserting 0 objects", func(t *testing.T) {
		err := driver.Insert(ctx)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), types.ErrorEmptyRow)
	})
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
		err = driver.Query(ctx, object, &result, model.DBM{"_id": object.GetObjectID()})
		assert.Nil(t, err)
		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())

		// delete the object from the database
		err = driver.Delete(ctx, object)
		assert.Nil(t, err)

		// check if the object was deleted
		err = driver.Query(ctx, object, &result, model.DBM{"_id": object.GetObjectID()})
		assert.NotNil(t, err)
		assert.True(t, utils.IsErrNoRows(err))
	})

	t.Run("deleting a non existent object", func(t *testing.T) {
		// delete the object from the database
		err := driver.Delete(ctx, object)
		assert.NotNil(t, err)
		assert.True(t, utils.IsErrNoRows(err))
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
		err = driver.Query(ctx, object, result, model.DBM{"_id": result.GetObjectID()})
		assert.Nil(t, err)

		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())
	})

	t.Run("Updating a non existing obj", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()

		object.SetObjectID(model.NewObjectID())

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.True(t, utils.IsErrNoRows(err))
	})

	t.Run("Updating an object without _id", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.False(t, utils.IsErrNoRows(err))
	})
}

func TestBulkUpdate(t *testing.T) {
	defer cleanDB(t)

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10,
		},
		{
			Name: "Jane", Email: "jane@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8,
		},
		{
			Name: "Bob", Email: "bob@example.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45,
		},
		{
			Name: "Peter", Email: "peter@test.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12,
		},
	}

	tcs := []struct {
		testName          string
		query             []model.DBM
		givenObjects      []model.DBObject
		expectedNewValues []model.DBObject
		errorExpected     error
	}{
		{
			testName:          "update only one - without modifying values",
			givenObjects:      []model.DBObject{&dummyData[0]},
			expectedNewValues: []model.DBObject{&dummyData[0]},
			errorExpected:     mgo.ErrNotFound,
		},
		{
			testName: "update only one - modifying values",
			givenObjects: []model.DBObject{&dummyDBObject{
				Name: "Test", Email: "test@test.com", ID: dummyData[0].ID,
				Country: dummyData[0].Country, Age: dummyData[0].Age,
			}},
			expectedNewValues: []model.DBObject{&dummyDBObject{
				Name: "Test", Email: "test@test.com", ID: dummyData[0].ID,
				Country: dummyData[0].Country, Age: dummyData[0].Age,
			}},
		},
		{
			testName: "update two - without query",
			givenObjects: []model.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					ID:      dummyData[0].ID,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					ID:      dummyData[1].ID,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []model.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					ID:      dummyData[0].ID,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					ID:      dummyData[1].ID,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
		},
		{
			testName: "update two - filter with query",
			givenObjects: []model.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					ID:      dummyData[0].ID,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					ID:      dummyData[1].ID,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []model.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					ID:      dummyData[0].ID,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					ID:      dummyData[1].ID,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			query: []model.DBM{{"_id": dummyData[0].GetObjectID()}, {"name": "Jane"}},
		},
		{
			testName:      "update error - empty rows",
			givenObjects:  []model.DBObject{},
			errorExpected: errors.New(types.ErrorEmptyRow),
		},
		{
			testName: "update error - different params len",
			givenObjects: []model.DBObject{
				&dummyDBObject{
					Name:    "Test",
					Email:   "test@test.com",
					ID:      dummyData[0].ID,
					Country: dummyData[0].Country,
					Age:     dummyData[0].Age,
				},
				&dummyDBObject{
					Name:    "Testina",
					Email:   "test@test.com",
					ID:      dummyData[1].ID,
					Country: dummyData[1].Country,
					Age:     dummyData[1].Age,
				},
			},
			expectedNewValues: []model.DBObject{
				&dummyData[0],
				&dummyData[1],
			},
			query:         []model.DBM{{"testName": "Jane"}},
			errorExpected: errors.New(types.ErrorRowQueryDiffLenght),
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
			err = driver.Query(context.Background(), object, &result, model.DBM{})
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
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     10,
		},
		{
			Name:  "Jane",
			Email: "jane@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"},
			Age:     8,
		},
		{
			Name:    "Bob",
			Email:   "bob@example.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"},
			Age:     25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     45,
		},
		{
			Name:    "Peter",
			Email:   "peter@test.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"},
			Age:     12,
		},
	}

	tcs := []struct {
		testName          string
		givenQuery        model.DBM
		givenUpdate       model.DBM
		givenObject       model.DBObject
		expectedNewValues func() []*dummyDBObject
		errorExpected     error
	}{
		{
			testName:    "unset all age",
			givenQuery:  model.DBM{},
			givenObject: &dummyDBObject{},
			givenUpdate: model.DBM{"$unset": model.DBM{"age": 0}},
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
			givenQuery:  model.DBM{},
			givenObject: &dummyDBObject{},
			givenUpdate: model.DBM{"$set": model.DBM{"age": 50}},
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
			givenQuery: model.DBM{
				"email": model.DBM{
					"$regex": "tyk.com$",
				},
			},
			givenObject: &dummyDBObject{},
			givenUpdate: model.DBM{"$inc": model.DBM{"age": 10}},
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
			givenQuery: model.DBM{
				"name": "John",
			},
			givenObject: &dummyDBObject{},
			givenUpdate: model.DBM{"$set": model.DBM{"country.country_name": "test"}},
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
			givenQuery: model.DBM{
				"random": "query",
			},
			givenObject:   &dummyDBObject{},
			errorExpected: mgo.ErrNotFound,
			givenUpdate:   model.DBM{"$set": model.DBM{"country.country_name": "test"}},
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
			err = driver.Query(ctx, tc.givenObject, &result, model.DBM{})
			assert.Nil(t, err)

			for i, expected := range tc.expectedNewValues() {
				assert.EqualValues(t, expected, &result[i])
			}
		})
	}
}

func TestCount(t *testing.T) {
	defer cleanDB(t)

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     10,
		},
		{
			Name:  "Jane",
			Email: "jane@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"},
			Age:     8,
		},
		{
			Name:    "Bob",
			Email:   "bob@example.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"},
			Age:     25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"},
			Age:     45,
		},
		{
			Name:    "Peter",
			Email:   "peter@test.com",
			ID:      model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"},
			Age:     12,
		},
	}

	ctx := context.Background()

	tcs := []struct {
		name        string
		prepareTc   func(*testing.T) (*mgoDriver, *dummyDBObject)
		givenFilter []model.DBM
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
			givenFilter: []model.DBM{{"country.country_name": "TestCountry"}},
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
			givenFilter: []model.DBM{{"country.country_name": "TestCountry", "email": "john@example.com"}},
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
			wantErr:     errors.New(types.ErrorMultipleDBM),
			givenFilter: []model.DBM{{"country.country_name": "TestCountry"}, {"testName": "test"}},
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
		query  model.DBM
	}

	dummyData := []dummyDBObject{
		{
			Name: "John", Email: "john@example.com", ID: model.ObjectID(bson.NewObjectId()),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10,
		},
		{
			Name: "Jane", Email: "jane@tyk.com", ID: model.ObjectID(bson.NewObjectId()),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8,
		},
		{
			Name: "Bob", Email: "bob@example.com", ID: model.ObjectID(bson.NewObjectId()),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com", ID: model.ObjectID(bson.NewObjectId()),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45,
		},
		{
			Name: "Peter", Email: "peter@test.com", ID: model.ObjectID(bson.NewObjectId()),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12,
		},
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
						"$in": []model.ObjectID{dummyData[0].GetObjectID(), dummyData[1].GetObjectID()},
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
		{
			Name: "John", Email: "john@example.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10,
		},
		{
			Name: "Jane", Email: "jane@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8,
		},
		{
			Name: "Bob", Email: "bob@example.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25,
		},
		{
			Name: "Alice", Email: "alice@tyk.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45,
		},
		{
			Name: "Peter", Email: "peter@test.com", ID: model.NewObjectID(),
			Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12,
		},
	}

	tests := []struct {
		name              string
		query             []model.DBM
		expectedNewValues []dummyDBObject
		errorExpected     error
	}{
		{
			name:              "empty query",
			query:             []model.DBM{},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     errors.New("not found"),
		},
		{
			name: "delete by email ending with tyk.com",
			query: []model.DBM{
				{
					"email": model.DBM{
						"$regex": "tyk.com$",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by name starting with A",
			query: []model.DBM{
				{
					"name": model.DBM{
						"$regex": "^A",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by country name",
			query: []model.DBM{{
				"country.country_name": "TestCountry",
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[4]},
		},
		{
			name: "delete by id",
			query: []model.DBM{{
				"_id": dummyData[0].GetObjectID(),
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age",
			query: []model.DBM{{
				"age": 10,
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by age and country name",
			query: []model.DBM{{
				"age":                  10,
				"country.country_name": "TestCountry",
			}},
			expectedNewValues: []dummyDBObject{dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j",
			query: []model.DBM{
				{
					"email": model.DBM{
						"$regex": "^j",
					},
				},
			},
			expectedNewValues: []dummyDBObject{dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete by emails starting with j and age lower than 10",
			query: []model.DBM{{
				"email": model.DBM{
					"$regex": "^j",
				},
				"age": model.DBM{
					"$lt": 10,
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[2], dummyData[3], dummyData[4]},
		},
		{
			name: "delete invalid value",
			query: []model.DBM{{
				"email": model.DBM{
					"$regex": "^x",
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     mgo.ErrNotFound,
		},
		{
			name: "delete invalid value",
			query: []model.DBM{{
				"email": model.DBM{
					"$regex": "^x",
				},
			}, {
				"email": model.DBM{
					"$regex": "^x",
				},
			}},
			expectedNewValues: []dummyDBObject{dummyData[0], dummyData[1], dummyData[2], dummyData[3], dummyData[4]},
			errorExpected:     errors.New(types.ErrorMultipleQueryForSingleRow),
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

			object.SetObjectID(model.NewObjectID())
			err := driver.Delete(ctx, object, tt.query...)
			assert.Equal(t, tt.errorExpected, err)
			if tt.errorExpected == nil {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, tt.errorExpected, err)
			}

			var result []dummyDBObject
			err = driver.Query(ctx, object, &result, model.DBM{})
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
				invalidMgo.options = types.ClientOpts{
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
		givenIndex        model.Index
		expectedCreateErr error
		expectedIndexes   []model.Index
		expectedGetError  error
	}{
		{
			testName:          "no index case",
			givenIndex:        model.Index{},
			expectedCreateErr: errors.New(types.ErrorIndexEmpty),
			expectedGetError:  errors.New(types.ErrorCollectionNotFound),
		},
		{
			testName: "simple index case",
			givenIndex: model.Index{
				Name: "test",
				Keys: []model.DBM{{"apiid": 1}},
			},
			expectedIndexes: []model.Index{
				{
					Name: "_id_",
					Keys: []model.DBM{{"_id": int32(1)}},
				},
				{
					Name: "test",
					Keys: []model.DBM{{"apiid": int32(1)}},
				},
			},
		},
		{
			testName: "simple index without name",
			givenIndex: model.Index{
				Name: "",
				Keys: []model.DBM{{"apiid": 1}},
			},
			expectedIndexes: []model.Index{
				{
					Name: "_id_",
					Keys: []model.DBM{{"_id": int32(1)}},
				},
				{
					Name: "apiid_1",
					Keys: []model.DBM{{"apiid": int32(1)}},
				},
			},
		},
		{
			testName: "composed index case",
			givenIndex: model.Index{
				Name: "logBrowser",
				Keys: []model.DBM{{"timestamp": -1}, {"apiid": 1}, {"orgid": 1}},
			},
			expectedIndexes: []model.Index{
				{
					Name: "_id_",
					Keys: []model.DBM{{"_id": int32(1)}},
				},
				{
					Name: "logBrowser",
					Keys: []model.DBM{{"timestamp": int32(-1)}, {"apiid": int32(1)}, {"orgid": int32(1)}},
				},
			},
		},
		{
			testName: "simple index with TTL case",
			givenIndex: model.Index{
				Name:       "test",
				Keys:       []model.DBM{{"apiid": 1}},
				IsTTLIndex: true,
				TTL:        1,
			},
			expectedIndexes: []model.Index{
				{
					Name: "_id_",
					Keys: []model.DBM{{"_id": int32(1)}},
				},
				{
					Name:       "test",
					Keys:       []model.DBM{{"apiid": int32(1)}},
					TTL:        1,
					IsTTLIndex: true,
				},
			},
		},
		{
			testName: "compound index with TTL case",
			givenIndex: model.Index{
				Name:       "test",
				Keys:       []model.DBM{{"apiid": 1}, {"orgid": -1}},
				IsTTLIndex: true,
				TTL:        1,
			},
			expectedCreateErr: errors.New(types.ErrorIndexComposedTTL),
			expectedGetError:  errors.New(types.ErrorCollectionNotFound),
		},
		{
			// cover https://www.mongodb.com/docs/drivers/go/v1.8/fundamentals/indexes/#geospatial-indexes
			testName: "compound case with string value",
			givenIndex: model.Index{
				Name: "test",
				Keys: []model.DBM{{"location.geo": "2dsphere"}},
			},
			expectedIndexes: []model.Index{
				{
					Name: "_id_",
					Keys: []model.DBM{{"_id": int32(1)}},
				},
				{
					Name: "test",
					Keys: []model.DBM{{"location.geo": "2dsphere"}},
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

			actualIndexes, err := driver.GetIndexes(context.Background(), obj)
			assert.Equal(t, tc.expectedGetError, err)

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
		assert.Equal(t, errors.New(types.ErrorSessionClosed), err)
	})
	t.Run("ping internal sess closed", func(t *testing.T) {
		driver, _ := prepareEnvironment(t)
		driver.session.Close()
		err := driver.Ping(context.Background())
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(types.ErrorSessionClosed+" from panic"), err)
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
		assert.Equal(t, errors.New(types.ErrorSessionClosed), err)
		assert.False(t, result)
	})
	t.Run("HasTable internat sess closed", func(t *testing.T) {
		// Test when session is closed
		driver, _ := prepareEnvironment(t)
		driver.session.Close() // mock a closed session
		result, err := driver.HasTable(context.Background(), "dummy")
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(types.ErrorSessionClosed+" from panic"), err)
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

func TestMigrate(t *testing.T) {
	t.Run("Migrate 1 object with no opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		defer dropCollection(t, driver, obj)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []model.DBObject{obj}

		err = driver.Migrate(context.Background(), objs)
		assert.Nil(t, err)

		cols, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		assert.Len(t, cols, 1)
		assert.Equal(t, "dummy", cols[0])
	})

	t.Run("Migrate 1 object with opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		defer dropCollection(t, driver, obj)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []model.DBObject{obj}
		opt := model.DBM{
			"capped":   true,
			"maxBytes": 1234,
		}

		err = driver.Migrate(context.Background(), objs, opt)
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

	t.Run("Migrate 1 object with multiple opts", func(t *testing.T) {
		driver, obj := prepareEnvironment(t)
		colNames, err := driver.db.CollectionNames()
		assert.Nil(t, err)

		for _, colName := range colNames {
			err := driver.db.C(colName).DropCollection()
			assert.Nil(t, err)
		}

		objs := []model.DBObject{obj}
		opt := model.DBM{
			"capped":   true,
			"maxBytes": 1234,
		}
		opt2 := model.DBM{
			"maxBytes": 1234,
		}

		err = driver.Migrate(context.Background(), objs, opt, opt2)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), types.ErrorRowOptDiffLenght)
	})
}

func TestDropDatabase(t *testing.T) {
	defer cleanDB(t)
	driver, object := prepareEnvironment(t)

	initialDatabases, err := driver.session.DatabaseNames()
	if err != nil {
		t.Fatal(err)
	}

	initialDatabaseCount := len(initialDatabases)
	// insert object so we force the database-collection creation
	err = driver.Insert(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}

	err = driver.DropDatabase(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	databases, err := driver.session.DatabaseNames()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, initialDatabaseCount, len(databases))
}

func TestDBTableStats(t *testing.T) {
	ctx := context.Background()
	driver, object := prepareEnvironment(t)
	tests := []struct {
		name        string
		want        model.DBM
		row         func() model.DBObject
		expectedErr error
	}{
		{
			name: "DBTableStats ok",
			want: model.DBM{
				"count":          0,
				"indexDetails":   model.DBM{},
				"indexSizes":     model.DBM{},
				"nindexes":       0,
				"ns":             "test.dummy",
				"ok":             float64(1),
				"scaleFactor":    1,
				"size":           0,
				"storageSize":    0,
				"totalIndexSize": 0,
				"totalSize":      0,
			},
			row:         func() model.DBObject { return object },
			expectedErr: nil,
		},
		{
			name: "DBTableStats error",
			want: model.DBM{
				"code":     73,
				"errmsg":   "Invalid namespace specified 'test.'",
				"ok":       float64(0),
				"codeName": "InvalidNamespace",
			},
			row: func() model.DBObject {
				return &dummyDBObject{
					ID:                model.NewObjectID(),
					invalidCollection: true,
				}
			},
			expectedErr: errors.New("Invalid namespace specified 'test.'"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanDB(t)
			row := tt.row()
			got, err := driver.DBTableStats(ctx, row)
			if (err != nil) != (tt.expectedErr != nil) {
				t.Errorf("mgoDriver.DBTableStats() error = %v, expectedErr %v", err, tt.expectedErr)
				return
			}

			if tt.expectedErr != nil {
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
				assert.Equal(t, tt.want["code"], got["code"])
				assert.Equal(t, tt.want["errmsg"], got["errmsg"])
				assert.Equal(t, tt.want["ok"], got["ok"])
				assert.Equal(t, tt.want["codeName"], got["codeName"])
				return
			}

			assert.Equal(t, tt.want["count"], got["count"])
			assert.Equal(t, tt.want["indexDetails"], got["indexDetails"])
			assert.Equal(t, tt.want["indexSizes"], got["indexSizes"])
			assert.Equal(t, tt.want["nindexes"], got["nindexes"])
			assert.Equal(t, tt.want["ns"], got["ns"])
			assert.Equal(t, tt.want["ok"], got["ok"])
			assert.Equal(t, tt.want["scaleFactor"], got["scaleFactor"])
			assert.Equal(t, tt.want["size"], got["size"])
			assert.Equal(t, tt.want["storageSize"], got["storageSize"])
			assert.Equal(t, tt.want["totalIndexSize"], got["totalIndexSize"])
		})
	}

	t.Run("DBTableStats with 1 object", func(t *testing.T) {
		defer cleanDB(t)
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)

		stats, err := driver.DBTableStats(ctx, object)
		assert.Nil(t, err)

		assert.Equal(t, 1, stats["count"])
		assert.Equal(t, 1, stats["nindexes"]) // must be 1 because of _id index
	})

	t.Run("DBTableStats with 3 indexes", func(t *testing.T) {
		defer cleanDB(t)
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		err = driver.CreateIndex(ctx, object, model.Index{
			Keys: []model.DBM{{"index1": 1}},
		})
		assert.Nil(t, err)

		err = driver.CreateIndex(ctx, object, model.Index{
			Keys: []model.DBM{{"index2": 1}},
		})
		assert.Nil(t, err)

		err = driver.CreateIndex(ctx, object, model.Index{
			Keys: []model.DBM{{"index3": 1}},
		})
		assert.Nil(t, err)

		stats, err := driver.DBTableStats(ctx, object)
		assert.Nil(t, err)

		assert.Equal(t, 1, stats["count"])
		assert.Equal(t, 4, stats["nindexes"])
	},
	)

	t.Run("DBTableStats with capped collection", func(t *testing.T) {
		defer cleanDB(t)
		opts := model.DBM{
			"capped":   true,
			"maxBytes": 9000,
		}

		err := driver.Migrate(ctx, []model.DBObject{object}, opts)
		assert.Nil(t, err)

		stats, err := driver.DBTableStats(ctx, object)
		assert.Nil(t, err)

		assert.Equal(t, true, stats["capped"])
	},
	)
}

type SalesExample struct {
	ID    model.ObjectID `bson:"_id,omitempty"`
	Items []Items        `bson:"items"`
}

type Items struct {
	Name     string        `bson:"name"`
	Tags     []interface{} `bson:"tags"`
	Price    float64       `bson:"price"`
	Quantity int           `bson:"quantity"`
}

func (SalesExample) TableName() string {
	d := dummyDBObject{}
	return d.TableName()
}

func (s *SalesExample) SetObjectID(id model.ObjectID) {
	s.ID = id
}

func (s SalesExample) GetObjectID() model.ObjectID {
	return s.ID
}

func TestAggregate(t *testing.T) {
	defer cleanDB(t)
	driver, object := prepareEnvironment(t)
	object.SetObjectID(model.NewObjectID())

	// Insert the object into the database
	ctx := context.Background()
	err := driver.Insert(ctx, object)
	assert.Nil(t, err)

	// Insert the object2 into the database
	object2 := &dummyDBObject{
		Name:  "Peter",
		Email: "peter@email.com",
		Country: dummyCountryField{
			Continent:   "Europe",
			CountryName: "Germany",
		},
		Age: 15,
	}
	err = driver.Insert(ctx, object2)
	assert.Nil(t, err)

	// Define an array of test cases
	tests := []struct {
		name           string
		pipeline       []model.DBM
		expectedResult []model.DBM
	}{
		{
			name: "aggregating one object",
			pipeline: []model.DBM{
				{
					"$match": model.DBM{"_id": object.GetObjectID()},
				},
			},
			expectedResult: []model.DBM{{
				"_id":   object.GetObjectID(),
				"name":  object.Name,
				"email": object.Email,
				"country": model.DBM{
					"continent":    object.Country.Continent,
					"country_name": object.Country.CountryName,
				},
				"age": object.Age,
			}},
		},
		{
			name: "aggregating objects with $project, $sort and $limit",
			pipeline: []model.DBM{
				{
					"$project": model.DBM{
						"_id":     1,
						"name":    1,
						"country": 1,
						"age":     1,
					},
				},
				{
					"$sort": model.DBM{"name": 1},
				},
				{
					"$limit": 1,
				},
			},
			expectedResult: []model.DBM{
				{
					"_id":  object2.GetObjectID(),
					"name": object2.Name,
					"country": model.DBM{
						"continent":    object2.Country.Continent,
						"country_name": object2.Country.CountryName,
					},
					"age": object2.Age,
				},
			},
		},
	}

	// Run each test case
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Execute the aggregation pipeline
			result, err := driver.Aggregate(ctx, object, tc.pipeline)
			assert.Nil(t, err)

			// Check if the result matches the expected result
			assert.ElementsMatch(t, tc.expectedResult, result)
		})
	}

	t.Run("2 $unwind and 1 $group - follow mongodb documentation example", func(t *testing.T) {
		// This test case is based on the example from the mongodb documentation:
		// https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/#unwind-embedded-arrays
		defer cleanDB(t)
		// Let's create a 2 "sales" objects
		sales1 := &SalesExample{
			ID: model.NewObjectID(),
			Items: []Items{
				{
					Name:     "abc",
					Tags:     []interface{}{"red", "blank"},
					Price:    12.99,
					Quantity: 10,
				},
				{
					Name:     "jkl",
					Tags:     []interface{}{"green", "rough"},
					Price:    14.99,
					Quantity: 20,
				},
			},
		}
		sales2 := &SalesExample{
			ID: model.NewObjectID(),
			Items: []Items{
				{
					Name:     "xyz",
					Tags:     []interface{}{"blue", "diamond"},
					Price:    9.99,
					Quantity: 5,
				},
				{
					Name:     "ijk",
					Tags:     []interface{}{"black", "glossy"},
					Price:    7.99,
					Quantity: 5,
				},
			},
		}

		// Insert the objects into the database
		err := driver.Insert(ctx, sales1)
		assert.Nil(t, err)
		err = driver.Insert(ctx, sales2)
		assert.Nil(t, err)

		// Define the aggregation pipeline
		pipeline := []model.DBM{
			{
				"$unwind": "$items",
			},
			{
				"$unwind": "$items.tags",
			},
			{
				"$group": model.DBM{
					"_id": "$items.tags",
					"totalSalesAmount": model.DBM{
						"$sum": model.DBM{
							"$multiply": []interface{}{"$items.price", "$items.quantity"},
						},
					},
				},
			},
		}

		// Execute the aggregation pipeline
		result, err := driver.Aggregate(ctx, sales1, pipeline)
		assert.Nil(t, err)

		// Check if the result matches the expected result
		expectedResult := []model.DBM{
			{
				"_id":              "diamond",
				"totalSalesAmount": 49.95,
			},
			{
				"_id":              "green",
				"totalSalesAmount": 299.8,
			},
			{
				"_id":              "glossy",
				"totalSalesAmount": 39.95,
			},
			{
				"_id":              "black",
				"totalSalesAmount": 39.95,
			},
			{
				"_id":              "blank",
				"totalSalesAmount": 129.9,
			},
			{
				"_id":              "rough",
				"totalSalesAmount": 299.8,
			},
			{
				"_id":              "red",
				"totalSalesAmount": 129.9,
			},
			{
				"_id":              "blue",
				"totalSalesAmount": 49.95,
			},
		}

		assert.ElementsMatch(t, result, expectedResult)
	})
}

func TestCleanIndexes(t *testing.T) {
	tests := []struct {
		name          string
		insertIndexes int
		wantErr       bool
	}{
		{
			name:          "clean 10 indexes",
			insertIndexes: 10,
			wantErr:       false,
		},
		{
			name:          "clean 1 index",
			insertIndexes: 1,
			wantErr:       false,
		},
		{
			name:          "clean 0 index",
			insertIndexes: 0,
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			driver, object := prepareEnvironment(t)

			// Insert indexes
			for i := 0; i < tt.insertIndexes; i++ {
				err := driver.CreateIndex(ctx, object, model.Index{
					Name: fmt.Sprintf("index_%d", i),
					Keys: []model.DBM{{fmt.Sprintf("key_%d", i): 1}},
				})
				assert.Nil(t, err)
			}

			if err := driver.CleanIndexes(ctx, object); (err != nil) != tt.wantErr {
				t.Errorf("mgoDriver.CleanIndexes() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Check if the indexes were removed
			indexes, err := driver.GetIndexes(ctx, object)
			assert.Nil(t, err)

			assert.Equal(t, 1, len(indexes)) // The default _id index is always present
		})
	}
}

func TestUpsert(t *testing.T) {
	ctx := context.Background()
	driver, object := prepareEnvironment(t)

	defer cleanDB(t)

	// Insert the object using upsert
	err := driver.Upsert(ctx, object, model.DBM{
		"age": 10,
	}, model.DBM{
		"$set": model.DBM{
			"name": "upsert_test",
		},
	})

	assert.Nil(t, err)

	assert.Equal(t, "upsert_test", object.Name)
	assert.Equal(t, 10, object.Age)

	// Check if the object was inserted
	err = driver.Query(ctx, object, object, model.DBM{
		"age":  10,
		"name": "upsert_test",
	})
	assert.Nil(t, err)

	assert.Equal(t, "upsert_test", object.Name)
	assert.Equal(t, 10, object.Age)

	// Update the object using upsert
	err = driver.Upsert(ctx, object, model.DBM{
		"age": 10,
	}, model.DBM{
		"$set": model.DBM{
			"name": "upsert_test_updated",
		},
	})

	assert.Nil(t, err)

	assert.Equal(t, "upsert_test_updated", object.Name)
	assert.Equal(t, 10, object.Age)

	// Check if the object was updated
	err = driver.Query(ctx, object, object, model.DBM{
		"age":  10,
		"name": "upsert_test_updated",
	})
	assert.Nil(t, err)

	assert.Equal(t, "upsert_test_updated", object.Name)
	assert.Equal(t, 10, object.Age)
}

func TestGetDBType(t *testing.T) {
	driver, _ := prepareEnvironment(t)
	info, err := driver.GetDatabaseInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// ToDo: update for those cases where it returns aws
	assert.Equal(t, utils.StandardMongo, info.Type)
}

func TestMongoDriver_GetTables(t *testing.T) {
	ctx := context.Background()
	driver, object := prepareEnvironment(t)
	defer cleanDB(t)

	err := driver.Insert(ctx, object)
	if err != nil {
		t.Fatalf("failed to insert test data into collection: %s", err)
	}

	collections, err := driver.GetTables(ctx)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(collections))
	if len(collections) > 0 {
		assert.Equal(t, object.TableName(), collections[0])
	}

	// Now test that drop works
	t.Run("Drop table", func(t *testing.T) {
		removed, err := driver.DropTable(ctx, object.TableName())
		assert.Nil(t, err)
		assert.Equal(t, 1, removed)
		collections, err = driver.GetTables(ctx)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(collections))
	})
}
