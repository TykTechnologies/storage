//go:build mongo
// +build mongo

package mgo

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/TykTechnologies/storage/persistent/dbm"
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
		Name:    "test",
		Email:   "test@test.com",
		Country: dummyCountryField{CountryName: "test_country", Continent: "test_continent"},
		Age:     10,
	}

	return mgo, object
}

func TestInsert(t *testing.T) {
	driver, object := prepareEnvironment(t)
	defer driver.Drop(context.Background(), object)

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
	driver, object := prepareEnvironment(t)
	ctx := context.Background()

	t.Run("deleting a existing object", func(t *testing.T) {
		// insert the object into the database
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		// delete the collection
		defer driver.Drop(ctx, object)

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
	t.Run("Updating an existing obj", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()

		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		defer driver.Drop(ctx, object)

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

		defer driver.Drop(ctx, object)

		object.SetObjectID(id.NewObjectID())

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})

	t.Run("Updating an object without _id", func(t *testing.T) {
		driver, object := prepareEnvironment(t)
		ctx := context.Background()
		defer driver.Drop(ctx, object)

		err := driver.Update(ctx, object)
		assert.NotNil(t, err)
		assert.False(t, driver.IsErrNoRows(err))
	})
}

func TestUpdateMany(t *testing.T) {
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
			defer driver.Drop(ctx, object)

			for _, obj := range dummyData {
				err := driver.Insert(ctx, &obj)
				assert.Nil(t, err)
			}

			err := driver.UpdateMany(ctx, tc.givenObjects, tc.query...)
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
			ctx := context.Background()

			driver, object := prepareEnvironment(t)
			defer driver.Drop(ctx, object)

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

				err := driver.Insert(ctx, object)
				assert.Nil(t, err)
			}

			object.invalidCollection = tt.wantErr

			got, err := driver.Count(ctx, object)
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

func TestQuery(t *testing.T) {
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
			defer driver.Drop(ctx, object)

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
			ctx := context.Background()
			defer driver.Drop(ctx, object)

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
