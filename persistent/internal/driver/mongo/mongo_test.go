//go:build mongo
// +build mongo

package mongo

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/TykTechnologies/storage/persistent/id"
	"github.com/TykTechnologies/storage/persistent/internal/model"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

type dummyDBObject struct {
	Id      id.ObjectId       `bson:"_id,omitempty"`
	Name    string            `bson:"testName"`
	Email   string            `bson:"email"`
	Country dummyCountryField `bson:"country"`
	Age     int               `bson:"age"`
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
	return "dummy"
}

func prepareEnvironment(t *testing.T) (*mongoDriver, *dummyDBObject) {
	t.Helper()
	// create a new mgo driver connection
	mgo, err := NewMongoDriver(&model.ClientOpts{
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

func TestNewMongoDriver(t *testing.T) {
	t.Run("new driver with connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "mongodb://localhost:27017/test",
		})

		assert.Nil(t, err)
		assert.NotNil(t, newDriver)
		assert.NotNil(t, newDriver.lifeCycle)
		assert.NotNil(t, newDriver.options)
		assert.Nil(t, newDriver.client.Ping(context.Background(), nil))
	})
	t.Run("new driver with invalid connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{
			ConnectionString: "test",
		})

		assert.NotNil(t, err)
		assert.Equal(t, "invalid connection string", err.Error())
		assert.Nil(t, newDriver)
	})
	t.Run("new driver without connection string", func(t *testing.T) {
		newDriver, err := NewMongoDriver(&model.ClientOpts{})

		assert.NotNil(t, err)
		assert.Equal(t, "can't connect without connection string", err.Error())
		assert.Nil(t, newDriver)
	})
}

func TestInsert(t *testing.T) {
	driver, object := prepareEnvironment(t)
	ctx := context.Background()
	collection := driver.client.Database("test").Collection(object.TableName())

	// insert the object into the database
	err := driver.Insert(ctx, object)
	assert.Nil(t, err)
	// delete the collection
	defer collection.Drop(ctx)

	// check if the object was inserted
	var result dummyDBObject
	err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
	assert.Nil(t, err)

	assert.Equal(t, object.Name, result.Name)
	assert.Equal(t, object.Email, result.Email)
	assert.Equal(t, object.GetObjectID(), result.GetObjectID())
}

func TestDelete(t *testing.T) {
	driver, object := prepareEnvironment(t)
	ctx := context.Background()
	collection := driver.client.Database("test").Collection(object.TableName())

	t.Run("deleting a existing object", func(t *testing.T) {
		// insert the object into the database
		err := driver.Insert(ctx, object)
		assert.Nil(t, err)
		// delete the collection
		defer collection.Drop(ctx)

		// validates that the object was inserted
		var result dummyDBObject
		err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
		assert.Nil(t, err)
		assert.Equal(t, object.Name, result.Name)
		assert.Equal(t, object.Email, result.Email)
		assert.Equal(t, object.GetObjectID(), result.GetObjectID())

		// delete the object from the database
		err = driver.Delete(ctx, object)
		assert.Nil(t, err)

		// check if the object was deleted
		err = collection.FindOne(ctx, bson.M{"_id": object.GetObjectID()}).Decode(&result)
		assert.NotNil(t, err)
		assert.True(t, driver.IsErrNoRows(err))
	})

	t.Run("deleting a non existent object", func(t *testing.T) {
		// delete the object from the database
		err := driver.Delete(ctx, object)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New("error deleting a non existing object"), err)
	})
}

func TestCount(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name      string
		prepareTc func(*testing.T) (*mongoDriver, *dummyDBObject)
		want      int
		wantErr   error
	}{
		{
			name: "0 objects",
			want: 0,
			prepareTc: func(t *testing.T) (*mongoDriver, *dummyDBObject) {
				return prepareEnvironment(t)
			},
		},
		{
			name: "1 object",
			want: 1,
			prepareTc: func(t *testing.T) (*mongoDriver, *dummyDBObject) {
				driver, object := prepareEnvironment(t)

				err := driver.Insert(ctx, object)
				assert.Nil(t, err)

				return driver, object
			},
		},
		{
			name: "10 objects",
			want: 10,
			prepareTc: func(t *testing.T) (*mongoDriver, *dummyDBObject) {
				driver, object := prepareEnvironment(t)

				for i := 0; i < 10; i++ {
					object = &dummyDBObject{
						Name:  "test" + strconv.Itoa(i),
						Email: "test@test.com",
					}

					err := driver.Insert(ctx, object)
					assert.Nil(t, err)
				}

				return driver, object
			},
		},
		{
			name: "error when counting on closed connection ",
			want: 0,
			prepareTc: func(t *testing.T) (*mongoDriver, *dummyDBObject) {
				driver, object := prepareEnvironment(t)

				driver.Close()

				return driver, object
			},
			wantErr: errors.New("client is disconnected"),
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			driver, object := tc.prepareTc(t)
			defer driver.client.Database(driver.database).Collection(object.TableName()).Drop(ctx)

			got, err := driver.Count(ctx, object)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestQuery(t *testing.T) {
	type args struct {
		result interface{}
		query  model.DBM
	}

	dummyData := []dummyDBObject{
		{Name: "John", Email: "john@example.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 10},
		{Name: "Jane", Email: "jane@tyk.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry2", Continent: "TestContinent2"}, Age: 8},
		{Name: "Bob", Email: "bob@example.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry3", Continent: "TestContinent3"}, Age: 25},
		{Name: "Alice", Email: "alice@tyk.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry", Continent: "TestContinent"}, Age: 45},
		{Name: "Peter", Email: "peter@test.com", Id: id.NewObjectID(), Country: dummyCountryField{CountryName: "TestCountry4", Continent: "TestContinent4"}, Age: 12},
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
			name: "4 objects with limit 2 and offset 2 and sort by testName",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"_limit":  2,
					"_offset": 2,
					"_sort":   "testName",
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
			name: "filter by email ending with tyk.com and sort by testName",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"email": model.DBM{
						"$regex": "tyk.com$",
					},
					"_sort": "testName",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[3], dummyData[1]},
		},
		{
			name: "filter by testName starting with A",
			args: args{
				result: &dummyDBObject{},
				query: model.DBM{
					"testName": model.DBM{
						"$regex": "^A",
					},
				},
			},
			expectedResult: &dummyData[3],
		},
		{
			name: "filter by testName starting with J and sort by testName",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"testName": model.DBM{
						"$regex": "^J",
					},
					"_sort": "testName",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[1], dummyData[0]},
		},
		{
			name: "filter by country testName",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"country.country_name": "TestCountry",
				},
			},
			expectedResult: &[]dummyDBObject{dummyData[0], dummyData[3]},
		},
		{
			name: "filter by country testName and sort by testName",
			args: args{
				result: &[]dummyDBObject{},
				query: model.DBM{
					"country.country_name": "TestCountry",
					"_sort":                "testName",
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

			if err := driver.Query(ctx, object, tt.args.result, tt.args.query); (err != nil) != tt.wantErr {
				t.Errorf("mongoDriver.Query() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedResult, tt.args.result)
		})
	}
}
