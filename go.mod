module github.com/TykTechnologies/storage

go 1.25.0

require (
	github.com/google/go-cmp v0.7.0
	github.com/lib/pq v1.10.9
	github.com/redis/go-redis/v9 v9.11.0
	github.com/stretchr/testify v1.11.1
	go.mongodb.org/mongo-driver v1.17.7
	golang.org/x/oauth2 v0.36.0
	golang.org/x/text v0.38.0
	google.golang.org/api v0.287.1
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
	gorm.io/driver/postgres v1.5.0
	gorm.io/gorm v1.30.1
)

require (
	github.com/helloeave/json v1.15.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
)

require (
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.17 // indirect
	github.com/googleapis/gax-go/v2 v2.22.0 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/grpc v1.82.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace gorm.io/gorm => github.com/TykTechnologies/gorm v1.20.7-0.20260417144542-d6d1a2e12d9d
