module storage-writer

go 1.19

replace github.com/go-redis/redis/v8 v8.8.0 => github.com/go-redis/redis/v9 v9.0.0-rc.2

require (
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	github.com/go-redis/redismock/v9 v9.0.0-rc.2
	github.com/joho/godotenv v1.4.0
	github.com/kneu-messenger-pigeon/events v0.1.30
	github.com/segmentio/kafka-go v0.4.38
	github.com/stretchr/testify v1.8.1
	golang.org/x/text v0.4.0
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
