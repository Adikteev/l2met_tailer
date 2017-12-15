### Kafka L2Met Tailer

#### Purpose
This program leverages ryandotsmith's l2met system (log to metric), replacing the need for logplex with a kafka consumer.

#### Usage
For direct usage, you can use the main package in this repo as entry point.
```
go get -u github.com/Adikteev/l2met_tailer
l2met_tailer -k localhost:9092 -t mytopic --librato-user me@librato.com --librato-token mytoken -r redis.local
```
or simply, for the many configuration options and usage 
```
go get -u github.com/Adikteev/l2met_tailer
l2met_tailer --help
```

For custom usage, see the godoc for the KafkaReceiver type, and for the rest refer to the l2met documentation.


#### Bare Setup 

````
export SECRETS=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
export TZ=UTC
export REDIS_URL=redis://localhost:6379
go build .
./l2met_tailer --help
````

Additional credentials : 
```
export LIBRATO_EMAIL=
export LIBRATO_TOKEN=
```