package main

import (
	"sync/atomic"
	"sync"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/conf"
	"bytes"
	"time"
	"github.com/ryandotsmith/l2met/store"
	"fmt"
	"github.com/ryandotsmith/l2met/parser"
	"bufio"
	"github.com/ryandotsmith/l2met/receiver"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"log/syslog"
	"github.com/satori/go.uuid"
	"strconv"
	"github.com/ryandotsmith/l2met/auth"
)

// The register accumulates buckets in memory.
// A seperate routine working on an interval will flush
// the buckets from the register.
type register struct {
	sync.Mutex
	m map[bucket.Id]*bucket.Bucket
}

type KafkaReceiver struct {
	// Keeping a register allows us to aggregate buckets in memory.
	// This decouples redis writes from HTTP requests.
	Register *register
	// After we pull data from the HTTP requests,
	// We put the data in the inbox to be processed.
	Inbox chan *receiver.LogRequest
	// The interval at which things are moved fron the inbox to the outbox
	TransferTicker *time.Ticker
	// After we flush our register of buckets, we put the
	// buckets in this channel to be flushed to redis.
	Outbox chan *bucket.Bucket
	// Flush buckets from register to redis. Number of seconds.
	FlushInterval time.Duration
	// How many outlet routines should be running.
	NumOutlets int
	// Bucket storage.
	Store store.Store
	//Count the number of times we accept a bucket.
	numBuckets, numReqs uint64
	// The number of time units allowed to pass before dropping a
	// log line.
	deadline int64
	// Publish receiver metrics on this channel.
	Mchan    *metchan.Channel
	inFlight sync.WaitGroup
	KafkaConfig KafkaConfig
}

type KafkaConfig struct {
	Broker string // Kafka broker url
	Group string // Kafka consumer group id 
	Topics []string // Kafka topics to tail
	Properties map[string]interface{} // Additional consumer properties to override
	SigChan chan os.Signal
}

func NewKafkaReceiver(cfg *conf.D, s store.Store, kafkaConfig KafkaConfig) *KafkaReceiver {
	r := new(KafkaReceiver)
	r.Inbox = make(chan *receiver.LogRequest, cfg.BufferSize)
	r.Outbox = make(chan *bucket.Bucket, cfg.BufferSize)
	r.Register = &register{m: make(map[bucket.Id]*bucket.Bucket)}
	r.FlushInterval = cfg.FlushInterval
	r.NumOutlets = cfg.Concurrency
	r.deadline = cfg.ReceiverDeadline
	r.numBuckets = uint64(0)
	r.numReqs = uint64(0)
	r.Store = s
	r.KafkaConfig = kafkaConfig
	return r
}

func (r *KafkaReceiver) StartConsumer() {
	kafkaConf := &kafka.ConfigMap{
		"bootstrap.servers":    r.KafkaConfig.Broker,
		"group.id":             r.KafkaConfig.Group,
		"session.timeout.ms":   6000,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"}}
	for k, v := range r.KafkaConfig.Properties { 
		kafkaConf.SetKey(k, kafka.ConfigValue(v))
	}
	c, err := kafka.NewConsumer(kafkaConf)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(r.KafkaConfig.Topics, nil)

	libratoCreds := os.Getenv("LIBRATO_USER") + ":" + os.Getenv("LIBRATO_TOKEN")
	pwd, err := auth.EncryptAndSign([]byte(libratoCreds))
	if err != nil {
		println("Could not sign password !", err.Error())
		return
	}

	run := true

	for run == true {
		select {
		case sig := <-r.KafkaConfig.SigChan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				msg, err := msgAsSyslog(e.Value)
				if err != nil {
					fmt.Printf("Could not serialize message for syslog %s : %v\n", msg, err)
				} else {
					r.Receive(msg, map[string][]string{"auth": {string(pwd)}})
				}
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
func msgAsSyslog(msg []byte) ([]byte, error) {
	hostname, _ := os.Hostname() //hostname
	p := syslog.LOG_INFO //log level
	tag := "l2mt" //tag
	nl := "\n" //newline
	buf := bytes.NewBufferString("")

	timestamp := time.Now().Format(time.RFC3339)

	_, err := fmt.Fprintf(buf, "<%d>1 %s %s %s %d %s %s%s",
		p, timestamp, hostname, tag, os.Getpid(), uuid.NewV4().String(), msg, nl)
	//println(string(buf.Bytes()))
	msgWithLen := append([]byte(strconv.Itoa(len(string(buf.Bytes())))), []byte(" ")...)
	msgWithLen = append(msgWithLen, buf.Bytes()...)
	return msgWithLen, err
}

func (r *KafkaReceiver) Receive(b []byte, opts map[string][]string) {
	r.inFlight.Add(1)
	r.Inbox <- &receiver.LogRequest{b, opts}

}

// Start moving data through the receiver's pipeline.
func (r *KafkaReceiver) Start() {
	// Start the kafka consumer, parallelism is determined by partitions within

	// Accepting the data involves parsing logs messages
	// into buckets. It is mostly CPU bound, so
	// it makes sense to parallelize this to the extent
	// of the number of CPUs.
	for i := 0; i < r.NumOutlets; i++ {
		go r.accept()
	}
	// Outletting data to the store involves sending
	// data out on the network to Redis. We may wish to
	// add more threads here since it is likely that
	// they will be blocking on I/O.
	for i := 0; i < r.NumOutlets; i++ {
		go r.outlet()
	}
	r.TransferTicker = time.NewTicker(r.FlushInterval)
	// The transfer is not a concurrent process.
	// It removes buckets from the register to the outbox.
	go r.scheduleTransfer()
	go r.Report()

	r.StartConsumer()
}

// This function can be used as
// and indicator of when it is safe
// to shutdown the process.
func (r *KafkaReceiver) Wait() {
	r.inFlight.Wait()
}

func (r *KafkaReceiver) accept() {
	for req := range r.Inbox {
		rdr := bufio.NewReader(bytes.NewReader(req.Body))
		//TODO(ryandotsmith): Use a cached store time.
		// The code to use here should look something like this:
		// storeTime := r.Store.Now()
		// However, since we are in a tight loop here,
		// we cant make this call. Benchmarks show that using a local
		// redis and making the time call on the redis store will slow
		// down the receive loop by 10x.
		// However, we run the risk of accepting data that is past
		// its deadline due to clock drift on the localhost. Although
		// we don't run the risk of re-reporting an interval to Librato
		// because our outlet uses the store time to process buckets.
		// So even if we write a bucket to redis that is past the
		// deadline, our outlet scanner should not pick it up because
		// it uses redis time to find buckets to process.
		storeTime := time.Now()
		startParse := time.Now()
		for b := range parser.BuildBuckets(rdr, req.Opts, r.Mchan) {
			if b.Id.Delay(storeTime) <= r.deadline {
				r.inFlight.Add(1)
				r.addRegister(b)
			} else {
				r.Mchan.Measure("receiver.drop", 1)
			}
		}
		r.Mchan.Time("receiver.accept", startParse)
		r.inFlight.Done()
	}
}

func (r *KafkaReceiver) addRegister(b *bucket.Bucket) {
	r.Register.Lock()
	defer r.Register.Unlock()
	atomic.AddUint64(&r.numBuckets, 1)
	k := *b.Id
	_, present := r.Register.m[k]
	if !present {
		r.Mchan.Measure("receiver.add-bucket", 1)
		r.Register.m[k] = b
	} else {
		r.Mchan.Measure("receiver.merge-bucket", 1)
		r.Register.m[k].Merge(b)
	}
}

func (r *KafkaReceiver) scheduleTransfer() {
	for _ = range r.TransferTicker.C {
		r.transfer()
	}
}

func (r *KafkaReceiver) transfer() {
	r.Register.Lock()
	defer r.Register.Unlock()
	for k := range r.Register.m {
		if m, ok := r.Register.m[k]; ok {
			delete(r.Register.m, k)
			r.Outbox <- m
		}
	}
}

func (r *KafkaReceiver) outlet() {
	for b := range r.Outbox {
		startPut := time.Now()
		if err := r.Store.Put(b); err != nil {
			fmt.Printf("error=%s\n", err)
		}
		r.Mchan.Time("receiver.outlet", startPut)
		r.inFlight.Done()
	}
}

// Keep an eye on the lenghts of our bufferes.
// If they are maxed out, something is going wrong.
func (r *KafkaReceiver) Report() {
	for _ = range time.Tick(time.Second) {
		nb := atomic.LoadUint64(&r.numBuckets)
		nr := atomic.LoadUint64(&r.numReqs)
		atomic.AddUint64(&r.numBuckets, -nb)
		atomic.AddUint64(&r.numReqs, -nr)
		fmt.Printf("receiver.http.num-buckets=%d\n", nb)
		fmt.Printf("receiver.http.num-reqs=%d\n", nr)
		pre := "receiver.buffer."
		r.Mchan.Measure(pre+"inbox", float64(len(r.Inbox)))
		r.Mchan.Measure(pre+"outbox", float64(len(r.Outbox)))
	}
}
