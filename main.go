package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"flag"

	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/outlet"
	"github.com/ryandotsmith/l2met/reader"
	"github.com/ryandotsmith/l2met/store"
	"strings"
)

var l2metCfg *conf.D
var broker *string
var group *string
var topics CsvFlag
var usage *bool

type CsvFlag []string 

func (f *CsvFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *CsvFlag) Set(value string) error {
	*f = strings.Split(value, ",")
	return nil
}

func init() {
	l2metCfg = conf.New()

	broker = flag.String("brokers", "localhost:9092", "kafka brokers")
	group = flag.String("group-id", "", "kafka consumer group id")
	usage = flag.Bool("help", false, "Help, usage")
	flag.Var(&topics, "topics", "kafka topics")

	flag.Parse()
}

func main() {
	
	if *usage {
		flag.Usage()
		os.Exit(1)
	}
	
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	//client := librato.NewClient(os.Getenv("LIBRATO_EMAIL"), os.Getenv("LIBRATO_TOKEN"))

	mchan := metchan.New(l2metCfg)
	mchan.Start()

	// The store will be used by receivers and outlets.
	var st store.Store
	if len(l2metCfg.RedisHost) > 0 {
		redisStore := store.NewRedisStore(l2metCfg)
		redisStore.Mchan = mchan
		st = redisStore
		fmt.Printf("at=initialized-redis-store\n")
	} else {
		st = store.NewMemStore()
		fmt.Printf("at=initialized-mem-store\n")
	}

	rdr := reader.New(l2metCfg, st)
	rdr.Mchan = mchan
	outlet := outlet.NewLibratoOutlet(l2metCfg, rdr)
	outlet.Mchan = mchan
	outlet.Start()

	if l2metCfg.UsingReciever {
		recv := NewKafkaReceiver(l2metCfg, st, KafkaConfig{
			Broker: *broker, Group: *group, Topics: topics, Properties: nil, SigChan: sigchan,
		})
		recv.Mchan = mchan
		recv.Start()

	}
}
