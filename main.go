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
	"github.com/ryandotsmith/l2met/receiver"
	"github.com/ryandotsmith/l2met/store"
)

var l2metCfg *conf.D
var broker string
var group string
var topics []string
var usage bool

type CsvFlag []string 
(f CsvFlag) String() string {
	return strings.Join(f, ",")
}

(f CsvFlag) Set(string) error {
	return strings.Split(f, ",")
}

func init() {
	l2metCfg = conf.New()
	broker = flag.String("b,brokers", "localhost:9092", "kafka brokers")
	group = flag.String("g,group-id", "", "kafka consumer group id")
	usage = flag.Bool("h,help", false, "Help, usage")
	flag.Var(topics, "t,topics", "", "kafka topics")

	flag.Parse()
}

func main() {
	
	if usage {
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

	if l2metCfg.UsingReciever {
		recv := receiver.NewKafkaReceiver(l2metCfg, st)
		recv.Mchan = mchan
		recv.Start()

	}

	rdr := reader.New(l2metCfg, st)
	rdr.Mchan = mchan
	outlet := outlet.NewLibratoOutlet(l2metCfg, rdr)
	outlet.Mchan = mchan
	outlet.Start()
}
