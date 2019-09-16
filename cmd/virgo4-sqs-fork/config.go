package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName    string
	OutQueue1Name  string
	OutQueue2Name  string
	PollTimeOut    int64
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.InQueueName, "inqueue", "", "Inbound queue name")
	flag.StringVar(&cfg.OutQueue1Name, "outqueue1", "", "Output queue 1 name")
	flag.StringVar(&cfg.OutQueue2Name, "outqueue2", "", "Output queue 2 name")
	flag.Int64Var(&cfg.PollTimeOut, "pollwait", 15, "Poll wait time (in seconds)")

	flag.Parse()

	if len( cfg.InQueueName ) == 0 {
		log.Fatalf( "InQueueName cannot be blank" )
	}

	if len( cfg.OutQueue1Name ) == 0 {
		log.Fatalf( "OutQueue1Name cannot be blank" )
	}

	if len( cfg.OutQueue2Name ) == 0 {
		log.Fatalf( "OutQueue2Name cannot be blank" )
	}

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName )
	log.Printf("[CONFIG] OutQueue1Name        = [%s]", cfg.OutQueue1Name )
	log.Printf("[CONFIG] OutQueue2Name        = [%s]", cfg.OutQueue2Name )
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut )

	return &cfg
}
