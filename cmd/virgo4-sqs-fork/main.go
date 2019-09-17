package main

import (
	"log"
	"os"
	"time"
	"github.com/uvalib/virgo4-sqs-fork/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := NewAwsSqs( AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle( cfg.InQueueName )
	if err != nil {
		log.Fatal( err )
	}

	outQueue1Handle, err := aws.QueueHandle( cfg.OutQueue1Name )
	if err != nil {
		log.Fatal( err )
	}

	outQueue2Handle, err := aws.QueueHandle( cfg.OutQueue2Name )
	if err != nil {
		log.Fatal( err )
	}

    for {

		log.Printf("Waiting for messages...")

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet( inQueueHandle, uint(MAX_SQS_BLOCK_COUNT), time.Duration( cfg.PollTimeOut ) * time.Second )
		if err != nil {
			log.Fatal( err )
		}

		// did we receive any?
		sz := len( messages )
		if sz != 0 {

			log.Printf( "Received %d messages", sz )

			//
			// send to each queue, ignore results for now
			//

			_, err = aws.BatchMessagePut( outQueue1Handle, messages )
			if err != nil {
				log.Fatal( err )
			}

			_, err = aws.BatchMessagePut( outQueue2Handle, messages )
			if err != nil {
				log.Fatal( err )
			}

			_, err = aws.BatchMessageDelete( inQueueHandle, messages )
			if err != nil {
				log.Fatal( err )
			}

		} else {
			log.Printf("No messages received...")
		}
	}
}

//
// end of file
//