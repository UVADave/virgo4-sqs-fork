package main

import (
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
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

		//log.Printf("Waiting for messages...")
		start := time.Now()

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet( inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration( cfg.PollTimeOut ) * time.Second )
		if err != nil {
			log.Fatal( err )
		}

		// did we receive any?
		sz := len( messages )
		if sz != 0 {

			//log.Printf( "Received %d messages", sz )

			//
			// send to each queue, ignore results for now
			//

			opStatus, err := aws.BatchMessagePut( outQueue1Handle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to send to queue 1", ix )
				}
			}

			opStatus, err = aws.BatchMessagePut( outQueue2Handle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to send to queue 2", ix )
				}
			}

			// delete them all anyway
			opStatus, err = aws.BatchMessageDelete( inQueueHandle, messages )
			if err != nil {
				log.Fatal( err )
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf( "WARNING: message %d failed to delete", ix )
				}
			}

			duration := time.Since(start)
			log.Printf("Processed %d messages (%0.2f tps)", sz, float64( sz ) / duration.Seconds() )

		} else {
			log.Printf("No messages received...")
		}
	}
}

//
// end of file
//