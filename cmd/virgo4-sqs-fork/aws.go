package main

import (
   //"log"
)

var MAX_MESSAGES = 10

// simplifications
type QueueHandle  string
type DeleteHandle string
type OpStatus     bool

// just a KV pair
type Attribute struct {
   name  string
   value string
}

type Attributes []Attribute
type Payload string

type Message struct {
   Attribs      Attributes
   DeleteHandle DeleteHandle
   Payload      Payload
}

type AWS interface {
   QueueHandle( string ) ( QueueHandle, error )
   BatchMessageGet( queue QueueHandle, maxMessages uint, waitTime uint ) ( []Message, error )
   BatchMessagePut( queue QueueHandle, messages []Message ) ( []OpStatus, error )
   BatchMessageDelete( queue QueueHandle, messages []Message ) ( []OpStatus, error )
}

// our singleton instance
//var aws AWS

// Initialize our AWS connection
func NewAWS( ) ( AWS, error ) {

   // mock the implementation here if necessary

   aws, err := newAws( )
   return aws, err
}

//
// end of file
//