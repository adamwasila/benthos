/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"strings"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/pebbe/zmq4"
)

//--------------------------------------------------------------------------------------------------

// ZMQ4Config - Configuration for the ZMQ4 input type.
type ZMQ4Config struct {
	Addresses  []string `json:"addresses" yaml:"addresses"`
	SocketType string   `json:"socket_type" yaml:"socket_type"`
}

// NewZMQ4Config - Creates a new ZMQ4Config with default values.
func NewZMQ4Config() ZMQ4Config {
	return ZMQ4Config{
		Addresses:  []string{"tcp://*:1234"},
		SocketType: "PUSH",
	}
}

//--------------------------------------------------------------------------------------------------

// ZMQ4 - An input type that serves ZMQ4 POST requests.
type ZMQ4 struct {
	conf Config

	socket *zmq4.Socket

	newMessagesChan chan (<-chan types.Message)

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewZMQ4 - Create a new ZMQ4 input type.
func NewZMQ4(conf Config) (*ZMQ4, error) {
	z := ZMQ4{
		conf:            conf,
		messages:        nil,
		responseChan:    make(chan types.Response),
		newMessagesChan: make(chan (<-chan types.Message)),
		closedChan:      make(chan struct{}),
		closeChan:       make(chan struct{}),
	}

	t, err := getZMQType(conf.ZMQ4.SocketType)
	if nil != err {
		return nil, err
	}

	ctx, err := zmq4.NewContext()
	if nil != err {
		return nil, err
	}

	if z.socket, err = ctx.NewSocket(t); nil != err {
		return nil, err
	}

	for _, address := range conf.ZMQ4.Addresses {
		if strings.Contains(address, "*") {
			err = z.socket.Bind(address)
		} else {
			err = z.socket.Connect(address)
		}
		if err != nil {
			return nil, err
		}
	}

	go z.loop()

	return &z, nil
}

//--------------------------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "PUB":
		return zmq4.PUB, nil
	case "XPUB":
		return zmq4.XPUB, nil
	case "PUSH":
		return zmq4.PUSH, nil
	}
	return zmq4.PULL, types.ErrInvalidZMQType
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (z *ZMQ4) loop() {
	running := true
	for running {
		select {
		case msg, open := <-z.messages:
			// If the messages chan is closed we do not close ourselves as it can replaced.
			if !open {
				z.messages = nil
			} else {
				_, err := z.socket.SendMessage(msg.Parts)
				z.responseChan <- types.NewSimpleResponse(err)
			}
		case newChan, open := <-z.newMessagesChan:
			if running = open; running {
				z.messages = newChan
			}
		case _, running = <-z.closeChan:
			running = false
		}
	}

	close(z.responseChan)
	close(z.newMessagesChan)
	close(z.closedChan)
}

// SetReadChan - Assigns a new messages channel for the output to read.
func (z *ZMQ4) SetReadChan(msgs <-chan types.Message) {
	z.newMessagesChan <- msgs
}

// ResponseChan - Returns the errors channel.
func (z *ZMQ4) ResponseChan() <-chan types.Response {
	return z.responseChan
}

// CloseAsync - Shuts down the ZMQ4 output and stops processing messages.
func (z *ZMQ4) CloseAsync() {
	close(z.closeChan)
}

// WaitForClose - Blocks until the ZMQ4 output has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	select {
	case <-z.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
