package mctext

import (
	"bytes"
	"github.com/box/memsniff/protocol/model"
	"io"
	"strconv"
)

const (
	crlf = "\r\n"
)

// Consumer generates events based on a memcached text protocol conversation.
type Consumer model.Consumer

// Run reads the conversation and returns at the close of the conversation.
// The client- and server-side readers are closed before Run returns.
func (c *Consumer) Run() {
	defer c.ClientReader.Close()
	defer c.ServerReader.Close()
	defer (*model.Consumer)(c).FlushEvents()
	for {
		c.log("awaiting command")
		line, err := c.ClientReader.ReadLine()
		if err == io.EOF {
			return
		}
		if err != nil {
			return
		}
		c.log("read command:", string(line))
		fields := bytes.Split(line, []byte(" "))
		if len(fields) <= 0 {
			continue
		}

		switch string(fields[0]) {
		case "get", "gets":
			err = c.handleGet(fields[1:])
			if err != nil {
				c.log("error processing stream:", err)
				return
			}
		case "set", "add", "replace", "append", "prepend":
			err = c.handleSet(fields[1:])
			if err != nil {
				c.log("error processing stream:", err)
				return
			}
		case "quit":
			return
		default:
			err = c.discardResponse()
			if err != nil {
				c.log("error processing unknown command", string(line), ":", err)
			}
		}
	}
}

func (c *Consumer) handleGet(fields [][]byte) error {
	if len(fields) < 1 {
		return c.discardResponse()
	}
	for {
		c.log("awaiting server reply to get")
		line, err := c.ServerReader.ReadLine()
		if err != nil {
			return err
		}
		c.log("server reply:", string(line))
		fields := bytes.Split(line, []byte(" "))
		if len(fields) >= 4 && bytes.Equal(fields[0], []byte("VALUE")) {
			key := fields[1]
			size, err := strconv.Atoi(string(fields[3]))
			if err != nil {
				return err
			}
			evt := model.Event{
				Type: model.EventGetHit,
				Key:  string(key),
				Size: size,
			}
			c.log("sending event:", evt)
			c.addEvent(evt)
			c.log("discarding value")
			_, err = c.ServerReader.Discard(size + len(crlf))
			if err != nil {
				return err
			}
			c.log("discarded value")
		} else {
			return nil
		}
	}
}

func (c *Consumer) handleSet(fields [][]byte) error {
	if len(fields) < 4 {
		return c.discardResponse()
	}
	size, err := strconv.Atoi(string(fields[3]))
	if err != nil {
		return c.discardResponse()
	}
	c.log("discarding", size+len(crlf), "from client")
	_, err = c.ClientReader.Discard(size + len(crlf))
	if err != nil {
		return nil
	}
	c.log("discarding response from server")
	return c.discardResponse()
}

func (c *Consumer) discardResponse() error {
	line, err := c.ServerReader.ReadLine()
	c.log("discarded response from server:", string(line))
	return err
}

func (c *Consumer) addEvent(evt model.Event) {
	(*model.Consumer)(c).AddEvent(evt)
}

func (c *Consumer) log(items ...interface{}) {
	if c.Logger != nil {
		c.Logger.Log(items...)
	}
}
