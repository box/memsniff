package mctext

import (
	"bytes"
	"io"
	"strconv"

	"github.com/box/memsniff/assembly/gapbuffer"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
)

const (
	crlf = "\r\n"
)

// Consumer generates events based on a memcached text protocol conversation.
type Consumer model.Consumer

func NewConsumer(logger log.Logger, handler model.EventHandler) *model.Consumer {
	c := &Consumer{
		Logger:       logger,
		Handler:      handler,
		ClientReader: gapbuffer.NewStream(),
		ServerReader: gapbuffer.NewStream(),
	}
	c.Run = c.run
	c.State = c.readCommand
	return (*model.Consumer)(c)
}

func (c *Consumer) run() {
	for {
		err := c.State()
		if err == gapbuffer.ErrShortRead {
			return
		}
		if err != nil {
			// data lost or protocol error, try to resync at the next command
			c.log(err)
			c.log("trying to resync")
			c.reset()
			c.State = c.readCommand
			return
		}
	}
}

func (c *Consumer) reset() {
	c.ServerReader.Clear()
}

func (c *Consumer) readCommand() error {
	c.log("reading command")
	line, err := c.ClientReader.ReadLine()
	if err != nil {
		return err
	}

	c.log("read command:", string(line))
	fields := bytes.Split(line, []byte(" "))
	if len(fields) <= 0 {
		c.log("malformed command")
		c.reset()
		return nil
	}

	switch string(fields[0]) {
	case "get", "gets":
		c.State = func() error { return c.handleGet(fields[1:]) }
	case "set", "add", "replace", "append", "prepend":
		c.State = func() error { return c.handleSet(fields[1:]) }
	case "quit":
		c.ClientReader.Close()
		c.ServerReader.Close()
		c.State = func() error { return io.EOF }
	default:
		c.State = func() error { return c.discardResponse() }
	}
	return nil
}

func (c *Consumer) handleGet(keys [][]byte) error {
	if len(keys) < 1 {
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
			c.State = c.readCommand
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
		return err
	}
	c.log("discarding response from server")
	return c.discardResponse()
}

func (c *Consumer) discardResponse() error {
	line, err := c.ServerReader.ReadLine()
	if err != nil {
		return err
	}
	c.log("discarded response from server:", string(line))
	c.State = c.readCommand
	return nil
}

func (c *Consumer) addEvent(evt model.Event) {
	(*model.Consumer)(c).AddEvent(evt)
}

func (c *Consumer) log(items ...interface{}) {
	if c.Logger != nil {
		c.Logger.Log(items...)
	}
}
