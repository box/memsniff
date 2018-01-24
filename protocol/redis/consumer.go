package redis

import (
	"io"
	"strings"

	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
)

const (
	maxCommandSize = 1024
)

type Consumer struct {
	*model.Consumer
	parser *RespParser
}

func NewConsumer(logger log.Logger, handler model.EventHandler) *model.Consumer {
	c := Consumer{
		Consumer: model.New(logger, handler),
		parser:   NewParser(nil),
	}
	c.Consumer.Run = c.run
	c.transitionTo(false, c.readCommand)
	return c.Consumer
}

func (c *Consumer) run() {
	for {
		err := c.State()
		switch err {
		case nil:
			continue
		case reader.ErrShortRead, io.EOF:
			return
		default:
			c.ClientReader.Reset()
			c.ServerReader.Reset()
			c.transitionTo(false, c.readCommand)
			return
		}
	}
}

func (c *Consumer) transitionTo(fromServer bool, state model.State) {
	if fromServer {
		c.parser.Reset(c.ServerReader)
		c.parser.Options.BulkCaptureLimit = 0
	} else {
		c.parser.Reset(c.ClientReader)
		c.parser.Options.BulkCaptureLimit = maxCommandSize
	}
	c.State = state
}

func (c *Consumer) readCommand() error {
	c.ServerReader.Truncate()
	err := c.parser.Run()
	if err != nil {
		return err
	}
	fields := c.parser.BytesArray()
	cmd := fields[0]
	switch strings.ToLower(string(cmd)) {
	case "get", "mget":
		if len(fields) < 2 {
			return ProtocolErr
		}
		c.transitionTo(true, c.handleGet(fields[1]))
		return nil
	default:
		c.transitionTo(true, c.discardResponse)
	}
	return nil
}

func (c *Consumer) handleGet(key []byte) func() error {
	return func() error {
		err := c.parser.Run()
		if err != nil {
			return err
		}
		res := c.parser.Result()
		if res == nil {
			c.Consumer.AddEvent(model.Event{
				Type: model.EventGetMiss,
				Key:  string(key),
			})
		} else {
			c.Consumer.AddEvent(model.Event{
				Type: model.EventGetHit,
				Key:  string(key),
				Size: res.(int),
			})
		}
		c.transitionTo(false, c.readCommand)
		return nil
	}
}

func (c *Consumer) discardResponse() error {
	err := c.parser.Run()
	if err != nil {
		return err
	}
	c.transitionTo(false, c.readCommand)
	return nil
}
