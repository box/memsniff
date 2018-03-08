package infer

import (
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/mctext"
	"github.com/box/memsniff/protocol/model"
	"github.com/box/memsniff/protocol/redis"
)

// Consumer guesses and redirects to a protocol-correct consumer.
type Consumer struct {
	*model.Consumer
	innerConsumer *model.Consumer
}

// NewConsumer returns a new Consumer that will guess the protocol in use.
func NewConsumer(logger log.Logger, handler model.EventHandler) *model.Consumer {
	c := Consumer{
		Consumer: model.New(logger, handler),
	}
	c.Consumer.Run = c.infer
	return c.Consumer
}

func (c *Consumer) infer() {
	out, err := c.ClientReader.PeekN(1)
	if err != nil {
		return
	}
	switch out[0] {
	case '*':
		redisConsumer := redis.NewConsumer(c.Consumer.Logger, c.Consumer.Handler)
		redisConsumer.AdoptStreams(c.Consumer.ClientReader, c.Consumer.ServerReader)
		c.innerConsumer = redisConsumer.Consumer
	default:
		mctextConsumer := mctext.NewConsumer(c.Consumer.Logger, c.Consumer.Handler)
		c.innerConsumer = mctextConsumer
	}
	c.Consumer.Run = c.forward
	c.Consumer.Run()
}

func (c *Consumer) forward() {
	c.innerConsumer.Run()
}

func (c *Consumer) log(items ...interface{}) {
	if c.Logger != nil {
		c.Logger.Log(items...)
	}
}
