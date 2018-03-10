package infer

import (
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/mctext"
	"github.com/box/memsniff/protocol/model"
	"github.com/box/memsniff/protocol/redis"
)

// fsm guesses and redirects to a protocol-correct consumer.
type fsm struct {
	logger   log.Logger
	consumer *model.Consumer
}

func NewFsm(logger log.Logger) model.Fsm {
	return &fsm{logger: logger}
}

func (f *fsm) SetConsumer(consumer *model.Consumer) {
	f.consumer = consumer
}

func (f *fsm) Run() {
	out, err := f.consumer.ClientReader.PeekN(1)
	if err != nil {
		return
	}
	var fsm model.Fsm
	switch out[0] {
	case '*':
		fsm = redis.NewFsm(f.logger)
	default:
		fsm = mctext.NewFsm(f.logger)
	}
	fsm.SetConsumer(f.consumer)
	f.consumer.Fsm = fsm
	fsm.Run()
}
