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

type state func() error

type fsm struct {
	logger   log.Logger
	consumer *model.Consumer
	state    state
	parser   *RespParser
}

func NewFsm(logger log.Logger) *fsm {
	f := &fsm{
		logger: logger,
		parser: NewParser(nil),
	}
	return f
}

func (f *fsm) SetConsumer(consumer *model.Consumer) {
	f.consumer = consumer
	f.transitionTo(false, f.readCommand)
}

func (f *fsm) Run() {
	for {
		err := f.state()
		switch err {
		case nil:
			continue
		case reader.ErrShortRead, io.EOF:
			return
		default:
			f.consumer.ClientReader.Reset()
			f.consumer.ServerReader.Reset()
			f.transitionTo(false, f.readCommand)
			return
		}
	}
}

func (f *fsm) transitionTo(fromServer bool, state state) {
	if fromServer {
		f.parser.Reset(f.consumer.ServerReader)
		f.parser.Options.BulkCaptureLimit = 0
	} else {
		f.parser.Reset(f.consumer.ClientReader)
		f.parser.Options.BulkCaptureLimit = maxCommandSize
	}
	f.state = state
}

func (f *fsm) readCommand() error {
	f.consumer.ServerReader.Truncate()
	err := f.parser.Run()
	if err != nil {
		return err
	}
	fields := f.parser.BulkArray()
	cmd := fields[0]
	switch strings.ToLower(string(cmd)) {
	case "get", "mget":
		if len(fields) < 2 {
			return ProtocolErr
		}
		f.transitionTo(true, f.handleGet(fields[1]))
		return nil
	default:
		f.transitionTo(true, f.discardResponse)
	}
	return nil
}

func (f *fsm) handleGet(key []byte) func() error {
	return func() error {
		err := f.parser.Run()
		if err != nil {
			return err
		}
		res := f.parser.Result()
		if res == nil {
			f.consumer.AddEvent(model.Event{
				Type: model.EventGetMiss,
				Key:  string(key),
			})
		} else {
			f.consumer.AddEvent(model.Event{
				Type: model.EventGetHit,
				Key:  string(key),
				Size: res.(int),
			})
		}
		f.transitionTo(false, f.readCommand)
		return nil
	}
}

func (f *fsm) discardResponse() error {
	err := f.parser.Run()
	if err != nil {
		return err
	}
	f.transitionTo(false, f.readCommand)
	return nil
}

func (f *fsm) log(items ...interface{}) {
	if f.logger != nil {
		f.logger.Log(items...)
	}
}
