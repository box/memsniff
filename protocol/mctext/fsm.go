package mctext

import (
	"bytes"
	"errors"
	"io"
	"regexp"
	"strconv"

	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
)

const (
	crlf       = "\r\n"
	debuglevel = 0
)

var (
	asciiRe, _        = regexp.Compile(`^[a-zA-Z]+$`)
	errProtocolDesync = errors.New("protocol desync while reading command")
)

// fsm generates events based on a memcached text protocol conversation.
type fsm struct {
	logger   log.Logger
	consumer *model.Consumer
	state    state
	cmd      string
	args     []string
}

type state func() error

func NewFsm(logger log.Logger) model.Fsm {
	fsm := &fsm{
		logger: logger,
	}
	fsm.state = fsm.peekBinaryProtocolMagicByte
	return fsm
}

func (f *fsm) SetConsumer(consumer *model.Consumer) {
	f.consumer = consumer
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
			// data lost or protocol error, try to resync at the next command
			f.log(2, "trying to resync after error:", err)
			f.consumer.ClientReader.Reset()
			f.consumer.ServerReader.Reset()
			f.state = f.readCommand
			return
		}
	}
}

func (f *fsm) peekBinaryProtocolMagicByte() error {
	f.consumer.ServerReader.Truncate()
	firstByte, err := f.consumer.ClientReader.PeekN(1)
	if err != nil {
		if _, ok := err.(reader.ErrLostData); ok {
			// try again, making sure we read from the start of a client packet.
			f.consumer.ClientReader.Truncate()
			err = reader.ErrShortRead
		}
		return err
	}
	if firstByte[0] == 0x80 {
		//binary memcached protocol, don't try to handle this connection
		f.log(2, "looks like binary protocol, ignoring connection")
		f.consumer.Close()
		return io.EOF
	}
	f.state = f.readCommand
	return nil
}

func (f *fsm) readCommand() error {
	f.args = f.args[:0]
	f.consumer.ServerReader.Truncate()
	f.log(3, "reading command")
	pos, err := f.consumer.ClientReader.IndexAny(" \n")
	if err != nil {
		return err
	}

	cmd, err := f.consumer.ClientReader.ReadN(pos + 1)
	if err != nil {
		return err
	}
	f.cmd = string(bytes.TrimRight(cmd, " \r\n"))
	f.log(3, "read command:", f.cmd)

	if !asciiRe.MatchString(f.cmd) {
		return errProtocolDesync
	}

	if f.commandState() != nil {
		f.state = f.readArgs
		return nil
	}

	f.state = f.handleUnknown
	return nil
}

// dispatchCommand is the state after the complete client request has been read.
func (f *fsm) commandState() state {
	switch f.cmd {
	case "get", "gets":
		return f.handleGet
	case "set", "add", "replace", "append", "prepend", "cas":
		return f.handleSet
	case "quit":
		return f.handleQuit
	default:
		return nil
	}
}

func (f *fsm) readArgs() error {
	f.consumer.ServerReader.Truncate()
	pos, err := f.consumer.ClientReader.IndexAny(" \n")
	if err != nil {
		return err
	}
	word, err := f.consumer.ClientReader.ReadN(pos + 1)
	if err != nil {
		return err
	}
	f.args = append(f.args, string(bytes.TrimRight(word[:len(word)-1], "\r")))
	delim := word[len(word)-1]
	if delim == ' ' {
		return nil
	}
	f.log(3, "read arguments:", f.args)
	f.state = f.commandState()
	return nil
}

func (f *fsm) handleGet() error {
	if len(f.args) < 1 {
		return f.discardResponse()
	}
	for {
		f.log(3, "awaiting server reply to get for", len(f.args), "keys")
		line, err := f.consumer.ServerReader.ReadLine()
		if err != nil {
			return err
		}
		f.log(3, "server reply:", string(line))
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
			// f.log("sending event:", evt)
			f.addEvent(evt)
			// f.log("discarding value")
			_, err = f.consumer.ServerReader.Discard(size + len(crlf))
			if err != nil {
				return err
			}
			// f.log("discarded value")
		} else {
			f.state = f.readCommand
			return nil
		}
	}
}

func (f *fsm) handleSet() error {
	if len(f.args) < 4 {
		return f.discardResponse()
	}
	size, err := strconv.Atoi(f.args[3])
	if err != nil {
		return f.discardResponse()
	}
	f.log(3, "discarding", size+len(crlf), "from client")
	_, err = f.consumer.ClientReader.Discard(size + len(crlf))
	if err != nil {
		return err
	}
	f.log(3, "discarding response from server")
	return f.discardResponse()
}

func (f *fsm) handleQuit() error {
	// don't call fsm.Close() because tcpassembly will still write data
	// to these readers for the FIN/FIN+ACK
	f.consumer.ClientReader.Close()
	f.consumer.ServerReader.Close()
	f.state = func() error { return io.EOF }
	return io.EOF
}

func (f *fsm) handleUnknown() error {
	return f.discardResponse()
}

func (f *fsm) discardResponse() error {
	f.state = f.discardResponse
	f.log(3, "discarding response from server")
	line, err := f.consumer.ServerReader.ReadLine()
	if err != nil {
		return err
	}
	f.log(3, "discarded response from server:", string(line))
	f.state = f.readCommand
	return nil
}

func (f *fsm) addEvent(evt model.Event) {
	f.consumer.AddEvent(evt)
}

func (f *fsm) log(level int, items ...interface{}) {
	if f.logger != nil && debuglevel >= level {
		f.logger.Log(items...)
	}
}
