package mctext

import (
	"bytes"
	"io"
	"strconv"

	"github.com/box/memsniff/assembly/reader"
	"github.com/box/memsniff/log"
	"github.com/box/memsniff/protocol/model"
)

const (
	crlf       = "\r\n"
	debuglevel = 3
)

// Consumer generates events based on a memcached text protocol conversation.
type Consumer struct {
	*model.Consumer
	cmd  string
	args []string
}

func NewConsumer(logger log.Logger, handler model.EventHandler) *model.Consumer {
	c := Consumer{
		Consumer: model.New(logger, handler),
	}
	c.Consumer.Run = c.run
	c.Consumer.State = c.peekMagicByte
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
		case io.ErrShortWrite:
			c.log(1, "buffer overrun, abandoning connection")
			c.Consumer.Close()
			panic("buffer overrun")
			return
		default:
			// data lost or protocol error, try to resync at the next command
			c.log(2, "trying to resync after error:", err)
			c.ClientReader.Truncate()
			c.State = c.readCommand
			return
		}
	}
}

func (c *Consumer) peekMagicByte() error {
	c.ServerReader.Truncate()
	firstByte, err := c.ClientReader.PeekN(1)
	if err != nil {
		if _, ok := err.(reader.ErrLostData); ok {
			// try again, making sure we read from the start of a client packet.
			c.ClientReader.Truncate()
			err = reader.ErrShortRead
		}
		return err
	}
	if firstByte[0] == 0x80 {
		//binary memcached protocol, don't try to handle this connection
		c.log(2, "looks like binary protocol, ignoring connection")
		c.Consumer.Close()
		return io.EOF
	}
	c.State = c.readCommand
	return nil
}

func (c *Consumer) readCommand() error {
	c.ServerReader.Truncate()
	c.log(3, "reading command")
	pos, err := c.ClientReader.IndexAny(" \n")
	if err != nil {
		return err
	}

	cmd, err := c.ClientReader.ReadN(pos + 1)
	if err != nil {
		return err
	}
	c.cmd = string(bytes.TrimRight(cmd, " \r\n"))

	c.args = c.args[:0]
	if c.commandState() != nil {
		c.State = c.readArgs
		return nil
	}
	c.State = c.handleUnknown
	return nil
}

// dispatchCommand is the state after the complete client request has been read.
func (c *Consumer) commandState() model.State {
	switch c.cmd {
	case "get", "gets":
		return c.handleGet
	case "set", "add", "replace", "append", "prepend", "cas":
		return c.handleSet
	case "quit":
		return c.handleQuit
	default:
		return nil
	}
}

func (c *Consumer) readArgs() error {
	pos, err := c.ClientReader.IndexAny(" \n")
	if err != nil {
		return err
	}
	word, err := c.ClientReader.ReadN(pos + 1)
	if err != nil {
		return err
	}
	c.args = append(c.args, string(word[:len(word)-1]))
	delim := word[len(word)-1]
	if delim == ' ' {
		return nil
	}
	c.State = c.commandState()
	return nil
}

func (c *Consumer) handleGet() error {
	if len(c.args) < 1 {
		return c.discardResponse()
	}
	for {
		c.log(3, "awaiting server reply to get for", len(c.args), "keys")
		line, err := c.ServerReader.ReadLine()
		if err != nil {
			return err
		}
		c.log(3, "server reply:", string(line))
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
			// c.log("sending event:", evt)
			c.addEvent(evt)
			// c.log("discarding value")
			_, err = c.ServerReader.Discard(size + len(crlf))
			if err != nil {
				return err
			}
			// c.log("discarded value")
		} else {
			c.State = c.readCommand
			return nil
		}
	}
}

func (c *Consumer) handleSet() error {
	if len(c.args) < 4 {
		return c.discardResponse()
	}
	size, err := strconv.Atoi(c.args[3])
	if err != nil {
		return c.discardResponse()
	}
	c.log(3, "discarding", size+len(crlf), "from client")
	_, err = c.ClientReader.Discard(size + len(crlf))
	if err != nil {
		return err
	}
	c.log(3, "discarding response from server")
	return c.discardResponse()
}

func (c *Consumer) handleQuit() error {
	// don't call Consumer.Close() because tcpassembly will still write data
	// to these readers for the FIN/FIN+ACK
	c.ClientReader.Close()
	c.ServerReader.Close()
	c.State = func() error { return io.EOF }
	return io.EOF
}

func (c *Consumer) handleUnknown() error {
	return c.discardResponse()
}

func (c *Consumer) discardResponse() error {
	c.State = c.discardResponse
	c.log(3, "discarding response from server")
	line, err := c.ServerReader.ReadLine()
	if err != nil {
		return err
	}
	c.log(3, "discarded response from server:", string(line))
	c.State = c.readCommand
	return nil
}

func (c *Consumer) addEvent(evt model.Event) {
	c.Consumer.AddEvent(evt)
}

func (c *Consumer) log(level int, items ...interface{}) {
	if c.Logger != nil && debuglevel >= level {
		c.Logger.Log(items...)
	}
}
