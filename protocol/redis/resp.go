package redis

import (
	"errors"
	"strconv"
	"github.com/box/memsniff/assembly/reader"
)

type frameType int

const (
	tagStatus = '+'
	tagError = '-'
	tagInt = ':'
	tagBulk = '$'
	tagArray = '*'
)

var (
	ProtocolErr = errors.New("RESP protocol error")
)

type ParserOptions struct {
	BulkCaptureLimit int
}

type RespParser struct {
	stack []stackFrame
	Options ParserOptions
}

type stackFrame struct {
	frameType frameType
	run func() error
	result interface{}
}

func NewParser(r *reader.Reader) *RespParser {
	p := &RespParser{
		// start with root frame to contain eventual result
		stack: []stackFrame{{}},
	}
	p.startParseValue(r)
	return p
}

func (s *RespParser) Run() error {
	for {
		if len(s.stack) == 1 {
			return nil
		}
		err := s.stack[len(s.stack)-1].run()
		if err != nil {
			return err
		}
	}
}

func (s *RespParser) Result() interface{} {
	return s.stack[len(s.stack)-1].result
}

func (s *RespParser) push(f func() error) {
	s.stack = append(s.stack, stackFrame{run: f})
}

func (s *RespParser) pop(result interface{}) {
	s.stack = s.stack[:len(s.stack)-1]
	s.stack[len(s.stack)-1].result = result
}

func (s *RespParser) startParseValue(r *reader.Reader) {
	s.push(func() error {
		out, err := r.ReadN(1)
		if err != nil {
			return err
		}
		s.pop(nil)
		switch out[0] {
		case tagStatus:
			s.startParseSimpleString(r, false)
		case tagError:
			s.startParseSimpleString(r, true)
		case tagInt:
			s.startParseInt(r)
		case tagBulk:
			s.startParseBulk(r)
		case tagArray:
			s.startParseArray(r)
		default:
			return ProtocolErr
		}
		return nil
	})
}

func (s *RespParser) startParseSimpleString(r *reader.Reader, asError bool) {
	s.push(func() error {
		out, err := r.ReadLine()
		if err != nil {
			return err
		}
		if asError {
			s.pop(errors.New(string(out)))
		} else {
			s.pop(string(out))
		}
		return nil
	})
}

func (s *RespParser) startParseInt(r *reader.Reader) {
	s.push(func() error {
		out, err := r.ReadLine()
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(string(out))
		if err != nil {
			return err
		}
		s.pop(i)
		return nil
	})
}

func (s *RespParser) startParseBulk(r *reader.Reader) {
	// prepare handler to read and discard the body
	s.push(func() error {
		result := s.Result().(int)
		if result < 0 {
			// Redis "nil" result
			s.pop(nil)
			return nil
		}
		if result <= s.Options.BulkCaptureLimit {
			s.pop(nil)
			s.startParseBulkN(r, nil, result)
		} else {
			s.pop(result)
			r.Discard(s.Result().(int) + 2)
		}
		return nil
	})
	s.startParseInt(r)
}

func (s *RespParser) startParseBulkN(r *reader.Reader, accum []byte, n int) {
	s.push(func() error {
		out, err := r.ReadN(n)
		if err != nil {
			if err == reader.ErrShortRead {
				accum = append(accum, out...)
				s.pop(nil)
				r.Discard(len(out))
				s.startParseBulkN(r, accum, n - len(out))
			}
			return err
		}
		r.Discard(2)
		s.pop(append(accum, out...))
		return nil
	})
}

func (s *RespParser) startParseArray(r *reader.Reader) {
	s.push(func() error {
		n := s.Result().(int)
		s.pop(nil)
		s.stack[len(s.stack)-1].result = []interface{}{}
		s.startParseNArrayFields(r, n)
		return nil
	})
	s.startParseInt(r)
}

func (s *RespParser) startParseNArrayFields(r *reader.Reader, n int) {
	s.push(func() error {
		// value parsed
		result := s.Result()
		results := append(s.stack[len(s.stack)-2].result.([]interface{}), result)
		s.pop(results)
		if n > 1 {
			s.startParseNArrayFields(r, n-1)
			return nil
		}
		return nil
	})
	s.startParseValue(r)
}