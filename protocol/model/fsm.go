package model

type ProtocolType uint8

const (
	ProtocolUnknown ProtocolType = iota
	ProtocolInfer
	ProtocolMemcacheText
	ProtocolRedis
)

func GetProtocolType(protocol string) ProtocolType {
	switch protocol {
	case "infer":
		return ProtocolInfer
	case "mctext":
		return ProtocolMemcacheText
	case "redis":
		return ProtocolRedis
	default:
		return ProtocolUnknown
	}
}

// Fsm is a finite-state machine that parses network traffic from a Consumer
// and produces events to that Consumer.
type Fsm interface {
	SetConsumer(consumer *Consumer)
	Run()
}

type noopFsm struct{}

func (f noopFsm) Run()                  {}
func (f noopFsm) SetConsumer(*Consumer) {}
