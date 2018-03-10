package model

// Fsm is a finite-state machine that parses network traffic from a Consumer
// and produces events to that Consumer.
type Fsm interface {
	SetConsumer(consumer *Consumer)
	Run()
}

type noopFsm struct{}

func (f noopFsm) Run()                  {}
func (f noopFsm) SetConsumer(*Consumer) {}
