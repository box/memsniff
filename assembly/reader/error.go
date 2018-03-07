package reader

import "fmt"

var (
	// ErrShortRead is returned if there is insufficient data in the buffer.
	ErrShortRead = fmt.Errorf("Insufficient data to complete read")
)

// ErrLostData is returned when there is a gap in the TCP stream due to missing
// or late packets. It is returned only once for each gap. Successive read
// attempts will proceed, returning the next available data.
type ErrLostData struct {
	Lost int
}

func (e ErrLostData) Error() string {
	if e.Lost < 0 {
		return "lost unknown amount of data from stream start"
	}
	return fmt.Sprint("lost ", e.Lost, " bytes from stream")
}
