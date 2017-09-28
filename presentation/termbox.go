package presentation

import (
	"errors"
	"fmt"
	"github.com/box/memsniff/analysis"
	"github.com/mattn/go-runewidth"
	"github.com/nsf/termbox-go"
	"strconv"
	"time"
)

const (
	numColumns  = 12
	statusLines = 1
	logLines    = 4
)

var (
	errQuitRequested = errors.New("user requested to quit")
)

func (u *uiContext) runTermbox() error {
	err := termbox.Init()
	if err != nil {
		return err
	}
	defer func() {
		// ensure that the termboxEvents goroutine shuts down
		termbox.Interrupt()
		termbox.Close()
	}()

	return u.eventLoop()
}

func (u *uiContext) eventLoop() error {
	updateTick := time.NewTicker(u.interval)
	defer updateTick.Stop()
	events := termboxEvents()
	if err := u.update(); err != nil {
		return err
	}

	for {
		select {
		case <-updateTick.C:
			if err := u.update(); err != nil {
				return err
			}

		case msg := <-u.msgChan:
			u.handleNewMessage(msg)

		case ev := <-events:
			if err := u.handleEvent(ev); err != nil {
				if err == errQuitRequested {
					return nil
				}
				return err
			}
		}
	}
}

func termboxEvents() <-chan termbox.Event {
	ch := make(chan termbox.Event)
	go func() {
		for {
			ev := termbox.PollEvent()
			if ev.Type == termbox.EventInterrupt {
				break
			}
			ch <- ev
		}
	}()
	return ch
}

func (u *uiContext) handleEvent(ev termbox.Event) error {
	switch ev.Type {
	case termbox.EventKey:
		if ev.Ch == 'p' {
			u.handlePause()
		}
		if ev.Ch == 'q' || ev.Key == termbox.KeyCtrlC {
			return errQuitRequested
		}
		if ev.Key == termbox.KeyCtrlL {
			if err := u.update(); err != nil {
				return err
			}
			if err := termbox.Sync(); err != nil {
				return err
			}
		}

	case termbox.EventResize:
		if err := u.update(); err != nil {
			return err
		}
	}
	return nil
}

func (u *uiContext) handlePause() {
	u.paused = !u.paused
	if u.paused {
		u.Log("Updates paused")
	} else {
		u.Log("Updates unpaused")
	}
}

func (u *uiContext) handleNewMessage(msg string) {
	if len(u.messages) < logLines {
		u.messages = append(u.messages, msg)
	} else {
		u.messages = append(u.messages[1:], msg)
	}
}

func renderHeader(rep analysis.Report) {
	var col int
	for _, h := range rep.KeyColNames {
		renderText(col, 0, h)
		col += 4
	}
	for _, h := range rep.ValColNames {
		renderText(col, 0, h)
		col++
	}
	renderLine(0, 12, 1, '-')
}

func renderReport(rep analysis.Report) {
	lastY := yFromBottom(statusLines + logLines)
	for i, r := range rep.Rows {
		col := 0
		y := i + 2
		for _, h := range r.Key {
			renderText(col, y, h)
			col += 4
		}
		for _, v := range r.Values {
			renderText(col, y, strconv.Itoa(int(v)))
			col++
		}
		y++
		if y > lastY {
			break
		}
	}
}

func (u *uiContext) renderMessages() {
	for i, msg := range u.messages {
		renderText(0, yFromBottom(i+statusLines), msg)
	}
}

func (u *uiContext) renderFooter(rep analysis.Report) {
	y := yFromBottom(0)
	stats := u.statProvider()
	renderText(0, y, rep.Timestamp.Format("15:04:05.000"))

	renderText(2, y, dropLabel(stats))
	renderText(4, y, fmt.Sprintf("Packets: %10d", stats.PacketsPassedFilter))
	renderText(6, y, fmt.Sprintf("GET responses: %10d", stats.ResponsesParsed))
}

func dropLabel(s Stats) string {
	var dropRate float64
	if s.PacketsPassedFilter == 0 {
		dropRate = 0
	} else {
		dropRate = float64(s.PacketsDroppedTotal) / float64(s.PacketsPassedFilter)
	}

	return fmt.Sprintf("Dropped: %d+%d+%d=%d (%5.2f%%)",
		s.PacketsDroppedKernel, s.PacketsDroppedParser,
		s.PacketsDroppedAnalysis, s.PacketsDroppedTotal, dropRate*100)
}

func renderText(column int, y int, txt string) {
	x := columnX(column)
	runes := []rune(txt)

	for _, r := range runes {
		termbox.SetCell(x, y, r, termbox.ColorDefault, termbox.ColorDefault)
		x += runewidth.RuneWidth(r)
	}
}

func renderLine(column int, span int, y int, ch rune) {
	w := runewidth.RuneWidth(ch)
	for x := columnX(column); x < columnX(column+span); x += w {
		termbox.SetCell(x, y, ch, termbox.ColorDefault, termbox.ColorDefault)
	}
}

func columnX(col int) int {
	w, _ := termbox.Size()
	if col >= numColumns {
		return w
	}
	return w * col / numColumns
}

func yFromBottom(n int) int {
	_, h := termbox.Size()
	return h - 1 - n
}

func (u *uiContext) update() error {
	err := termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
	if err != nil {
		return err
	}

	// Continue to clear the accumulated data every interval even when paused
	// so we don't get a big burst of data on unpause.
	rep := u.analysis.Report(!u.cumulative)
	if !u.paused {
		u.prevReport = rep
		u.prevReport.SortBy(-2)
	}
	renderHeader(u.prevReport)
	renderReport(u.prevReport)
	u.renderFooter(u.prevReport)
	u.renderMessages()

	return termbox.Flush()
}
