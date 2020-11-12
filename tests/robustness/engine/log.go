// +build darwin,amd64 linux,amd64

package engine

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Log keeps track of the actions taken by the engine.
type Log struct {
	runOffset int
	Log       LogEntries
}

// LogEntries is a sequence of action log entries.
type LogEntries []*LogEntry

// LogEntry is an entry for the engine log.
type LogEntry struct {
	StartTime       time.Time
	EndTime         time.Time
	EngineTimestamp int64
	Action          ActionKey
	Error           string
	Idx             int64
	ActionOpts      map[string]string
	CmdOpts         map[string]string
}

func (e *LogEntry) String() string {
	var b strings.Builder

	e.WriteTo(&b) // nolint:errcheck

	return b.String()
}

// WriteTo implements io.WriterTo.
func (e *LogEntry) WriteTo(w io.Writer) (int64, error) {
	const timeResol = 100 * time.Millisecond

	n, err := fmt.Fprintf(w, "%4v t=%ds %s (%s): %v -> error=%s",
		e.Idx,
		e.EngineTimestamp,
		formatTime(e.StartTime),
		e.EndTime.Sub(e.StartTime).Round(timeResol),
		e.Action,
		e.Error,
	)

	return int64(n), err
}

func formatTime(tm time.Time) string {
	return tm.Format("2006/01/02 15:04:05 MST")
}

// WriteTo implements io.WriterTo.
func (l LogEntries) WriteTo(w io.Writer) (int64, error) {
	var t int64

	for _, e := range l {
		n, err := e.WriteTo(w)
		t += n

		if err != nil {
			return t, err
		}

		m, err := io.WriteString(w, "\n")
		t += int64(m)

		if err != nil {
			return t, err
		}
	}

	return t, nil
}

// StringThisRun returns a string of only the log entries generated
// by actions in this run of the engine.
func (l *Log) StringThisRun() string {
	var b strings.Builder

	l.Log[l.runOffset:].WriteTo(&b) // nolint:errcheck

	return b.String()
}

func (l *Log) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Log size:    %10v\n========\n", len(l.Log))
	l.Log.WriteTo(&b) // nolint:errcheck

	return b.String()
}

// AddEntry adds a LogEntry to the Log.
func (l *Log) AddEntry(e *LogEntry) {
	e.Idx = int64(len(l.Log))
	l.Log = append(l.Log, e)
}

// AddCompleted finalizes a log entry at the time it is called
// and with the provided error, before adding it to the Log.
func (l *Log) AddCompleted(e *LogEntry, err error) {
	e.EndTime = time.Now()
	if err != nil {
		e.Error = err.Error()
	}

	l.AddEntry(e)

	if len(l.Log) == 0 {
		panic("Did not get added")
	}
}

// FindLast finds the most recent log entry with the provided ActionKey.
func (l *Log) FindLast(actionKey ActionKey) *LogEntry {
	return l.findLastUntilIdx(actionKey, 0)
}

// FindLastThisRun finds the most recent log entry with the provided ActionKey,
// limited to the current run only.
func (l *Log) FindLastThisRun(actionKey ActionKey) (found *LogEntry) {
	return l.findLastUntilIdx(actionKey, l.runOffset)
}

func (l *Log) findLastUntilIdx(actionKey ActionKey, limitIdx int) *LogEntry {
	for i := len(l.Log) - 1; i >= limitIdx; i-- {
		entry := l.Log[i]
		if entry != nil && entry.Action == actionKey {
			return entry
		}
	}

	return nil
}

func setLogEntryCmdOpts(e *LogEntry, opts map[string]string) {
	if e == nil {
		return
	}

	e.CmdOpts = opts
}
