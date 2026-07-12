// Package smartmutex implements an RWMutex with lock wait logging.
package smartmutex

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// enableLockWaitLogging controls whether we will log long mutex waits.
const enableLockWaitLogging = true

// warnTimeout is the time limit for showing the warning
const warnTimeout = 100 * time.Millisecond

// SmartMutex is a wrapper around sync.RWMutex that logs long waits.
type SmartMutex struct {
	sync.RWMutex

	// Name is an optional human-readable mutex identifier for slow-lock logs.
	// Set it before the mutex is used.
	Name string
}

// Lock locks the mutex, logging long waits if enabled.
func (m *SmartMutex) Lock() {
	if !enableLockWaitLogging {
		m.RWMutex.Lock()
		return
	}
	if m.TryLock() {
		return
	}

	stop := m.startTimer("write")
	m.RWMutex.Lock()
	stop()
}

// Unlock unlocks the mutex
func (m *SmartMutex) Unlock() {
	m.RWMutex.Unlock()
}

// RLock locks the mutex for reading, logging long waits if enabled.
func (m *SmartMutex) RLock() {
	if !enableLockWaitLogging {
		m.RWMutex.RLock()
		return
	}
	if m.TryRLock() {
		return
	}

	stop := m.startTimer("read")
	m.RWMutex.RLock()
	stop()
}

// RUnlock unlocks the mutex for reading
func (m *SmartMutex) RUnlock() {
	m.RWMutex.RUnlock()
}

// startTimer is a helper to start a timer that logs long waits if enabled.
func (m *SmartMutex) startTimer(lockType string) func() {
	start := time.Now()
	var pcs [32]uintptr
	frameCount := runtime.Callers(3, pcs[:])
	finished := atomic.Bool{}

	timer := time.AfterFunc(warnTimeout, func() {
		if finished.Load() {
			return
		}
		log.Printf(
			"SmartMutex %s %s lock wait exceeded %s after %s -- Stack:\n%s",
			m.logName(),
			lockType,
			warnTimeout,
			time.Since(start),
			formatStack(pcs[:frameCount]),
		)
	})

	return func() {
		finished.Store(true)
		timer.Stop()
	}
}

func (m *SmartMutex) logName() string {
	if m.Name != "" {
		return fmt.Sprintf("%q", m.Name)
	}
	return "<unnamed>"
}

func formatStack(pcs []uintptr) string {
	var b strings.Builder
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&b, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			break
		}
	}
	return b.String()
}
