// Package smartmutex implements an RWMutex with lock wait logging.
package smartmutex

import (
	"context"
	"log"
	"runtime/debug"
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
}

// Lock locks the mutex, logging long waits if enabled.
func (m *SmartMutex) Lock() {
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
	if !enableLockWaitLogging {
		return func() {}
	}

	savedStack := debug.Stack()
	ctx, cancel := context.WithTimeout(context.Background(), warnTimeout)
	finishedHappily := atomic.Bool{}
	ret := func() {
		finishedHappily.Store(true)
		cancel()
	}

	go func() {
		<-ctx.Done()
		if !finishedHappily.Load() {
			log.Printf("SmartMutex %s lock wait exceeded %s -- Stack:\n%s\n", lockType, warnTimeout, savedStack)
		}
	}()

	return ret
}
