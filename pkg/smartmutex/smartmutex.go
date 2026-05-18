// Package smartmutex implements a RWMutex but with timers to assist with deadlock detection
package smartmutex

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// enableDeadlockDetection controls whether we will detect deadlocks and log them
const enableDeadlockDetection = true

// warnTimeout is the time limit for showing the warning
const warnTimeout = 100 * time.Millisecond

// SmartMutex is a wrapper around sync.RWMutex that will detect deadlocks and log them
type SmartMutex struct {
	sync.RWMutex
}

// Lock locks the mutex, detecting deadlocks if enabled
func (m *SmartMutex) Lock() {
	stop := m.startTimer()
	m.RWMutex.Lock()
	stop()
}

// Unlock unlocks the mutex
func (m *SmartMutex) Unlock() {
	m.RWMutex.Unlock()
}

// RLock locks the mutex for reading, detecting deadlocks if enabled
func (m *SmartMutex) RLock() {
	stop := m.startTimer()
	m.RWMutex.RLock()
	stop()
}

// RUnlock unlocks the mutex for reading
func (m *SmartMutex) RUnlock() {
	m.RWMutex.RUnlock()
}

// startTimer is a helper to start a timer that will detect deadlocks if enabled
func (m *SmartMutex) startTimer() func() {
	if !enableDeadlockDetection {
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
			log.Printf("Deadlock detected -- Stack:\n%s\n", savedStack)
		}
	}()

	return ret
}
