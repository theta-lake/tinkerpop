/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"sync"
)

const defaultCapacity = 1000

// ResultSet interface to define the functions of a ResultSet.
//
// A ResultSet must be either drained (with All, or One until it reports no more results) or closed with Close.
// Abandoning one without doing either leaves the connection's read loop parked once more than the channel's capacity
// of results is buffered, which stalls the other requests in flight on that connection until it is closed or retired.
type ResultSet interface {
	setAggregateTo(val string)
	GetAggregateTo() string
	setStatusAttributes(statusAttributes map[string]interface{})
	GetStatusAttributes() map[string]interface{}
	GetRequestID() string
	IsEmpty() bool
	Close()
	unlockedClose()
	Channel() chan *Result
	addResult(result *Result)
	One() (*Result, bool, error)
	All() ([]*Result, error)
	GetError() error
	setError(error)
}

// channelResultSet Channel based implementation of ResultSet.
//
// Locks are acquired in the order container.syncLock, channelMutex, waitSignalMutex. fieldMutex is a leaf guarding
// err, aggregateTo and statusAttributes, which the read loop writes and the exported getters read, and is never held
// while acquiring another lock. It is deliberately separate from channelMutex: One reads err before receiving from the
// channel, and addResult holds channelMutex across a blocking send, so guarding these with channelMutex would let a
// full channel deadlock a reader against its own producer.
type channelResultSet struct {
	channel          chan *Result
	requestID        string
	container        *synchronizedMap
	aggregateTo      string
	statusAttributes map[string]interface{}
	closed           bool
	err              error
	waitSignal       chan bool
	// done is closed by closeDone before any closer contends for channelMutex, which releases an addResult parked on
	// a full channel.
	done            chan struct{}
	channelMutex    sync.Mutex
	waitSignalMutex sync.Mutex
	fieldMutex      sync.Mutex
	closeOnce       sync.Once
}

// closeDone signals that no further results will be consumed. It takes no locks so that it can be called before
// contending for channelMutex.
func (channelResultSet *channelResultSet) closeDone() {
	channelResultSet.closeOnce.Do(func() { close(channelResultSet.done) })
}

func (channelResultSet *channelResultSet) sendSignal() {
	// Lock wait
	channelResultSet.waitSignalMutex.Lock()
	defer channelResultSet.waitSignalMutex.Unlock()
	if channelResultSet.waitSignal != nil {
		channelResultSet.waitSignal <- true
		channelResultSet.waitSignal = nil
	}
}

// GetError returns error from the channelResultSet.
func (channelResultSet *channelResultSet) GetError() error {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	return channelResultSet.err
}

func (channelResultSet *channelResultSet) setError(err error) {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	channelResultSet.err = err
}

// IsEmpty returns true when the channelResultSet is empty.
func (channelResultSet *channelResultSet) IsEmpty() bool {
	channelResultSet.channelMutex.Lock()
	// If our channel is empty and we have no data in it, wait for signal that the state has been updated.
	if len(channelResultSet.channel) != 0 {
		// Channel is not empty.
		channelResultSet.channelMutex.Unlock()
		return false
	} else if channelResultSet.closed {
		// Channel is empty and closed.
		channelResultSet.channelMutex.Unlock()
		return true
	} else {
		// Channel is empty and not closed. Need to wait for signal that state has changed, otherwise
		// we do not know if it is empty or not.
		// We need to grab the wait signal mutex before we release the channel mutex.
		channelResultSet.waitSignalMutex.Lock()
		channelResultSet.channelMutex.Unlock()

		// Create a wait signal and unlock the wait signal mutex.
		waitSignal := make(chan bool)
		channelResultSet.waitSignal = waitSignal
		channelResultSet.waitSignalMutex.Unlock()

		// Technically if we assigned channelResultSet.waitSignal then unlocked, it could be set to nil or
		// overwritten to another channel before we check it, so to be safe, create additional variable and
		// check that instead.
		<-waitSignal
		return channelResultSet.IsEmpty()
	}
}

// Close can be used to close the channelResultSet.
func (channelResultSet *channelResultSet) Close() {
	// The container lock is taken first and unlockedClose then takes channelMutex. Taking them in this order in both
	// close paths is what keeps Close and synchronizedMap.closeAll from deadlocking against each other.
	channelResultSet.container.syncLock.Lock()
	defer channelResultSet.container.syncLock.Unlock()
	channelResultSet.unlockedClose()
}

// Close and remove from the channelResultSet from the container without locking container. Meant for use when calling
// function already locks the container.
func (channelResultSet *channelResultSet) unlockedClose() {
	// Released before channelMutex is requested, so an addResult parked on a full channel gives the mutex up.
	channelResultSet.closeDone()
	channelResultSet.channelMutex.Lock()
	if channelResultSet.closed {
		channelResultSet.channelMutex.Unlock()
		return
	}
	channelResultSet.closed = true
	delete(channelResultSet.container.internalMap, channelResultSet.requestID)
	close(channelResultSet.channel)
	channelResultSet.channelMutex.Unlock()
	channelResultSet.sendSignal()
}

func (channelResultSet *channelResultSet) setAggregateTo(val string) {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	channelResultSet.aggregateTo = val
}

// GetAggregateTo returns aggregateTo for the channelResultSet.
func (channelResultSet *channelResultSet) GetAggregateTo() string {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	return channelResultSet.aggregateTo
}

func (channelResultSet *channelResultSet) setStatusAttributes(val map[string]interface{}) {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	channelResultSet.statusAttributes = val
}

// GetStatusAttributes returns statusAttributes for the channelResultSet.
func (channelResultSet *channelResultSet) GetStatusAttributes() map[string]interface{} {
	channelResultSet.fieldMutex.Lock()
	defer channelResultSet.fieldMutex.Unlock()
	return channelResultSet.statusAttributes
}

// GetRequestID returns requestID for the channelResultSet.
func (channelResultSet *channelResultSet) GetRequestID() string {
	return channelResultSet.requestID
}

// Channel returns channel for the channelResultSet.
func (channelResultSet *channelResultSet) Channel() chan *Result {
	return channelResultSet.channel
}

// One returns the next Result from the channelResultSet, blocking until one is available.
// The value of ok is true if the value received was delivered by a successful send operation to the channel,
// or false if it is a zero value generated because the channel is closed and empty.
func (channelResultSet *channelResultSet) One() (*Result, bool, error) {
	// A result which already arrived is delivered ahead of a pending error, so erroring or closing the connection does
	// not discard rows the server had already sent. All has always drained first and reported the error afterwards.
	select {
	case result, ok := <-channelResultSet.channel:
		if ok {
			return result, true, nil
		}
		return nil, false, channelResultSet.GetError()
	default:
	}

	if err := channelResultSet.GetError(); err != nil {
		return nil, false, err
	}
	result, ok := <-channelResultSet.channel
	if err := channelResultSet.GetError(); err != nil {
		return nil, false, err
	}
	return result, ok, nil
}

// All returns all remaining results for the channelResultSet (results grabbed through One will not be present).
func (channelResultSet *channelResultSet) All() ([]*Result, error) {
	var results []*Result
	for result := range channelResultSet.channel {
		results = append(results, result)
	}
	return results, channelResultSet.GetError()
}

func (channelResultSet *channelResultSet) addResult(r *Result) {
	if channelResultSet.addResultLocked(r) {
		channelResultSet.sendSignal()
	}
}

// addResultLocked delivers r under channelMutex and reports whether anything was delivered. Holding channelMutex
// across the sends is what guarantees close(channel) cannot race with one.
func (channelResultSet *channelResultSet) addResultLocked(r *Result) bool {
	channelResultSet.channelMutex.Lock()
	defer channelResultSet.channelMutex.Unlock()
	if channelResultSet.closed {
		return false
	}
	// A type assertion rather than a reflect.Kind test: r.Data is nil for a server-sent null, which makes
	// reflect.TypeOf(r.Data) a nil reflect.Type, and it can be a slice whose element type is not interface{} when a
	// custom type reader is registered. Both are delivered as a single result instead of panicking.
	if data, ok := r.Data.([]interface{}); ok {
		for _, v := range data {
			if traverser, isTraverser := v.(*Traverser); isTraverser {
				for i := int64(0); i < traverser.bulk; i++ {
					if !channelResultSet.send(&Result{traverser.value}) {
						return false
					}
				}
			} else if !channelResultSet.send(&Result{v}) {
				return false
			}
		}
		return true
	}
	return channelResultSet.send(&Result{r.Data})
}

// send delivers one result, giving up if the result set is closed while the channel is full. Without the done case a
// consumer that stops reading parks this send forever with channelMutex held, which wedges the connection's read loop
// and every other request in flight on it, and makes Client.Close hang waiting for that goroutine.
func (channelResultSet *channelResultSet) send(result *Result) bool {
	select {
	case channelResultSet.channel <- result:
		return true
	case <-channelResultSet.done:
		return false
	}
}

func newChannelResultSetCapacity(requestID string, container *synchronizedMap, channelSize int) ResultSet {
	return &channelResultSet{
		channel:   make(chan *Result, channelSize),
		requestID: requestID,
		container: container,
		done:      make(chan struct{}),
	}
}

func newChannelResultSet(requestID string, container *synchronizedMap) ResultSet {
	return newChannelResultSetCapacity(requestID, container, defaultCapacity)
}
