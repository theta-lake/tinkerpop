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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// receiveOne returns the next Result already delivered to the result set, failing the test rather than blocking
// forever if nothing arrives.
func receiveOne(t *testing.T, resultSet ResultSet) *Result {
	t.Helper()
	select {
	case result := <-resultSet.Channel():
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("no result was delivered to the result set")
		return nil
	}
}

func getSyncMap() *synchronizedMap {
	return &synchronizedMap{
		make(map[string]ResultSet),
		sync.Mutex{},
	}
}

func TestChannelResultSet(t *testing.T) {
	const mockID = "mockID"

	t.Run("Test ResultSet test getter/setters.", func(t *testing.T) {
		r := newChannelResultSet(mockID, getSyncMap())
		testStatusAttribute := map[string]interface{}{
			"1": 1234,
			"2": "foo",
		}
		testAggregateTo := "test2"
		r.setStatusAttributes(testStatusAttribute)
		assert.Equal(t, r.GetStatusAttributes(), testStatusAttribute)
		r.setAggregateTo(testAggregateTo)
		assert.Equal(t, r.GetAggregateTo(), testAggregateTo)
	})

	t.Run("Test ResultSet close.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		assert.NotPanics(t, func() { channelResultSet.Close() })
	})

	t.Run("Test ResultSet close releases a producer blocked on a full channel.", func(t *testing.T) {
		channelResultSet := newChannelResultSetCapacity(mockID, getSyncMap(), 1)
		channelResultSet.addResult(&Result{"first"})

		producerDone := make(chan struct{})
		producerStarted := make(chan struct{})
		go func() {
			defer close(producerDone)
			close(producerStarted)
			// Nothing reads from the channel, so this send parks with channelMutex held until Close intervenes.
			channelResultSet.addResult(&Result{"second"})
		}()
		<-producerStarted
		time.Sleep(50 * time.Millisecond)

		// Close is run on its own goroutine because it must not be blocked by the channelMutex the parked producer
		// holds; before the fix it was, and the two deadlocked.
		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			channelResultSet.Close()
		}()

		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Close blocked on the mutex held by the producer")
		}

		select {
		case <-producerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not release the producer blocked on a full channel")
		}
	})

	t.Run("Test ResultSet error reaches a caller blocked in One.", func(t *testing.T) {
		container := getSyncMap()
		channelResultSet := newChannelResultSet(mockID, container)
		container.store(mockID, channelResultSet)

		const readers = 4
		errs := make(chan error, readers)
		for i := 0; i < readers; i++ {
			go func() {
				_, _, err := channelResultSet.One()
				errs <- err
			}()
			go channelResultSet.GetError()
		}

		// closeAll is what the read loop runs when a connection errors, on a different goroutine to the readers.
		go container.closeAll(newError(err0106ConnectionClosedPendingResultsErr))

		for i := 0; i < readers; i++ {
			select {
			case err := <-errs:
				assert.NotNil(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("One did not return after the result set was errored")
			}
		}
	})

	t.Run("Test ResultSet delivers buffered results before a pending error.", func(t *testing.T) {
		container := getSyncMap()
		channelResultSet := newChannelResultSet(mockID, container)
		container.store(mockID, channelResultSet)
		for i := 0; i < 3; i++ {
			channelResultSet.addResult(&Result{i})
		}

		// This is what a connection error or a graceful close does to every result set still in flight.
		container.closeAll(newError(err0106ConnectionClosedPendingResultsErr))

		for i := 0; i < 3; i++ {
			result, ok, err := channelResultSet.One()
			assert.Nil(t, err, "row %d was discarded in favour of the error", i)
			assert.True(t, ok)
			assert.Equal(t, i, result.Data)
		}
		// Only once drained does the error surface.
		result, ok, err := channelResultSet.One()
		assert.Nil(t, result)
		assert.False(t, ok)
		assert.NotNil(t, err)
	})

	t.Run("Test ResultSet addResult with nil data.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		assert.NotPanics(t, func() { channelResultSet.addResult(&Result{nil}) })
		assert.Nil(t, receiveOne(t, channelResultSet).Data)
	})

	t.Run("Test ResultSet addResult with a non-interface slice.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		data := []string{"a", "b"}
		assert.NotPanics(t, func() { channelResultSet.addResult(&Result{data}) })
		assert.Equal(t, data, receiveOne(t, channelResultSet).Data)
	})

	t.Run("Test ResultSet one.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		AddResults(channelResultSet, 10)
		idx := 0
		for i := 0; i < 10; i++ {
			result, ok, err := channelResultSet.One()
			assert.Nil(t, err)
			assert.True(t, ok)
			assert.Equal(t, result.GetString(), fmt.Sprintf("%v", idx))
			idx++
		}
		go closeAfterTime(500, channelResultSet)
		res, ok, err := channelResultSet.One()
		assert.Nil(t, err)
		assert.False(t, ok)
		assert.Nil(t, res)
	})

	t.Run("Test ResultSet one Paused.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		go AddResultsPause(channelResultSet, 10, 500)
		idx := 0
		for i := 0; i < 10; i++ {
			result, ok, err := channelResultSet.One()
			assert.Nil(t, err)
			assert.True(t, ok)
			assert.Equal(t, result.GetString(), fmt.Sprintf("%v", idx))
			idx++
		}
		go closeAfterTime(500, channelResultSet)
		result, ok, err := channelResultSet.One()
		assert.Nil(t, err)
		assert.False(t, ok)
		assert.Nil(t, result)
	})

	t.Run("Test ResultSet one close.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		channelResultSet.Close()
	})

	t.Run("Test ResultSet All.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		AddResults(channelResultSet, 10)
		go closeAfterTime(500, channelResultSet)
		results, err := channelResultSet.All()
		assert.Nil(t, err)
		for idx, result := range results {
			assert.Equal(t, result.GetString(), fmt.Sprintf("%v", idx))
		}
	})

	t.Run("Test ResultSet All close before.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		AddResults(channelResultSet, 10)
		channelResultSet.Close()
		results, err := channelResultSet.All()
		assert.Nil(t, err)
		assert.Equal(t, len(results), 10)
		for idx, result := range results {
			assert.Equal(t, result.GetString(), fmt.Sprintf("%v", idx))
		}
	})

	t.Run("Test ResultSet IsEmpty before signal.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		go closeAfterTime(500, channelResultSet)
		empty := channelResultSet.IsEmpty()
		assert.True(t, empty)
	})

	t.Run("Test ResultSet IsEmpty after signal.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		channelResultSet.Close()
		empty := channelResultSet.IsEmpty()
		assert.True(t, empty)
	})

	t.Run("Test ResultSet IsEmpty after close.", func(t *testing.T) {
		channelResultSet := newChannelResultSet(mockID, getSyncMap())
		go addAfterTime(500, channelResultSet)
		empty := channelResultSet.IsEmpty()
		assert.False(t, empty)
		channelResultSet.One()
		go closeAfterTime(500, channelResultSet)
		empty = channelResultSet.IsEmpty()
		assert.True(t, empty)
	})

	t.Run("Test ResultSet removes self from container.", func(t *testing.T) {
		container := getSyncMap()
		assert.Equal(t, 0, container.size())
		channelResultSet := newChannelResultSet(mockID, container)
		container.store(mockID, channelResultSet)
		assert.Equal(t, 1, container.size())
		channelResultSet.Close()
		assert.Equal(t, 0, container.size())
	})
}

func AddResultsPause(resultSet ResultSet, count int, ticks time.Duration) {
	for i := 0; i < count/2; i++ {
		resultSet.addResult(&Result{i})
	}
	time.Sleep(ticks * time.Millisecond)
	for i := count / 2; i < count; i++ {
		resultSet.addResult(&Result{i})
	}
}

func AddResults(resultSet ResultSet, count int) {
	for i := 0; i < count; i++ {
		resultSet.addResult(&Result{i})
	}
}

func closeAfterTime(millisecondTicks time.Duration, resultSet ResultSet) {
	time.Sleep(millisecondTicks * time.Millisecond)
	resultSet.Close()
}

func addAfterTime(millisecondTicks time.Duration, resultSet ResultSet) {
	time.Sleep(millisecondTicks * time.Millisecond)
	resultSet.addResult(&Result{1})
}
