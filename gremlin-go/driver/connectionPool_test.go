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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

// Arbitrarily high value to use to not trigger creation of new connections
const newConnectionThreshold = 100
const maxConcurrentConnections = 4

var logger = newLogHandler(&defaultLogger{}, Info, language.English)

func getPoolForTesting() *loadBalancingPool {
	return &loadBalancingPool{
		url:                      "",
		connSettings:             newDefaultConnectionSettings(),
		logHandler:               newLogHandler(&defaultLogger{}, Info, language.English),
		newConnectionThreshold:   newConnectionThreshold,
		maxConcurrentConnections: maxConcurrentConnections,
		connections:              nil,
		loadBalanceLock:          sync.Mutex{},
	}
}

func getMockConnection() *connection {
	return &connection{
		logHandler: logger,
		protocol:   nil,
		results: &synchronizedMap{
			internalMap: make(map[string]ResultSet),
			syncLock:    sync.Mutex{},
		},
		state: established,
	}
}

// getExpiredMockConnection returns a mock connection whose retirement deadline has already passed.
func getExpiredMockConnection() *connection {
	conn := getMockConnection()
	conn.retireAfter = time.Now().Add(-time.Minute)
	return conn
}

func TestConnectionPool(t *testing.T) {
	t.Run("loadBalancingPool", func(t *testing.T) {
		smallMap := make(map[string]ResultSet)
		bigMap := make(map[string]ResultSet)
		for i := 1; i < 4; i++ {
			bigMap[strconv.Itoa(i)] = nil
			if i < 3 {
				smallMap[strconv.Itoa(i)] = nil
			}
		}

		t.Run("getLeastUsedConnection", func(t *testing.T) {
			t.Run("getting the least used connection", func(t *testing.T) {
				pool := getPoolForTesting()
				defer pool.close()
				mockConnection1 := getMockConnection()
				mockConnection2 := getMockConnection()
				mockConnection3 := getMockConnection()
				mockConnection1.results.internalMap = bigMap
				mockConnection2.results.internalMap = smallMap
				mockConnection3.results.internalMap = bigMap
				connections := []*connection{mockConnection1, mockConnection2, mockConnection3}
				pool.connections = connections

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Equal(t, mockConnection2, connection)
			})

			t.Run("purge non-established connections", func(t *testing.T) {
				pool := getPoolForTesting()
				defer pool.close()
				mockConnection := getMockConnection()
				mockConnection.results.internalMap = smallMap
				nonEstablished := &connection{
					logHandler: logger,
					protocol:   nil,
					results:    nil,
					state:      closed,
				}
				connections := []*connection{nonEstablished, mockConnection}
				pool.connections = connections

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Equal(t, mockConnection, connection)
				assert.Len(t, pool.connections, 1)
			})

			t.Run("no retirement when max connection lifetime is disabled", func(t *testing.T) {
				pool := getPoolForTesting()
				defer pool.close()
				mockConnection := getMockConnection()
				pool.connections = []*connection{mockConnection}

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, mockConnection, connection)
				assert.False(t, mockConnection.retiring)
				assert.Equal(t, established, mockConnection.state)
				assert.Len(t, pool.connections, 1)
			})

			t.Run("expired and idle connection is closed and dropped", func(t *testing.T) {
				pool := getPoolForTesting()
				defer pool.close()
				expired := getExpiredMockConnection()
				fresh := getMockConnection()
				pool.connections = []*connection{expired, fresh}

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, fresh, connection)
				assert.Equal(t, closed, expired.state)
				assert.Len(t, pool.connections, 1)
				assert.NotContains(t, pool.connections, expired)
			})

			t.Run("expired connection drains before it is closed", func(t *testing.T) {
				pool := getPoolForTesting()
				defer pool.close()
				expired := getExpiredMockConnection()
				expired.results.store("in-flight", nil)
				fresh := getMockConnection()
				pool.connections = []*connection{expired, fresh}

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, fresh, connection)
				assert.True(t, expired.retiring)
				assert.Equal(t, established, expired.state)
				assert.Len(t, pool.connections, 2)

				// Once drained, the next selection closes and drops it.
				expired.results.delete("in-flight")
				connection, err = pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, fresh, connection)
				assert.Equal(t, closed, expired.state)
				assert.Len(t, pool.connections, 1)
				assert.NotContains(t, pool.connections, expired)
			})

			t.Run("draining connection is a fallback when the replacement cannot be dialled", func(t *testing.T) {
				pool := getPoolForTesting()
				pool.maxConcurrentConnections = 1
				defer pool.close()
				expired := getExpiredMockConnection()
				expired.results.store("in-flight", nil)
				pool.connections = []*connection{expired}

				// The pool url is empty so dialling the replacement fails. Rather than failing the request, the
				// draining connection is used, since it is still healthy.
				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, expired, connection)
				assert.True(t, expired.retiring)
				assert.Len(t, pool.connections, 1)
			})

			t.Run("draining connections cannot grow the pool without bound", func(t *testing.T) {
				pool := getPoolForTesting()
				pool.maxConcurrentConnections = 1
				defer pool.close()
				// Two connections which are retiring but will never drain, already at the hard ceiling of
				// 2 * maxConcurrentConnections.
				stuck := getExpiredMockConnection()
				stuck.results.store("in-flight-1", nil)
				stuck.results.store("in-flight-2", nil)
				leastUsedStuck := getExpiredMockConnection()
				leastUsedStuck.results.store("in-flight-1", nil)
				pool.connections = []*connection{stuck, leastUsedStuck}

				connection, err := pool.getLeastUsedConnection()
				assert.Nil(t, err)
				assert.Same(t, leastUsedStuck, connection)
				assert.Len(t, pool.connections, 2)
			})
		})

		t.Run("close", func(t *testing.T) {
			pool := getPoolForTesting()
			empty := &synchronizedMap{
				internalMap: make(map[string]ResultSet),
				syncLock:    sync.Mutex{},
			}
			openConn1 := &connection{
				logHandler: logger,
				protocol:   nil,
				results:    empty,
				state:      established,
			}
			openConn2 := &connection{
				logHandler: logger,
				protocol:   nil,
				results:    empty,
				state:      established,
			}
			connections := []*connection{openConn1, openConn2}
			pool.connections = connections

			pool.close()
			assert.Equal(t, closed, openConn1.state)
			assert.Equal(t, closed, openConn2.state)
		})

		t.Run("close with a retiring connection which has not drained", func(t *testing.T) {
			pool := getPoolForTesting()
			retiring := getExpiredMockConnection()
			retiring.results.store("in-flight", nil)
			retiring.retiring = true
			pool.connections = []*connection{retiring}

			pool.close()
			assert.Equal(t, closed, retiring.state)
		})
	})
}
