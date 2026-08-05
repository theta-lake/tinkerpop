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
	"time"
)

type connectionPool interface {
	write(*request) (ResultSet, error)
	close()
}

const defaultNewConnectionThreshold = 4
const defaultInitialConcurrentConnections = 1

// loadBalancingPool has two configurations: maxConcurrentConnections and newConnectionThreshold.
// maxConcurrentConnections denotes the maximum amount of active connections at any given time.
// newConnectionThreshold specifies the minimum amount of concurrent active traversals on the least used connection
// which will trigger creation of a new connection if maxConcurrentConnections has not been reached.
// loadBalancingPool will use the least-used connection, and as a part of the process, getLeastUsedConnection(), will
// remove any errored connections from the pool and ensure that the returned connection is usable.
type loadBalancingPool struct {
	url          string
	logHandler   *logHandler
	connSettings *connectionSettings

	newConnectionThreshold   int
	maxConcurrentConnections int
	connections              []*connection
	loadBalanceLock          sync.Mutex
	isClosed                 bool
}

func (pool *loadBalancingPool) close() {
	pool.loadBalanceLock.Lock()
	defer pool.loadBalanceLock.Unlock()

	if !pool.isClosed {
		for _, connection := range pool.connections {
			err := connection.close()
			if err != nil {
				pool.logHandler.logf(Warning, errorClosingConnection, err.Error())
			}
		}
		pool.isClosed = true
	}
}

func (pool *loadBalancingPool) write(request *request) (ResultSet, error) {
	pool.loadBalanceLock.Lock()
	defer pool.loadBalanceLock.Unlock()

	if pool.isClosed {
		return nil, newError(err0103ConnectionPoolClosedError)
	}

	conn, err := pool.getLeastUsedConnection()
	if err != nil {
		return nil, err
	}
	return conn.write(request)
}

// canDial reports whether the pool may open another connection. Connections which are draining after retirement are
// excluded from activeCount so that they can never block a replacement, which lets the pool transiently hold more than
// maxConcurrentConnections. The hard ceiling bounds that overshoot, so a connection which never drains cannot leak
// connections indefinitely.
// Not thread-safe. Should only be called by getLeastUsedConnection.
func (pool *loadBalancingPool) canDial(activeCount int) bool {
	return activeCount < pool.maxConcurrentConnections &&
		len(pool.connections) < 2*pool.maxConcurrentConnections
}

// Not thread-safe. Should only be called by write which ensures no concurrency.
func (pool *loadBalancingPool) getLeastUsedConnection() (*connection, error) {
	// newConnection should only be called within getLeastUsedConnection and therefore is a lambda.
	newConnection := func() (*connection, error) {
		connection, err := createConnection(pool.url, pool.logHandler, pool.connSettings)
		if err != nil {
			return nil, err
		}
		pool.connections = append(pool.connections, connection)
		return connection, nil
	}

	// If our pool is empty, return a new connection.
	if len(pool.connections) == 0 {
		return newConnection()
	}

	// Remove connections which are dead or fully drained after retirement, and find least used.
	now := time.Now()
	var leastUsed *connection = nil
	// leastUsedRetiring is only used as a fallback when the pool has nothing else to offer. Handing new work to a
	// draining connection delays its retirement, which is preferable to failing the request.
	var leastUsedRetiring *connection = nil
	// activeCount counts connections which are still eligible for new work. Retiring connections are excluded, so they
	// do not block the pool from dialling a replacement.
	activeCount := 0
	validConnections := make([]*connection, 0, pool.maxConcurrentConnections)
	for _, connection := range pool.connections {
		state := connection.getState()
		if state != established && state != initialized {
			continue
		}

		if connection.shouldRetire(now) {
			if !connection.retiring {
				connection.retiring = true
				pool.logHandler.logf(Debug, connectionRetiring, connection.activeResults())
			}
			if connection.activeResults() == 0 {
				// Drained, so it can be closed without dropping any in-flight work.
				if err := connection.close(); err != nil {
					pool.logHandler.logf(Warning, errorClosingConnection, err.Error())
				}
				continue
			}
		}

		validConnections = append(validConnections, connection)

		if connection.retiring {
			// Still draining its existing result sets, so it must not be given new work unless it is all the pool has.
			if connection.getState() == established &&
				(leastUsedRetiring == nil || connection.activeResults() < leastUsedRetiring.activeResults()) {
				leastUsedRetiring = connection
			}
			continue
		}
		activeCount++

		if connection.getState() == established {
			// Set the least used connection.
			if leastUsed == nil || connection.activeResults() < leastUsed.activeResults() {
				leastUsed = connection
			}
		}
	}
	pool.connections = validConnections

	if leastUsed == nil {
		// If no valid connection is found.
		if !pool.canDial(activeCount) {
			if leastUsedRetiring != nil {
				// The pool cannot grow, so fall back to a draining connection rather than failing the request.
				return leastUsedRetiring, nil
			}
			// Return error if pool is full and no valid connection was found (should not ever happen).
			return nil, newError(err0105ConnectionPoolFullButNoneValid)
		}
		// Return new connection if no valid connection was found and pool has capacity.
		newConnection, err := newConnection()
		if err != nil {
			if leastUsedRetiring != nil {
				// New connection creation failed; a draining connection is still usable.
				pool.logHandler.logf(Warning, poolNewConnectionError, err.Error())
				return leastUsedRetiring, nil
			}
			return nil, err
		}
		return newConnection, nil
	} else if leastUsed.activeResults() >= pool.newConnectionThreshold && pool.canDial(activeCount) {
		// If the number of active results in our least used connection has reached the threshold
		// AND our pool size has not reached the capacity, attempt to return a new connection.
		newConnection, err := newConnection()
		if err != nil {
			// New connection creation failed; use existing grabbed least-used connection.
			pool.logHandler.logf(Warning, poolNewConnectionError, err.Error())
			return leastUsed, nil
		}
		return newConnection, nil
	} else {
		// If our pool is at capacity OR our least busy connection is less than the threshold, return the least used connection.
		return leastUsed, nil
	}
}

func newLoadBalancingPool(url string, logHandler *logHandler, connSettings *connectionSettings,
	newConnectionThreshold int, maximumConcurrentConnections int, initialConcurrentConnections int) (connectionPool, error) {
	var wg sync.WaitGroup
	wg.Add(initialConcurrentConnections)
	var appendLock sync.Mutex

	pool := make([]*connection, 0, maximumConcurrentConnections)
	errorList := make([]error, 0, maximumConcurrentConnections)
	for i := 0; i < initialConcurrentConnections; i++ {
		go func() {
			defer wg.Done()
			connection, err := createConnection(url, logHandler, connSettings)

			appendLock.Lock()
			defer appendLock.Unlock()
			if err != nil {
				logHandler.logf(Warning, createConnectionError, err.Error())
				errorList = append(errorList, err)
			} else {
				pool = append(pool, connection)
			}
		}()
	}

	wg.Wait()
	if len(pool) == 0 && len(errorList) != 0 {
		// If all instantiation fails return the first error's details.
		return nil, newError(err0104ConnectionPoolInstantiateFail, errorList[0].Error())
	}
	return &loadBalancingPool{
		url:                      url,
		logHandler:               logHandler,
		connSettings:             connSettings,
		newConnectionThreshold:   newConnectionThreshold,
		maxConcurrentConnections: maximumConcurrentConnections,
		connections:              pool,
	}, nil
}
