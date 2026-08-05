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
	"crypto/tls"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type connectionState int

const (
	initialized connectionState = iota + 1
	established
	closed
	closedDueToError
)

type connection struct {
	logHandler *logHandler
	protocol   protocol
	results    *synchronizedMap
	// state is atomic because errorCallback writes it from the read loop goroutine, which cannot take the pool's
	// loadBalanceLock that guards every other access: pool.write holds that lock while protocol.close(true) waits for
	// the read loop to exit, so taking it here would deadlock.
	state       atomic.Int32
	retireAfter time.Time // zero means never
	retiring    bool      // sticky once set
}

func (connection *connection) getState() connectionState {
	return connectionState(connection.state.Load())
}

func (connection *connection) setState(state connectionState) {
	connection.state.Store(int32(state))
}

type connectionSettings struct {
	authInfo                 AuthInfoProvider
	tlsConfig                *tls.Config
	keepAliveInterval        time.Duration
	writeDeadline            time.Duration
	connectionTimeout        time.Duration
	enableCompression        bool
	readBufferSize           int
	writeBufferSize          int
	maxResponseLength        int64
	enableUserAgentOnConnect bool
	maxConnectionLifetime    time.Duration
}

func (connection *connection) errorCallback() {
	connection.logHandler.log(Error, errorCallback)
	connection.setState(closedDueToError)

	// This callback is called from within protocol.readLoop. Therefore,
	// it cannot wait for it to finish to avoid a deadlock.
	// protocol is nil when the read loop errors before createConnection has assigned it.
	if connection.protocol == nil {
		return
	}
	if err := connection.protocol.close(false); err != nil {
		connection.logHandler.logf(Error, failedToCloseInErrorCallback, err.Error())
	}
}

func (connection *connection) close() error {
	if connection.getState() != established {
		return newError(err0101ConnectionCloseError)
	}
	connection.logHandler.log(Info, closeConnection)
	var err error
	if connection.protocol != nil {
		// Errored and closed first so that the read loop is not parked delivering results to a consumer which stopped
		// reading. protocol.close(true) waits for that goroutine to exit, so the wait would otherwise never return.
		// On a graceful close the read loop returns without closing them itself, so this also stops callers blocked in
		// One from waiting on a connection that is going away.
		connection.results.closeAll(newError(err0106ConnectionClosedPendingResultsErr))
		err = connection.protocol.close(true)
	}
	connection.setState(closed)
	return err
}

func (connection *connection) write(request *request) (ResultSet, error) {
	if connection.getState() != established {
		return nil, newError(err0102WriteConnectionClosedError)
	}
	connection.logHandler.log(Debug, writeRequest)
	requestID := request.requestID.String()
	connection.logHandler.logf(Debug, creatingRequest, requestID)
	resultSet := newChannelResultSet(requestID, connection.results)
	connection.results.store(requestID, resultSet)
	return resultSet, connection.protocol.write(request)
}

func (connection *connection) activeResults() int {
	return connection.results.size()
}

// shouldRetire reports whether the connection has reached the end of its configured lifetime and should stop accepting
// new work. A zero retireAfter means the connection never retires.
func (connection *connection) shouldRetire(now time.Time) bool {
	return !connection.retireAfter.IsZero() && now.After(connection.retireAfter)
}

// retirementDeadline returns the absolute time at which a connection created at now should stop accepting new work.
// A zero time means never. The lifetime is jittered into [0.8 * maxLifetime, maxLifetime) so that connections do not
// all rotate at the same instant.
func retirementDeadline(now time.Time, maxLifetime time.Duration) time.Time {
	if maxLifetime <= 0 {
		return time.Time{}
	}
	span := int64(maxLifetime) / 5
	if span <= 0 {
		return now.Add(maxLifetime)
	}
	return now.Add(time.Duration(int64(maxLifetime) - span + rand.Int64N(span)))
}

// createConnection establishes a connection with the given parameters. A connection should always be closed to avoid
// leaking connections. The connection has the following states:
//
//	initialized: connection struct is created but has not established communication with server
//	established: connection has established communication established with the server
//	closed: connection was closed by the user.
//	closedDueToError: connection was closed internally due to an error.
func createConnection(url string, logHandler *logHandler, connSettings *connectionSettings) (*connection, error) {
	conn := &connection{
		logHandler: logHandler,
		protocol:   nil,
		results:    &synchronizedMap{map[string]ResultSet{}, sync.Mutex{}},
	}
	conn.setState(initialized)
	logHandler.log(Info, connectConnection)
	// newGremlinServerWSProtocol starts the read loop, so conn.errorCallback can race the writes below from here on.
	protocol, err := newGremlinServerWSProtocol(logHandler, Gorilla, url, connSettings, conn.results, conn.errorCallback)
	if err != nil {
		logHandler.logf(Warning, failedConnection)
		conn.setState(closedDueToError)
		return nil, err
	}
	conn.protocol = protocol
	// Compare and swap rather than store: the read loop is already running and errorCallback may have set
	// closedDueToError, in which case an unconditional store would mark a connection whose transporter is closed and
	// whose read loop has exited as established. The pool never prunes an established connection, so it would occupy a
	// slot for the life of the process with result sets that can never drain.
	if !conn.state.CompareAndSwap(int32(initialized), int32(established)) {
		logHandler.logf(Warning, failedConnection)
		return nil, newError(err0107ConnectionErroredWhileConnectingErr)
	}
	conn.retireAfter = retirementDeadline(time.Now(), connSettings.maxConnectionLifetime)
	return conn, err
}

type synchronizedMap struct {
	internalMap map[string]ResultSet
	syncLock    sync.Mutex
}

func (s *synchronizedMap) store(key string, value ResultSet) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	s.internalMap[key] = value
}

func (s *synchronizedMap) load(key string) ResultSet {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return s.internalMap[key]
}

func (s *synchronizedMap) delete(key string) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	delete(s.internalMap, key)
}

func (s *synchronizedMap) size() int {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return len(s.internalMap)
}

func (s *synchronizedMap) closeAll(err error) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	for _, resultSet := range s.internalMap {
		resultSet.setError(err)
		resultSet.unlockedClose()
	}
}
