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
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

// stubTransporter feeds readLoop a canned response and records whether it was closed.
type stubTransporter struct {
	message []byte
	sent    bool
	closed  atomic.Bool
}

func (transporter *stubTransporter) Connect() error { return nil }

func (transporter *stubTransporter) Write([]byte) error { return nil }

func (transporter *stubTransporter) Read() ([]byte, error) {
	if transporter.sent {
		// The read loop only reaches a second read if it did not treat the first response as fatal.
		return nil, errors.New("no more messages")
	}
	transporter.sent = true
	return transporter.message, nil
}

func (transporter *stubTransporter) Close() error {
	transporter.closed.Store(true)
	return nil
}

func (transporter *stubTransporter) IsClosed() bool { return transporter.closed.Load() }

func (transporter *stubTransporter) getAuthInfo() AuthInfoProvider { return &AuthInfo{} }

// The read loop owns closing its transport, because errorCallback runs before createConnection has assigned the
// protocol to the connection and so cannot do it. Without this the socket and the write loop are stranded whenever a
// malformed frame arrives, which is the case the deserializer's recover made reachable in the first place.
func TestReadLoopClosesItsTransportOnAMalformedResponse(t *testing.T) {
	transport := &stubTransporter{message: []byte(`{"message":"502 Bad Gateway"}`)}
	wg := &sync.WaitGroup{}
	protocol := &gremlinServerWSProtocol{
		protocolBase: &protocolBase{transporter: transport},
		serializer:   newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English)),
		logHandler:   newLogHandler(&defaultLogger{}, Error, language.English),
		mutex:        sync.Mutex{},
		wg:           wg,
	}

	results := &synchronizedMap{internalMap: make(map[string]ResultSet), syncLock: sync.Mutex{}}
	resultSet := newChannelResultSet("pending", results)
	results.store("pending", resultSet)

	// errorCallback deliberately touches nothing but connection state, so it cannot be what closes the transport.
	var callbackFired atomic.Bool
	wg.Add(1)
	protocol.readLoop(results, func() { callbackFired.Store(true) })

	assert.True(t, transport.IsClosed(), "read loop exited without closing its transport")
	assert.True(t, callbackFired.Load(), "read loop exited without reporting the error")
	assert.NotNil(t, resultSet.GetError(), "in-flight result set was not errored")
	assert.Equal(t, 0, results.size(), "in-flight result set was not removed from the connection")
}

// Closing a ResultSet is the documented way to abandon a request, and it removes the result set from the connection,
// so the frames the server keeps sending for it must not take the connection down with every other request on it.
func TestResponseHandlerIgnoresAFrameForAClosedResultSet(t *testing.T) {
	protocol := &gremlinServerWSProtocol{
		protocolBase: &protocolBase{transporter: &stubTransporter{}},
		logHandler:   newLogHandler(&defaultLogger{}, Error, language.English),
		mutex:        sync.Mutex{},
		wg:           &sync.WaitGroup{},
	}

	results := &synchronizedMap{internalMap: make(map[string]ResultSet), syncLock: sync.Mutex{}}
	survivor := newChannelResultSet("survivor", results)
	results.store("survivor", survivor)

	abandoned := response{responseID: uuid.New(), responseStatus: responseStatus{code: http.StatusOK}}
	err := protocol.responseHandler(results, abandoned)

	assert.Nil(t, err, "a frame for a closed result set must not be fatal to the connection")
	assert.Nil(t, survivor.GetError(), "an unrelated in-flight request was errored")
	assert.Equal(t, 1, results.size(), "an unrelated in-flight request was dropped")
}

func TestProtocol(t *testing.T) {
	t.Run("Test protocol connect error.", func(t *testing.T) {
		connSettings := newDefaultConnectionSettings()
		connSettings.authInfo, connSettings.tlsConfig = nil, nil
		connSettings.keepAliveInterval, connSettings.writeDeadline, connSettings.writeDeadline = keepAliveIntervalDefault, writeDeadlineDefault, connectionTimeoutDefault

		protocol, err := newGremlinServerWSProtocol(newLogHandler(&defaultLogger{}, Info, language.English), Gorilla,
			"ws://localhost:9000/gremlin", connSettings,
			nil, nil)
		assert.NotNil(t, err)
		assert.Nil(t, protocol)
	})

	t.Run("Test protocol close wait", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		protocol := &gremlinServerWSProtocol{
			closed: true,
			mutex:  sync.Mutex{},
			wg:     wg,
		}
		wg.Add(1)

		done := make(chan bool)

		go func() {
			protocol.close(true)
			done <- true
		}()

		select {
		case <-time.After(1 * time.Second):
			// Ok. Close must wait.
		case <-done:
			t.Fatal("protocol.close is not waiting")
		}
	})

	t.Run("Test protocol close no wait", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		protocol := &gremlinServerWSProtocol{
			closed: true,
			mutex:  sync.Mutex{},
			wg:     wg,
		}
		wg.Add(1)

		done := make(chan bool)

		go func() {
			protocol.close(false)
			done <- true
		}()

		select {
		case <-time.After(1 * time.Second):
			t.Fatal("protocol.close is waiting")
		case <-done:
			// Ok. Close must not wait.
		}
	})
}
