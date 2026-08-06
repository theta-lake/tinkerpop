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
	"encoding/base64"
	"net/http"
	"sync"
)

// protocol handles invoking serialization and deserialization, as well as handling the lifecycle of raw data passed to
// and received from the transport layer.
type protocol interface {
	readLoop(resultSets *synchronizedMap, errorCallback func())
	write(request *request) error
	close(wait bool) error
}

const authenticationFailed = uint16(151)

type protocolBase struct {
	protocol

	transporter transporter
}

type gremlinServerWSProtocol struct {
	*protocolBase

	serializer serializer
	logHandler *logHandler
	closed     bool
	mutex      sync.Mutex
	wg         *sync.WaitGroup
}

func (protocol *gremlinServerWSProtocol) readLoop(resultSets *synchronizedMap, errorCallback func()) {
	defer protocol.wg.Done()
	// Closing the transport is this goroutine's own responsibility on every exit path, so errorCallback does not need
	// to reach back for the protocol to do it. That matters because the protocol is assigned to the connection after
	// this goroutine is already running, so a callback firing in between would find it nil and skip the close,
	// stranding the socket and the write loop. transporter.Close is idempotent, so the graceful path is unaffected.
	defer func() {
		_ = protocol.transporter.Close()
	}()

	for {
		// Read from transport layer. If the channel is closed, this will error out and exit.
		msg, err := protocol.transporter.Read()
		protocol.mutex.Lock()
		if protocol.closed {
			protocol.mutex.Unlock()
			return
		}
		protocol.mutex.Unlock()
		if err != nil {
			protocol.logHandler.logf(Error, readLoopError, err.Error())
			protocol.fail(resultSets, errorCallback, err)
			return
		}

		// Deserialize message and unpack.
		resp, err := protocol.serializer.deserializeMessage(msg)
		if err != nil {
			protocol.logHandler.logf(Error, logErrorGeneric, "gremlinServerWSProtocol.readLoop()", err.Error())
			protocol.fail(resultSets, errorCallback, err)
			return
		}

		err = protocol.responseHandler(resultSets, resp)
		if err != nil {
			protocol.fail(resultSets, errorCallback, err)
			return
		}
	}
}

// fail tears the connection down from inside the read loop. The transport is closed before anything else so that a
// caller racing this teardown fails fast in transporter.Write rather than being accepted onto a connection whose read
// loop has exited, and the connection is marked before its result sets are errored so the pool stops handing it out.
func (protocol *gremlinServerWSProtocol) fail(resultSets *synchronizedMap, errorCallback func(), err error) {
	_ = protocol.transporter.Close()
	errorCallback()
	readErrorHandler(resultSets, err, protocol.logHandler)
}

// If there is an error, we need to close the ResultSets and then pass the error back.
func readErrorHandler(resultSets *synchronizedMap, err error, log *logHandler) {
	log.logf(Error, readLoopError, err.Error())
	resultSets.closeAll(err)
}

func (protocol *gremlinServerWSProtocol) responseHandler(resultSets *synchronizedMap, response response) error {
	responseID, statusCode, metadata, data := response.responseID, response.responseStatus.code,
		response.responseResult.meta, response.responseResult.data
	responseIDString := responseID.String()

	rs := resultSets.load(responseIDString)
	if rs == nil {
		// An auth challenge is minted under a fresh request id with no result set of its own, so discarding it would
		// silently strand the request that triggered it. Kept fatal, as it was before.
		if statusCode == http.StatusProxyAuthRequired || statusCode == authenticationFailed {
			return newError(err0501ResponseHandlerResultSetNotCreatedError)
		}
		// Not fatal to the connection. Close removes the result set from this map, and the documented way to abandon a
		// request is to close its ResultSet, so the server will keep sending frames for a request nobody is reading.
		// Tearing the connection down here would punish every other request in flight on it for that. Each websocket
		// message is framed and deserialized independently, so discarding one leaves the stream in step.
		protocol.logHandler.logf(Warning, logErrorGeneric, "gremlinServerWSProtocol.responseHandler()",
			newError(err0501ResponseHandlerResultSetNotCreatedError).Error())
		return nil
	}
	if aggregateTo, ok := metadata["aggregateTo"]; ok {
		// The metadata map holds whatever the server sent, so a non-string value must not be asserted.
		if aggregateToString, isString := aggregateTo.(string); isString {
			rs.setAggregateTo(aggregateToString)
		} else {
			protocol.logHandler.logf(Warning, logErrorGeneric, "gremlinServerWSProtocol.responseHandler()",
				newError(err0410ReadUnexpectedTypeError, "string", aggregateTo).Error())
		}
	}

	// Handle status codes appropriately. If status code is http.StatusPartialContent, we need to re-read data.
	if statusCode == http.StatusNoContent {
		rs.addResult(&Result{make([]interface{}, 0)})
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusOK {
		// Add data and status attributes to the ResultSet.
		rs.addResult(&Result{data})
		rs.setStatusAttributes(response.responseStatus.attributes)
		rs.Close()
		protocol.logHandler.logf(Debug, readComplete, responseIDString)
	} else if statusCode == http.StatusPartialContent {
		// Add data to the ResultSet.
		rs.addResult(&Result{data})
	} else if statusCode == http.StatusProxyAuthRequired || statusCode == authenticationFailed {
		// http status code 151 is not defined here, but corresponds with 403, i.e. authentication has failed.
		// Server has requested basic auth.
		authInfo := protocol.transporter.getAuthInfo()
		if ok, username, password := authInfo.GetBasicAuth(); ok {
			authBytes := make([]byte, 0)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(username)...)
			authBytes = append(authBytes, 0)
			authBytes = append(authBytes, []byte(password)...)
			encoded := base64.StdEncoding.EncodeToString(authBytes)
			request := makeBasicAuthRequest(encoded)
			err := protocol.write(&request)
			if err != nil {
				return err
			}
		} else {
			rs.Close()
			return newError(err0503ResponseHandlerAuthError, response.responseStatus, response.responseResult)
		}
	} else {
		newError := newError(err0502ResponseHandlerReadLoopError, response.responseStatus, statusCode)
		rs.setError(newError)
		rs.Close()
		protocol.logHandler.logf(Error, logErrorGeneric, "gremlinServerWSProtocol.responseHandler()", newError.Error())
	}
	return nil
}

func (protocol *gremlinServerWSProtocol) write(request *request) error {
	bytes, err := protocol.serializer.serializeMessage(request)
	if err != nil {
		return err
	}
	return protocol.transporter.Write(bytes)
}

func (protocol *gremlinServerWSProtocol) close(wait bool) error {
	var err error

	protocol.mutex.Lock()
	if !protocol.closed {
		err = protocol.transporter.Close()
		protocol.closed = true
	}
	protocol.mutex.Unlock()

	if wait {
		protocol.wg.Wait()
	}

	return err
}

func newGremlinServerWSProtocol(handler *logHandler, transporterType TransporterType, url string, connSettings *connectionSettings, results *synchronizedMap,
	errorCallback func()) (protocol, error) {
	wg := &sync.WaitGroup{}
	transport, err := getTransportLayer(transporterType, url, connSettings, handler)
	if err != nil {
		return nil, err
	}

	gremlinProtocol := &gremlinServerWSProtocol{
		protocolBase: &protocolBase{transporter: transport},
		serializer:   newGraphBinarySerializer(handler),
		logHandler:   handler,
		closed:       false,
		mutex:        sync.Mutex{},
		wg:           wg,
	}
	err = gremlinProtocol.transporter.Connect()
	if err != nil {
		return nil, err
	}
	wg.Add(1)
	go gremlinProtocol.readLoop(results, errorCallback)
	return gremlinProtocol, nil
}
