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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const mapDataOrder1 = "[32 97 112 112 108 105 99 97 116 105 111 110 47 118 110 100 46 103 114 97 112 104 98 105 110 97 114 121 45 118 49 46 48 129 65 210 226 138 32 164 74 176 179 121 216 16 222 222 55 134 0 0 0 4 101 118 97 108 0 0 0 0 0 0 0 2 3 0 0 0 0 7 103 114 101 109 108 105 110 3 0 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41 3 0 0 0 0 7 97 108 105 97 115 101 115 10 0 0 0 0 1 3 0 0 0 0 1 103 3 0 0 0 0 1 103]"
const mapDataOrder2 = "[32 97 112 112 108 105 99 97 116 105 111 110 47 118 110 100 46 103 114 97 112 104 98 105 110 97 114 121 45 118 49 46 48 129 65 210 226 138 32 164 74 176 179 121 216 16 222 222 55 134 0 0 0 4 101 118 97 108 0 0 0 0 0 0 0 2 3 0 0 0 0 7 97 108 105 97 115 101 115 10 0 0 0 0 1 3 0 0 0 0 1 103 3 0 0 0 0 1 103 3 0 0 0 0 7 103 114 101 109 108 105 110 3 0 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"

// capturedResponse is a well formed GraphBinary response frame: status 200, one status attribute, no meta and a
// single int64 result.
var capturedResponse = []byte{129, 0, 251, 37, 42, 74, 117, 221, 71, 191, 183, 78, 86, 53, 0, 12, 132, 100, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 16, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54, 50, 48, 51, 53, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func TestSerializer(t *testing.T) {
	t.Run("test serialized request message", func(t *testing.T) {
		var u, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
		testRequest := request{
			requestID: u,
			op:        "eval",
			processor: "",
			args:      map[string]interface{}{"gremlin": "g.V().count()", "aliases": map[string]interface{}{"g": "g"}},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		serialized, _ := serializer.serializeMessage(&testRequest)
		stringified := fmt.Sprintf("%v", serialized)
		if stringified != mapDataOrder1 && stringified != mapDataOrder2 {
			assert.Fail(t, "Error, expected serialized map data to match one of the provided binary arrays. Can vary based on ordering of keyset, but must map to one of two.")
		}
	})

	t.Run("test serialized response message", func(t *testing.T) {
		responseByteArray := capturedResponse
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.deserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, "fb252a4a-75dd-47bf-b74e-5635000c8464", response.responseID.String())
		assert.Equal(t, uint16(200), response.responseStatus.code)
		assert.Equal(t, "", response.responseStatus.message)
		assert.Equal(t, map[string]interface{}{"host": "/127.0.0.1:62035"}, response.responseStatus.attributes)
		assert.Equal(t, map[string]interface{}{}, response.responseResult.meta)
		assert.Equal(t, []interface{}{int64(0)}, response.responseResult.data)
	})

	t.Run("test serialized response message w/ custom type", func(t *testing.T) {
		RegisterCustomTypeReader("janusgraph.RelationIdentifier", exampleJanusgraphRelationIdentifierReader)
		defer func() {
			UnregisterCustomTypeReader("janusgraph.RelationIdentifier")
		}()
		responseByteArray := []byte{129, 0, 69, 222, 40, 55, 95, 62, 75, 249, 134, 133, 155, 133, 43, 151, 221, 68, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 18, 47, 49, 48, 46, 50, 52, 52, 46, 48, 46, 51, 51, 58, 53, 49, 52, 55, 48, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 33, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 29, 106, 97, 110, 117, 115, 103, 114, 97, 112, 104, 46, 82, 101, 108, 97, 116, 105, 111, 110, 73, 100, 101, 110, 116, 105, 102, 105, 101, 114, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 16, 240, 0, 0, 0, 0, 0, 0, 100, 21, 0, 0, 0, 0, 0, 0, 24, 30, 0, 0, 0, 0, 0, 0, 0, 32, 56}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.deserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, "45de2837-5f3e-4bf9-8685-9b852b97dd44", response.responseID.String())
		assert.Equal(t, uint16(200), response.responseStatus.code)
		assert.Equal(t, "", response.responseStatus.message)
		assert.Equal(t, map[string]interface{}{"host": "/10.244.0.33:51470"}, response.responseStatus.attributes)
		assert.Equal(t, map[string]interface{}{}, response.responseResult.meta)
		assert.NotNil(t, response.responseResult.data)
	})
}

func TestSerializerTruncatedInput(t *testing.T) {
	serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))

	t.Run("test every truncated prefix returns an error", func(t *testing.T) {
		for n := 0; n < len(capturedResponse); n++ {
			prefix := capturedResponse[:n:n]
			assert.NotPanics(t, func() {
				_, err := serializer.deserializeMessage(prefix)
				assert.NotNil(t, err, "prefix of length %d should return an error", n)
			}, "prefix of length %d should not panic", n)
		}
	})

	t.Run("test a plausible text frame returns an error", func(t *testing.T) {
		// gorillaTransporter.Read discards the websocket message type, so a proxy error page reaches the deserializer.
		body := []byte(`{"message":"502 Bad Gateway","code":502,"detail":"upstream connect error or disconnect"}`)
		for n := 0; n <= len(body); n++ {
			prefix := body[:n:n]
			assert.NotPanics(t, func() {
				_, err := serializer.deserializeMessage(prefix)
				assert.NotNil(t, err, "prefix of length %d should return an error", n)
			}, "prefix of length %d should not panic", n)
		}
	})
}

// FuzzDeserializeMessage asserts the containment in deserializeMessage holds for inputs of the size a fuzzer
// generates: a panic must become an error rather than escaping. It cannot cover the deep recursion case, which needs
// megabytes of maximally nested input to reach a stack overflow, and a stack overflow is a fatal runtime error that no
// recover can contain. Nothing bounds nesting depth today.
func FuzzDeserializeMessage(f *testing.F) {
	serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))

	f.Add(capturedResponse)
	f.Add([]byte{})
	f.Add([]byte{129})
	f.Add([]byte(`{"message":"502 Bad Gateway"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = serializer.deserializeMessage(data)
	})
}

func TestSerializerFailures(t *testing.T) {
	t.Run("test convertArgs failure", func(t *testing.T) {
		var u, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
		testRequest := request{
			requestID: u,
			op:        "traversal",
			processor: "",
			// Invalid Input in args, so should fail
			args: map[string]interface{}{"invalidInput": "invalidInput", "aliases": map[string]interface{}{"g": "g"}},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.serializeMessage(&testRequest)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0704ConvertArgsNoSerializerError), err))
	})

	t.Run("test unkownCustomType failure", func(t *testing.T) {
		responseByteArray := []byte{129, 0, 69, 222, 40, 55, 95, 62, 75, 249, 134, 133, 155, 133, 43, 151, 221, 68, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 18, 47, 49, 48, 46, 50, 52, 52, 46, 48, 46, 51, 51, 58, 53, 49, 52, 55, 48, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 33, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 29, 106, 97, 110, 117, 115, 103, 114, 97, 112, 104, 46, 82, 101, 108, 97, 116, 105, 111, 110, 73, 100, 101, 110, 116, 105, 102, 105, 101, 114, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 16, 240, 0, 0, 0, 0, 0, 0, 100, 21, 0, 0, 0, 0, 0, 0, 24, 30, 0, 0, 0, 0, 0, 0, 0, 32, 56}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.deserializeMessage(responseByteArray)
		// a partial message will still be returned
		assert.NotNil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0409GetSerializerToReadUnknownCustomTypeError), err))
	})
}

// exampleJanusgraphRelationIdentifierReader this implementation is not complete and is used only for the purposes of testing custom readers
func exampleJanusgraphRelationIdentifierReader(data *[]byte, i *int) (interface{}, error) {
	const relationIdentifierType = 0x1001
	const longMarker = 0

	// expect type code
	customDataTyp := readUint32Safe(data, i)
	if customDataTyp != relationIdentifierType {
		return nil, fmt.Errorf("unknown type code. got 0x%x, expected 0x%x", customDataTyp, relationIdentifierType)
	}

	// value flag, expect this to be non-nullable
	if readByteSafe(data, i) != valueFlagNone {
		return nil, errors.New("expected non-null value")
	}

	// outVertexId
	if readByteSafe(data, i) == longMarker {
		return readLongSafe(data, i), nil
	} else {
		return readString(data, i)
	}
}
