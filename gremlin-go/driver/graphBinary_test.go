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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
	"math/big"
	"reflect"
	"testing"
	"time"
)

// appendUnqualifiedString appends {int32 length}{utf8 bytes}.
func appendUnqualifiedString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// appendFullyQualified appends {type code}{value flag} ahead of an already encoded payload.
func appendFullyQualified(b []byte, dataTyp dataType, payload []byte) []byte {
	b = append(b, dataTyp.getCodeByte(), valueFlagNone)
	return append(b, payload...)
}

// buildMetricsPayload assembles the GraphBinary payload of a Metrics:
// {unqualified id}{unqualified name}{int64 duration}{counts map}{annotations map}{nested metrics list}
func buildMetricsPayload(id string, name string, duration int64, counts map[string]int64, nested [][]byte) []byte {
	b := appendUnqualifiedString(nil, id)
	b = appendUnqualifiedString(b, name)
	b = binary.BigEndian.AppendUint64(b, uint64(duration))

	b = binary.BigEndian.AppendUint32(b, uint32(len(counts)))
	for k, v := range counts {
		b = appendFullyQualified(b, stringType, appendUnqualifiedString(nil, k))
		b = appendFullyQualified(b, longType, binary.BigEndian.AppendUint64(nil, uint64(v)))
	}

	// No annotations.
	b = binary.BigEndian.AppendUint32(b, 0)

	b = binary.BigEndian.AppendUint32(b, uint32(len(nested)))
	for _, n := range nested {
		b = appendFullyQualified(b, metricsType, n)
	}
	return b
}

func TestGraphBinaryV1(t *testing.T) {
	t.Run("graphBinaryTypeSerializer tests", func(t *testing.T) {
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}

		t.Run("getType should return correct type for simple type", func(t *testing.T) {
			res, err := serializer.getType(int32(1))
			assert.Nil(t, err)
			assert.Equal(t, intType, res)
		})
		t.Run("getType should return correct type for complex type", func(t *testing.T) {
			res, err := serializer.getType([]byte{1, 2, 3})
			assert.Nil(t, err)
			assert.Equal(t, listType, res)
		})
		t.Run("getType should return error for missing type", func(t *testing.T) {
			_, err := serializer.getType(Error)
			assert.NotNil(t, err)
		})

		t.Run("getWriter should return correct func for dataType", func(t *testing.T) {
			writer, err := serializer.getWriter(intType)
			assert.Nil(t, err)
			assert.Equal(t, reflect.ValueOf(intWriter).Pointer(), reflect.ValueOf(writer).Pointer())
		})
		t.Run("getWriter should return error for missing dataType", func(t *testing.T) {
			_, err := serializer.getWriter(nullType)
			assert.NotNil(t, err)
		})

		t.Run("getSerializerToWrite should return correct func for int", func(t *testing.T) {
			writer, dataType, err := serializer.getSerializerToWrite(int32(1))
			assert.Nil(t, err)
			assert.Equal(t, intType, dataType)
			assert.Equal(t, reflect.ValueOf(intWriter).Pointer(), reflect.ValueOf(writer).Pointer())
		})
		t.Run("getSerializerToWrite should return error for missing dataType", func(t *testing.T) {
			_, _, err := serializer.getSerializerToWrite(nullType)
			assert.NotNil(t, err)
		})
	})

	t.Run("read-write tests", func(t *testing.T) {
		t.Run("read-write string", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			str := "test string"
			buf, err := stringWriter(str, &buffer, nil)
			assert.Nil(t, err)
			res, err := readString(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, str, res)
		})
		t.Run("read-write GremlinType", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := &GremlinType{"test fqcn"}
			buf, err := classWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readClass(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write bool", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			f := func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
				err := binary.Write(buffer, binary.BigEndian, value.(bool))
				return buffer.Bytes(), err
			}
			data, err := f(false, &buffer, nil)
			assert.Nil(t, err)
			res, err := readBoolean(&data, &pos)
			assert.Nil(t, err)
			assert.False(t, res.(bool))

			data, err = f(true, &buffer, nil)
			assert.Nil(t, err)
			res, err = readBoolean(&data, &pos)
			assert.Nil(t, err)
			assert.True(t, res.(bool))
		})
		t.Run("read-write BigDecimal", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := &BigDecimal{11, *big.NewInt(int64(22))}
			buf, err := bigDecimalWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readBigDecimal(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write int", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := int32(123)
			buf, err := intWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readInt(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write short", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := int16(123)
			buf, err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readShort(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write short int8", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := int8(123)
			buf, err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readShort(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, int16(source), res)
		})
		t.Run("read-write long", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := 123
			buf, err := longWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readLong(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, int64(source), res)
		})
		t.Run("read-write bigInt", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := big.NewInt(123)
			buf, err := bigIntWriter(*source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readBigInt(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write bigInt uint64", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := uint64(123)
			buf, err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readBigInt(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(source), res)
		})
		t.Run("read-write bigInt uint64", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := uint(123)
			buf, err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readBigInt(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(uint64(source)), res)
		})
		t.Run("read-write list", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := []interface{}{int32(111), "str"}
			buf, err := listWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readList(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write byteBuffer", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := &ByteBuffer{[]byte{byte(127), byte(255)}}
			buf, err := byteBufferWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readByteBuffer(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write set", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := NewSimpleSet(int32(111), "str")
			buf, err := setWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readSet(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write map", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := map[interface{}]interface{}{1: "s1", "s2": 2, nil: nil}
			buf, err := mapWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := readMap(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, fmt.Sprintf("%v", source), fmt.Sprintf("%v", res))
		})
		t.Run("read incomparable map: a map value as the key", func(t *testing.T) {
			// prepare test data
			var buf = &bytes.Buffer{}
			typeSerializer := &graphBinaryTypeSerializer{}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a map value as the key
			k1 := map[string]string{"key": "value"}
			_, err = typeSerializer.write(reflect.ValueOf(k1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v1 := "value1"
			_, err = typeSerializer.write(reflect.ValueOf(v1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			data := buf.Bytes()
			i := 0
			result, err := readMap(&data, &i)
			if err != nil {
				t.Fatalf("readMap failed: %v", err)
			}
			mResult, ok := result.(map[interface{}]interface{})
			if !ok {
				t.Fatalf("readMap result not map[interface{}]interface{}")
			}
			for k, v := range mResult {
				assert.Equal(t, reflect.Ptr, reflect.TypeOf(k).Kind())
				assert.Equal(t, "value1", v)
			}
		})
		t.Run("read incomparable map: a slice value as the key", func(t *testing.T) {
			// prepare test data
			var buf = &bytes.Buffer{}
			typeSerializer := &graphBinaryTypeSerializer{}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a slice value as the key
			k2 := []int{1, 2, 3}
			_, err = typeSerializer.write(reflect.ValueOf(k2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v2 := "value2"
			_, err = typeSerializer.write(reflect.ValueOf(v2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			data := buf.Bytes()
			i := 0
			result, err := readMap(&data, &i)
			if err != nil {
				t.Fatalf("readMap failed: %v", err)
			}
			expected := map[interface{}]interface{}{
				"[1 2 3]": "value2",
			}
			if !reflect.DeepEqual(result, expected) {
				t.Errorf("Expected %v, but got %v", expected, result)
			}
		})
		t.Run("read-write time", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.Local)
			buf, err := timeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res, err := timeReader(&buf, &pos)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
	})

	t.Run("metrics reader tests", func(t *testing.T) {
		t.Run("read metrics with nested metrics", func(t *testing.T) {
			inner := buildMetricsPayload("3.0.0()", "NestedStep", 200,
				map[string]int64{"traverserCount": 3}, nil)
			outer := buildMetricsPayload("2.0.0()", "OuterStep", 500,
				map[string]int64{"traverserCount": 6}, [][]byte{inner})

			i := 0
			res, err := metricsReader(&outer, &i)
			assert.Nil(t, err)
			metrics, ok := res.(*Metrics)
			assert.True(t, ok)
			assert.Equal(t, "OuterStep", metrics.Name)
			assert.Equal(t, int64(500), metrics.Duration)
			assert.Equal(t, map[string]int64{"traverserCount": 6}, metrics.Counts)
			assert.Equal(t, 1, len(metrics.NestedMetrics))
			assert.Equal(t, "NestedStep", metrics.NestedMetrics[0].Name)
			assert.Equal(t, int64(200), metrics.NestedMetrics[0].Duration)
			assert.Equal(t, map[string]int64{"traverserCount": 3}, metrics.NestedMetrics[0].Counts)
			assert.Equal(t, len(outer), i)
		})

		t.Run("read traversalMetrics with nested metrics", func(t *testing.T) {
			inner := buildMetricsPayload("3.0.0()", "NestedStep", 200, nil, nil)
			step := buildMetricsPayload("2.0.0()", "OuterStep", 500, nil, [][]byte{inner})

			data := binary.BigEndian.AppendUint64(nil, uint64(700))
			data = binary.BigEndian.AppendUint32(data, 1)
			data = appendFullyQualified(data, metricsType, step)

			i := 0
			res, err := traversalMetricsReader(&data, &i)
			assert.Nil(t, err)
			traversalMetrics, ok := res.(*TraversalMetrics)
			assert.True(t, ok)
			assert.Equal(t, int64(700), traversalMetrics.Duration)
			assert.Equal(t, 1, len(traversalMetrics.Metrics))
			assert.Equal(t, 1, len(traversalMetrics.Metrics[0].NestedMetrics))
			assert.Equal(t, "NestedStep", traversalMetrics.Metrics[0].NestedMetrics[0].Name)
			assert.Equal(t, len(data), i)
		})

		t.Run("read metrics with non-string count key returns error", func(t *testing.T) {
			data := appendUnqualifiedString(nil, "2.0.0()")
			data = appendUnqualifiedString(data, "OuterStep")
			data = binary.BigEndian.AppendUint64(data, 500)
			data = binary.BigEndian.AppendUint32(data, 1)
			data = appendFullyQualified(data, intType, binary.BigEndian.AppendUint32(nil, 7))
			data = appendFullyQualified(data, longType, binary.BigEndian.AppendUint64(nil, 6))

			i := 0
			res, err := metricsReader(&data, &i)
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.True(t, isSameErrorCode(newError(err0410ReadUnexpectedTypeError, "", ""), err))
		})

		t.Run("read metrics with non-metrics nested entry returns error", func(t *testing.T) {
			data := appendUnqualifiedString(nil, "2.0.0()")
			data = appendUnqualifiedString(data, "OuterStep")
			data = binary.BigEndian.AppendUint64(data, 500)
			data = binary.BigEndian.AppendUint32(data, 0)
			data = binary.BigEndian.AppendUint32(data, 0)
			data = binary.BigEndian.AppendUint32(data, 1)
			data = appendFullyQualified(data, stringType, appendUnqualifiedString(nil, "not metrics"))

			i := 0
			res, err := metricsReader(&data, &i)
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.True(t, isSameErrorCode(newError(err0410ReadUnexpectedTypeError, "", ""), err))
		})
	})

	t.Run("path reader tests", func(t *testing.T) {
		t.Run("read path with null labels returns error", func(t *testing.T) {
			data := []byte{nullType.getCodeByte(), valueFlagNull}
			i := 0
			res, err := pathReader(&data, &i)
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.True(t, isSameErrorCode(newError(err0410ReadUnexpectedTypeError, "", ""), err))
		})

		t.Run("read path with non-set label entry returns error", func(t *testing.T) {
			labels := binary.BigEndian.AppendUint32(nil, 1)
			labels = appendFullyQualified(labels, stringType, appendUnqualifiedString(nil, "a"))
			data := appendFullyQualified(nil, listType, labels)

			i := 0
			res, err := pathReader(&data, &i)
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.True(t, isSameErrorCode(newError(err0410ReadUnexpectedTypeError, "", ""), err))
		})

		t.Run("read path with null objects returns error", func(t *testing.T) {
			data := appendFullyQualified(nil, listType, binary.BigEndian.AppendUint32(nil, 0))
			data = append(data, nullType.getCodeByte(), valueFlagNull)

			i := 0
			res, err := pathReader(&data, &i)
			assert.Nil(t, res)
			assert.NotNil(t, err)
			assert.True(t, isSameErrorCode(newError(err0410ReadUnexpectedTypeError, "", ""), err))
		})
	})

	t.Run("error handle tests", func(t *testing.T) {
		t.Run("test map key not string failure", func(t *testing.T) {
			i := 0
			buff := []byte{0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01}
			m, err := readMapUnqualified(&buff, &i)
			assert.Nil(t, m)
			assert.Equal(t, newError(err0703ReadMapNonStringKeyError, intType), err)
		})
	})
}
