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
	"testing"

	"github.com/stretchr/testify/assert"
)

// optionsConfiguration returns the configuration map of the OptionsStrategy carried by bc, or nil if there is none.
func optionsConfiguration(t *testing.T, bc *Bytecode) map[string]interface{} {
	t.Helper()
	for _, insn := range bc.sourceInstructions {
		if insn.operator != "withStrategies" {
			continue
		}
		for _, argument := range insn.arguments {
			if strategy, ok := argument.(*traversalStrategy); ok &&
				strategy.name == decorationNamespace+"OptionsStrategy" {
				return strategy.configuration
			}
		}
	}
	return nil
}

func TestGraphTraversalSource(t *testing.T) {

	t.Run("GraphTraversalSource.With tests", func(t *testing.T) {
		t.Run("Test for single property", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.bytecode.sourceInstructions))
			instruction := traversal.bytecode.sourceInstructions[0]
			assert.Equal(t, "withStrategies", instruction.operator)
			assert.Equal(t, "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy",
				instruction.arguments[0].(*traversalStrategy).name)
			config := instruction.arguments[0].(*traversalStrategy).configuration
			assert.Equal(t, map[string]interface{}{"foo": "bar"}, config)
		})

		t.Run("Test for multiple property", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar").With("foo2", "bar2")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.bytecode.sourceInstructions))
			instruction := traversal.bytecode.sourceInstructions[0]
			assert.Equal(t, "withStrategies", instruction.operator)
			assert.Equal(t, "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy",
				instruction.arguments[0].(*traversalStrategy).name)
			config := instruction.arguments[0].(*traversalStrategy).configuration
			assert.Equal(t, map[string]interface{}{"foo": "bar", "foo2": "bar2"}, config)
		})

		t.Run("Test for property replacement", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil), remoteConnection: nil}
			traversal := g.With("foo", "bar").With("foo", "not bar")
			assert.NotNil(t, traversal)
			assert.Equal(t, 1, len(traversal.bytecode.sourceInstructions))
			instruction := traversal.bytecode.sourceInstructions[0]
			assert.Equal(t, "withStrategies", instruction.operator)
			assert.Equal(t, "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy",
				instruction.arguments[0].(*traversalStrategy).name)
			config := instruction.arguments[0].(*traversalStrategy).configuration
			assert.Equal(t, map[string]interface{}{"foo": "not bar"}, config)
		})

		t.Run("Test derived sources do not observe each other", func(t *testing.T) {
			base := (&GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil)}).With("shared", "yes")

			left := base.With("left", 1)
			right := base.With("right", 2)

			assert.Equal(t, map[string]interface{}{"shared": "yes"}, optionsConfiguration(t, base.bytecode))
			assert.Equal(t, map[string]interface{}{"shared": "yes", "left": 1}, optionsConfiguration(t, left.bytecode))
			assert.Equal(t, map[string]interface{}{"shared": "yes", "right": 2}, optionsConfiguration(t, right.bytecode))
		})

		t.Run("Test source spawned before With does not observe it", func(t *testing.T) {
			base := (&GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil)}).With("shared", "yes")
			traversal := base.V()

			base.With("added", "later")

			assert.Equal(t, map[string]interface{}{"shared": "yes"}, optionsConfiguration(t, traversal.Bytecode))
		})

		t.Run("Test empty withStrategies instruction", func(t *testing.T) {
			g := NewGraphTraversalSource(nil, nil)
			assert.Equal(t, 1, len(g.bytecode.sourceInstructions))
			assert.Equal(t, 0, len(g.bytecode.sourceInstructions[0].arguments))

			source := g.With("evaluationTimeout", 500)
			assert.NotNil(t, source)
			assert.Equal(t, map[string]interface{}{"evaluationTimeout": 500}, optionsConfiguration(t, source.bytecode))
		})

		t.Run("Test non-string key", func(t *testing.T) {
			g := &GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil)}

			named := g.With(T.Id, "a")
			assert.Equal(t, map[string]interface{}{"id": "a"}, optionsConfiguration(t, named.bytecode))

			nonString := g.With(7, "b")
			assert.Equal(t, map[string]interface{}{"7": "b"}, optionsConfiguration(t, nonString.bytecode))
		})

		t.Run("Test concurrent With on a shared source", func(t *testing.T) {
			base := (&GraphTraversalSource{graph: &Graph{}, bytecode: NewBytecode(nil)}).
				With("evaluationTimeout", 1000)

			const goroutines = 8
			var wg sync.WaitGroup
			wg.Add(goroutines)
			for i := 0; i < goroutines; i++ {
				go func(i int) {
					defer wg.Done()
					// Mixes the write path with the read path used at submission time.
					extractReqArgs(base.With("evaluationTimeout", i).bytecode)
				}(i)
			}
			wg.Wait()

			assert.Equal(t, map[string]interface{}{"evaluationTimeout": 1000},
				optionsConfiguration(t, base.bytecode))
		})
	})
}
