// Copyright 2012 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// This file contains the benchmark manager class which is responsible for
// carrying out the benchmarking process from cursor reception to DOT graph
// drawing.

#ifndef SUPERSONIC_BENCHMARK_MANAGER_BENCHMARK_MANAGER_H_
#define SUPERSONIC_BENCHMARK_MANAGER_BENCHMARK_MANAGER_H_

#include <memory>

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/benchmark/infrastructure/node.h"
#include "supersonic/benchmark/infrastructure/tree_builder.h"
#include "supersonic/benchmark/dot/dot_drawer.h"

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"

namespace supersonic {

class Cursor;

// Enum, whose values describe the possible graph generation destinations.
enum Destination {
  DOT_FILE,
  DOT_STRING
};

// Structure containing options for graph visualisation, currently
// the destination enum and possibly a file name.
struct GraphVisualisationOptions {
  explicit GraphVisualisationOptions(Destination destination)
      : destination(destination) {}

  GraphVisualisationOptions(Destination destination, const string& file_name)
      : destination(destination),
        file_name(file_name) {}

  Destination destination;
  string file_name;
};

// BenchmarkDataWrapper contains the instances of objects necessary to run
// a benchmark on a cursor.
class BenchmarkDataWrapper {
 public:
  // Takes ownership of cursor, tree builder and node.
  BenchmarkDataWrapper(
      Cursor* cursor,
      BenchmarkTreeBuilder* builder,
      BenchmarkTreeNode* node)
      : cursor_(cursor),
        tree_builder_(builder),
        node_(node) {}

  // Caller takes ownership of the result.
  Cursor* release_cursor() { return cursor_.release(); }

  // No ownership transfer.
  BenchmarkTreeNode* node() { return node_.get(); }

 private:
  std::unique_ptr<Cursor> cursor_;
  std::unique_ptr<BenchmarkTreeBuilder> tree_builder_;
  std::unique_ptr<BenchmarkTreeNode> node_;
};

// Transforms the cursor tree, whose root is passed as the argument, by
// attaching spy cursors in between benchmarked nodes. The result is
// a BenchmarkDataWrapper object, which stores the transformed cursor. In
// this use case the caller then takes over the cursor and proceeds
// to carrying out computations on it. The data wrapper must not be destroyed
// before the benchmarking has been completed, that is before the graph has
// been created using the CreateGraph() function, and before the lifetime of
// the cursor ends.
//
// Caller takes ownership of the result data wrapper. The transformed cursor
// resident in the wrapper will take ownership of the argument cursor. The
// caller will have to drain the transformed cursor before proceeding to
// graph creation.
BenchmarkDataWrapper* SetUpBenchmarkForCursor(Cursor* cursor);

// CreateGraph() is used to finalise the benchmarking process by drawing
// a performance graph. It accepts the name of the benchmark, a pointer to
// the benchmark node accessible from BenchmarkDataWrapper and created when
// a call to SetUpBenchmarkForCursor() is made, and an options structure.
// Depending on the options the type of the performed visualisation will vary,
// while options' destination field will determine the semantics of the result
// string:
//
// DOT_FILE - an empty string,
// DOT_STRING - the generated DOT code,
// GRAPHVIZ_RPC - url to the generated DOT graph.
//
// The caller is responsible for draining the benchmarked cursor manually before
// calling CreateGraph().
//
// Does not take ownership of the node.
string CreateGraph(
    const string& benchmark_name,
    BenchmarkTreeNode* node,
    GraphVisualisationOptions options);

// PerformBenchmark() is an all-in-one function which runs a benchmark on
// the given cursor. The benchmark will be labelled with benchmark_name and
// visualised using options. The function will drain the cursor by calling
// its Next() function with the specified block size until the data have been
// depleted. The returned value will depend on the options argument analogously
// to CreateGraph().
//
// Takes ownership of the cursor and destroys it when the benchmark graph
// is ready.
string PerformBenchmark(
    const string& benchmark_name,
    Cursor* cursor,
    rowcount_t max_block_size,
    GraphVisualisationOptions options);

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_MANAGER_BENCHMARK_MANAGER_H_
