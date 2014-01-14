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
// This file introduces the benchmark tree builder responsible for creating
// a benchmark tree out of a standard cursor.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_TREE_BUILDER_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_TREE_BUILDER_H_

#include <memory>

#include "supersonic/benchmark/infrastructure/benchmark_transformer.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

class BenchmarkTreeNode;
class Cursor;
class CursorStatistics;

// A simple uncopiable wrapper class which contains the resulting benchmark tree
// node and a wrapped cursor to be used for computation.
class BenchmarkResult {
 public:
  BenchmarkResult() {}

  // Takes ownership of both the node and the cursor.
  BenchmarkResult(BenchmarkTreeNode* node, Cursor* cursor)
      : node_(node),
        cursor_(cursor) {}

  // Simple field getters, ownership is not transferred.
  const BenchmarkTreeNode& node() { return *node_; }
  const Cursor& cursor() { return *cursor_; }

  // Field getters which transfer ownership of the retrieved object
  // to the caller.
  BenchmarkTreeNode* release_node() { return node_.release(); }
  Cursor* release_cursor() { return cursor_.release(); }

 private:
  std::unique_ptr<BenchmarkTreeNode> node_;
  std::unique_ptr<Cursor> cursor_;

  DISALLOW_COPY_AND_ASSIGN(BenchmarkResult);
};

// The tree builder is a utility that can create a benchmarking tree from
// a cursor. The tree can then be used to investigate data processing times,
// once the cursors have been launched.
//
// One such object can be used to create trees for one cursor only.
//
// The tree builder will not own any cursors created in the process, but it will
// take ownership of the benchmark listeners used for spying, therefore it
// should outlive the cursor it has been used to build a tree for.
class BenchmarkTreeBuilder {
 public:
  BenchmarkTreeBuilder()
      : root_node_stats_(NULL),
        tree_created_(false) {}

  virtual ~BenchmarkTreeBuilder() {}

  // Creates a benchmarking tree for the argument cursor. The caller will take
  // ownership of the returned object which contains a handle to the tree root
  // and a new wrapped-up cursor - the BenchmarkResult object will be
  // responsible for destroying both these objects.
  //
  // The method should only be called for one cursor and will fail otherwise.
  //
  // Ownership of the argument cursor will be taken by the created cursor
  // wrapper.
  virtual BenchmarkResult* CreateTree(Cursor* cursor);

 private:
  // The method will create a tree node for a CursorWithBenchmarkListener object
  // describing the output cursor and launch itself recursively to create the
  // node's children.
  //
  // node_out_entry - wrapper containing the cursor which we are creating
  // the node for.
  //
  // transformer - cursor transformer with history to be used to wrap up
  // cursors with spies.
  //
  // is_root - whether the node's cursor is the computation root.
  //
  // is_parallel_descendant - whether there is a parallel (concurrent)
  // computation cursor on the path from the node's cursor to the root.
  //
  // The function does not take ownership of the arguments.
  BenchmarkTreeNode* CreateTreeNode(
      CursorWithBenchmarkListener* node_out_entry,
      CursorTransformerWithBenchmarkHistory* transformer,
      bool is_root,
      bool is_parallel_descendant);

  // Utility method used to create a node containing proper cursor statistics
  // for the given cursor. It takes both the input and output cursor entries
  // as arguments. The is_root and is_parallel_descendant arguments convey
  // the same information as the ones for CreateTreeNode().
  //
  // No ownership transfer takes place.
  BenchmarkTreeNode* ConstructNodeForCursor(
      CursorWithBenchmarkListener* node_out_entry,
      const vector<CursorWithBenchmarkListener*>& node_in_entries,
      bool is_root,
      bool is_parallel_descendant);

  // The function takes the history out of the transformer relinquishing its
  // ownership. The tree builder then takes ownership of these entries itself
  // and inserts them into the output_history vector, so that it can be used
  // for the construction of cursor statistics objects.
  void RecoverHistory(CursorTransformerWithBenchmarkHistory* transformer,
                      vector<CursorWithBenchmarkListener*>* output_history);

  // Field storing a handle to the statistics object describing the computation
  // root. Ownership is not taken, as the object is owned by the node
  // corresponding to the cursor it describes. This is used only to have access
  // to the statistics of the root so that they can be used during the recursive
  // node construction.
  CursorStatistics* root_node_stats_;

  // Pointer vector for managing the lifetime of history entries.
  util::gtl::PointerVector<CursorWithBenchmarkListener> entries_;

  // Boolean flag which is set to true when a tree is built for a cursor, used
  // to ensure that the builder is used only for one cursor.
  bool tree_created_;

  DISALLOW_COPY_AND_ASSIGN(BenchmarkTreeBuilder);
};

}  // namespace supersonic


#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_TREE_BUILDER_H_
