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
// Tree builder implementation.

#include "supersonic/benchmark/infrastructure/tree_builder.h"

#include <memory>

#include "supersonic/benchmark/base/benchmark_types.h"
#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/benchmark/infrastructure/benchmark_transformer.h"
#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/benchmark/infrastructure/node.h"
#include "supersonic/cursor/base/cursor.h"

#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

namespace {

using util::gtl::PointerVector;

typedef CursorWithBenchmarkListener Entry;
typedef CursorTransformerWithBenchmarkHistory Transformer;

// Function which resolves the cursor's benchmarking type based on its id.
BenchmarkType GetBenchmarkType(const Cursor& cursor) {
  CursorId id = cursor.GetCursorId();
  switch (id) {
    case GENERATE:
    case FILE_INPUT:
    case VIEW:
    case SELECTION_VECTOR_VIEW:
    case REPEATING_BLOCK:
      return LEAF;

    case COMPUTE:
    case MERGE_UNION_ALL:
    case PROJECT:
      return PASS_ALL;

    case AGGREGATE_CLUSTERS:
    case COALESCE:
    case FILTER:
    case FOREIGN_FILTER:
    case LIMIT:
    case ROWID_MERGE_JOIN:
      return PASS_SOME;

    case GROUP_AGGREGATE:
    case SCALAR_AGGREGATE:
    case SORT:
      return PREPROCESS;

    case BEST_EFFORT_GROUP_AGGREGATE:
    case HYBRID_GROUP_FINAL_AGGREGATION:
    case HYBRID_GROUP_TRANSFORM:
      return MAY_PREPROCESS;

    case HASH_JOIN:
      return JOIN;

    case PARALLEL_UNION:
      return PARALLEL;

    case BENCHMARK:
    case OWNERSHIP_TAKER:
    case CANCELLATION_WATCH:
    case DECORATOR:
    case READ_STATISTICS:
      return TRANSPARENT;

    default:
      return UNKNOWN;
  }
}

// Creates a statistics object associated with the output cursor's entry.
// The object will be aware of the benchmark listeners of both input and output
// spy cursors so that accurate benchmarking characteristics could be measured.
//
// The root_stats argument is the statistics object for the root of
// the computation tree. NULL value should be passed, if either relative times
// are not to be calculated (for nodes which have a concurrent ancestor)
// or the object currently created is itself the root.
//
// The function does not take ownership of any of its arguments.
//
// The caller will take ownership of the returned CursorStatistics object.
CursorStatistics* CreateStatsForCursor(
    Entry* node_out_entry,
    const vector<Entry*>& node_in_entries,
    const CursorStatistics* root_stats) {
  string description;
  BenchmarkType type = GetBenchmarkType(*node_out_entry->cursor());
  switch (type) {
    case LEAF:
      return LeafStats(node_out_entry, root_stats);
    case PASS_ALL:
      return PassAllStats(node_in_entries, node_out_entry, root_stats);
    case PASS_SOME:
      return PassSomeStats(node_in_entries, node_out_entry, root_stats);
    case PREPROCESS:
      return PreprocessStats(node_in_entries, node_out_entry, root_stats);
    case MAY_PREPROCESS:
      return MayPreprocessStats(node_in_entries, node_out_entry, root_stats);
    case JOIN:
      return JoinStats(node_in_entries[0],
                       node_in_entries[1],
                       node_out_entry,
                       root_stats);
    case PARALLEL:
      return ParallelStats(node_in_entries, node_out_entry, root_stats);
    case TRANSPARENT:
      return TransparentStats(node_in_entries, node_out_entry);
    case UNKNOWN:
      return UnknownStats(node_in_entries, node_out_entry);
    default:
      LOG(FATAL)
          << "Cursor of benchmarking type: " << type
          << " not fit for benchmarking.";
    }
}

}  // namespace

BenchmarkTreeNode* BenchmarkTreeBuilder::ConstructNodeForCursor(
    Entry* node_out_entry,
    const vector<Entry*>& node_in_entries,
    bool is_root,
    bool is_parallel_descendant) {
  std::unique_ptr<CursorStatistics> stats(CreateStatsForCursor(
      node_out_entry, node_in_entries,
      is_root || is_parallel_descendant ? NULL : root_node_stats_));

  if (is_root) {
    stats->InitRoot();
    root_node_stats_ = stats.get();
  }
  return new BenchmarkTreeNode(stats.release());
}

BenchmarkResult* BenchmarkTreeBuilder::CreateTree(Cursor* cursor) {
  CHECK(!tree_created_) << "BenchmarkTreeBuilder can only be used to build"
      " a tree for one cursor.";

  // Create a transformer which will be used to position spies in
  // the cursor tree.
  std::unique_ptr<Transformer> transformer(BenchmarkSpyTransformer());
  std::unique_ptr<Cursor> wrapped_cursor(transformer->Transform(cursor));

  CHECK_EQ(1, transformer->GetHistoryLength())
      << "There should only be one root node in history.";

  std::unique_ptr<PointerVector<Entry>> history(transformer->ReleaseHistory());
  std::unique_ptr<Entry> root_entry(history->front().release());

  std::unique_ptr<BenchmarkTreeNode> result_node(
      CreateTreeNode(root_entry.get(), transformer.get(),
                     /* root? */ true,
                     /* parallel descendant? */ false));

  // Take ownership of the root entry.
  entries_.push_back(root_entry.release());

  // Set a flag to note that the object has already been used.
  tree_created_ = true;

  return new BenchmarkResult(result_node.release(), wrapped_cursor.release());
}

typedef vector<Entry*>::iterator entry_iterator;

BenchmarkTreeNode* BenchmarkTreeBuilder::CreateTreeNode(
    Entry* current_output,
    Transformer* transformer,
    bool is_root,
    bool is_parallel_descendant) {
  current_output->cursor()->ApplyToChildren(transformer);

  // We now have the children stored in the transformer's history. We take over
  // the history for iteration and launch the cleaned transformer on each child
  // recursively creating tree nodes for them.
  vector<Entry*> history;
  RecoverHistory(transformer, &history);

  BenchmarkTreeNode* node = ConstructNodeForCursor(
      current_output,
      history,
      is_root,
      is_parallel_descendant);

  bool parallel_cursor =
      GetBenchmarkType(*current_output->cursor()) == PARALLEL;
  for (entry_iterator entry = history.begin(); entry != history.end();
       ++entry) {
    node->AddChild(CreateTreeNode(*entry,
                                  transformer,
                                  /* is root? */ false,
                                  is_parallel_descendant || parallel_cursor));
  }
  return node;
}

typedef PointerVector<Entry>::iterator entry_ptr_iterator;

void BenchmarkTreeBuilder::RecoverHistory(Transformer* transformer,
                                          vector<Entry*>* output_history) {
  std::unique_ptr<PointerVector<Entry>> history(transformer->ReleaseHistory());

  // Transfer ownership of all Entry objects to the BenchmarkTreeBuilder class
  // and populate the output_history vector.
  for (entry_ptr_iterator ptr_to_entry = history->begin();
       ptr_to_entry != history->end(); ++ptr_to_entry) {
    entries_.push_back(ptr_to_entry->release());
    output_history->push_back(entries_.back().get());
  }
}

}  // namespace supersonic
