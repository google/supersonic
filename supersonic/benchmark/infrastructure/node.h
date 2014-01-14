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
// This file contains the components which are added together to form
// a benchmarking tree.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_NODE_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_NODE_H_

#include <memory>

#include "supersonic/utils/macros.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

class CursorStatistics;

// Benchmarking node, contains a list of children nodes and a CursorStatistics
// object.
class BenchmarkTreeNode {
 public:
  // Creates a benchmark tree node with no children and a handle to an object
  // containing cursor statistics. Takes ownership of that object.
  explicit BenchmarkTreeNode(CursorStatistics* cursor_statistics)
    : cursor_statistics_(cursor_statistics) {}

  virtual ~BenchmarkTreeNode() {}

  // Calls GatherData() on all CursorStatistics objects in the entire tree
  // recursively.
  virtual void GatherAllData();

  // Node children list getter.
  const util::gtl::PointerVector<BenchmarkTreeNode>& GetChildren() const {
    return children_;
  }

  // Adds child to the list of children nodes. Takes ownership of that child.
  void AddChild(BenchmarkTreeNode* child) {
    children_.push_back(child);
  }

  const CursorStatistics& GetStats() const {
    return *cursor_statistics_;
  }

 private:
  // Node's children in the tree. Ownership taken.
  util::gtl::PointerVector<BenchmarkTreeNode> children_;

  // Statistics for the benchmarked cursor related to the current node.
  std::unique_ptr<CursorStatistics> cursor_statistics_;

  DISALLOW_COPY_AND_ASSIGN(BenchmarkTreeNode);
};

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_NODE_H_

