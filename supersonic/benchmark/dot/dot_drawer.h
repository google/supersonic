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
// This file contains the class which is responsible for creating a dot graph
// from a given benchmark tree.

#ifndef SUPERSONIC_BENCHMARK_DOT_DOT_DRAWER_H_
#define SUPERSONIC_BENCHMARK_DOT_DOT_DRAWER_H_

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"

namespace supersonic {

class BenchmarkTreeNode;

// The DOTOutputWriter is an abstract class representing the policy
// of dispatching the diagram to its destination.
class DOTOutputWriter {
 public:
  DOTOutputWriter() {}
  virtual ~DOTOutputWriter() {}

  // Writes the argument dot graph to the output defined by the class's
  // implementation.
  virtual void WriteDOT(const string& dot) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DOTOutputWriter);
};

// DOTDrawer is a class which provides visual descriptions of DOT benchmark
// diagrams. It is initialised by a DOT output writer which provides the
// information about where the graph should be generated. The class objects are
// for single use only.
class DOTDrawer {
 public:
  static const char* kGraphName;
  static const char* kRootNodeName;
  static const char* kGlobalStatsNodeName;

  // Takes ownership of the writer.
  explicit DOTDrawer(DOTOutputWriter* dot_writer, const string& benchmark_name)
      : dot_writer_(dot_writer),
        benchmark_name_(benchmark_name),
        node_counter_(0),
        already_used_(false) {}

  // Draws a diagram from the given benchmark node. Should only be called once
  // on a given object and will fail on any other subsequent call.
  void DrawDOT(const BenchmarkTreeNode& node);

 private:
  // Outputs a line of DOT code to the generated_code_ buffer. The buffer can
  // then be passed on to the output writer.
  void OutputDOTLine(const string& code);

  // Utility function for drawing the node's subtree into the diagram. Takes the
  // parent node's name in order to draw the edge.
  void DrawBenchmarkSubtree(const BenchmarkTreeNode& node,
                            const string& parent_name);

  // Draws the root's output node of the diagram then calls
  // DrawBenchmarkSubtree() to finish drawing the whole benchmark tree.
  void DrawRootBenchmarkSubtree(const BenchmarkTreeNode& node);

  // Draws a node, separate from the cursor tree, which contains global
  // computation statistics.
  void DrawGlobalStats(const BenchmarkTreeNode& node);

  // Appends some introductory DOT instructions to the dot code buffer.
  void OutputDOTBegin();

  // Appends closing instructions for the DOT diagram code to the buffer.
  void OutputDOTEnd();

  // Field representing the policy on sending the generated graph code to its
  // destination.
  std::unique_ptr<DOTOutputWriter> dot_writer_;

  string benchmark_name_;

  // Simple counter used for generating unique node names.
  uint32 node_counter_;

  // Initially empty string used as a buffer for the generated DOT code.
  string generated_graph_code_;

  // Boolean flag to mark whether the drawer has been used already.
  bool already_used_;

  DISALLOW_COPY_AND_ASSIGN(DOTDrawer);
};

// Output writer construction functions. Caller takes ownership of the returned
// writer.

// Creates a DOT writer which outputs the diagram code to the given file.
// Creates the file if it does not exist, and overwrites it if it does.
// Will fail if there are any errors concerning file access.
DOTOutputWriter* CreateFileOutputWriter(const string& file_name);


// Appends the created graph's code to the argument string. Does not take
// ownership of the argument.
DOTOutputWriter* CreateStringOutputWriter(string* dot_output);

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_DOT_DOT_DRAWER_H_
