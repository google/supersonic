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
// The implementations of different DOT drawer classes.

#include "supersonic/benchmark/dot/dot_drawer.h"

#include <memory>

#include "supersonic/benchmark/proto/benchmark.pb.h"
#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/benchmark/infrastructure/node.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/walltime.h"
#include "supersonic/utils/file_util.h"
#include "supersonic/utils/strings/human_readable.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/strings/strcat.h"
#include "supersonic/utils/strings/substitute.h"
#include "supersonic/utils/pointer_vector.h"

namespace supersonic {

namespace {

using strings::Substitute;
using util::gtl::PointerVector;

// Describes the position of the node in the graph, which affects its visual
// representation.
enum NodeType {
  LEAF_NODE, REGULAR_NODE, ROOT_NODE
};

// Matches the node type with an appropriate "shape" parameter in DOT.
string NodeShapeFromType(NodeType type) {
  switch (type) {
    case LEAF_NODE:
      return "folder";
    case REGULAR_NODE:
      return "box";
    case ROOT_NODE:
      return "point";
    default:
      LOG(FATAL) << "Invalid node type: " << type;
  }
}

// Utility drawing functions and shortcuts.

inline string ToCompactString(double value) {
  return HumanReadableNum::DoubleToString(value);
}

inline double ToDouble(int64 value) {
  return static_cast<double>(value);
}

string ReadableRowRate(double rows_per_micro) {
  return StrCat(ToCompactString(rows_per_micro * kNumMicrosPerSecond),
                " rows/s");
}

string ReadableTime(int64 micros) {
  if (micros > kNumMicrosPerSecond) {
    return StrCat(ToCompactString(ToDouble(micros) / kNumMicrosPerSecond),
                  " s");
  }
  if (micros > kNumMicrosPerMilli) {
    return StrCat(ToCompactString(ToDouble(micros) / kNumMicrosPerMilli),
                  " ms");
  }
  // We're printing the integer value without double conversion, as spy cursors
  // do not provide better-than-microsecond resolution.
  return StrCat(micros, " us");
}

// Cannot use HumanReadableNum::DoubleToString here, as it leaves one fraction
// digit for values in [10, 100] and two for [0, 10] which would stop
// the printed percentages from summing up to 100 when they should.
inline string ReadablePercentage(double value) {
  return StringPrintf("%.2lf%%", value);
}

// Returns a DOT code string for a node with a given cursor name and a vector of
// benchmark parameters. Uses cursor name for printable description if the
// description argument is an empty string.
string CodeForNode(
    const string& cursor_name,
    const vector<string>& node_params,
    NodeType type,
    const string& description) {
  string table_delim = "</td></tr><tr><td align=\"right\">";
  string table_start = "<table border=\"0\" "
                       "align=\"right\" "
                       "cellpadding=\"0\">";
  string label =
      StrCat(description.empty() ? cursor_name : description, table_delim,
             strings::Join<vector<string>>(node_params, table_delim));

  return Substitute(
      "$0 [shape=$1, label=<$2<tr><td bgcolor=\"grey\">$3</td></tr></table>>]",
      cursor_name,
      NodeShapeFromType(type),
      table_start,
      label);
}

// Returns a DOT code string representation of the edge between the specified
// nodes. Attaches the parameters to the edge.
inline string CodeForEdge(
    const string& from,
    const string& to,
    const vector<string>& edge_params) {
  string label = strings::Join<vector<string>>(edge_params, "\\n");
  return Substitute("$0->$1 [label=\"$2\"];", from, to, label);
}

// Inserts node parameter descriptions to the node_params vector. Sets the
// output boolean value to true iff the processing time is available
// and positive (it may have been rounded down to 0 us). Will only do the latter
// if valid_processing_time is not null.
// The descriptions are derived from the benchmark data.
void PopulateNodeData(const BenchmarkData& data,
                      vector<string>* node_params,
                      bool* valid_processing_time) {
  if (data.cursor_type() == BenchmarkData::UNRECOGNISED) {
    node_params->push_back("<font color=\"red\">unrecognised</font>");
    return;
  }

  string relative_time;
  if (data.has_relative_time()) {
    StrAppend(&relative_time,
              " (",
              ReadablePercentage(data.relative_time()),
              ")");
  }

  if (data.has_processing_time()) {
    node_params->push_back(StrCat(ReadableTime(data.processing_time()),
                                  relative_time));
  }

  if (valid_processing_time != NULL) {
    *valid_processing_time =
        data.has_processing_time() && data.processing_time() > 0;
  }

  if (data.has_next_calls()) {
    node_params->push_back(StrCat("Next() calls: ",
                                  data.next_calls()));
  }

  if (data.has_row_processing_rate()) {
    node_params->push_back(StrCat(
        "row processing rate: ",
        ReadableRowRate(data.row_processing_rate())));
  }

  if (data.has_preprocessing_time()) {
    node_params->push_back(StrCat("pre-process: ",
                                  ReadableTime(data.preprocessing_time())));
  }

  if (data.has_index_set_up_time()) {
    node_params->push_back(StrCat("index setup: ",
                                  ReadableTime(data.index_set_up_time())));
  }

  if (data.has_matching_time() && data.matching_time() > 0) {
    node_params->push_back(StrCat("matching: ",
                                  ReadableTime(data.matching_time())));
  }

  if (data.has_return_rate()) {
    node_params->push_back(StrCat(
        "return rate: ",
        ReadablePercentage(data.return_rate())));
  }

  if (data.has_speed_up()) {
    node_params->push_back(StrCat(
        "speed-up: ",
        ToCompactString(data.speed_up())));
  }
}

// Inserts edge parameter descriptions to the edge_params vector.
// The descriptions are derived from the benchmark data.
void PopulateEdgeData(const BenchmarkData& data,
                      vector<string>* edge_params,
                      bool throughput_available) {
  if (data.cursor_type() == BenchmarkData::UNRECOGNISED) {
    return;
  }

  if (data.has_rows_processed()) {
    edge_params->push_back(StrCat(data.rows_processed(), " rows total"));
  }

  if (throughput_available && data.has_throughput()) {
    edge_params->push_back(StrCat(
        "(",
        ReadableRowRate(data.throughput()),
        ")"));
  }
}

}  // namespace

const char* DOTDrawer::kGraphName = "SupersonicBenchmarkGraph";
const char* DOTDrawer::kRootNodeName = "Root";
const char* DOTDrawer::kGlobalStatsNodeName = "GlobalStats";

void DOTDrawer::OutputDOTBegin() {
  OutputDOTLine(StrCat("digraph ", kGraphName, " {"));
  OutputDOTLine("center=true;");
  OutputDOTLine("rankdir=BT;");
  OutputDOTLine("node [color=black];");
  OutputDOTLine("edge [minlen=2.5];");
}

void DOTDrawer::OutputDOTEnd() {
  OutputDOTLine("}");
}

void DOTDrawer::OutputDOTLine(const string& code) {
    StrAppend(&generated_graph_code_, code, "\n");
}

void DOTDrawer::DrawDOT(const BenchmarkTreeNode& node) {
  CHECK(!already_used_) << "DOTDrawer object is for single use only.";
  OutputDOTBegin();
  DrawGlobalStats(node);
  DrawRootBenchmarkSubtree(node);
  OutputDOTEnd();
  dot_writer_->WriteDOT(generated_graph_code_);
  already_used_ = true;
}

typedef PointerVector<BenchmarkTreeNode>::const_iterator
    const_node_iterator;

void DOTDrawer::DrawBenchmarkSubtree(
    const BenchmarkTreeNode& node,
    const string& parent_name) {
  const BenchmarkData& node_data = node.GetStats().GetBenchmarkData();
  string node_name = StrCat(node_data.cursor_name(), node_counter_++);

  bool draw_node = node_data.cursor_type() != BenchmarkData::NOT_BENCHMARKED;

  // If node is not benchmarked (transparent) we skip the drawing part and move
  // on to its children.
  if (draw_node) {
    vector<string> params;

    // Will be overwritten by PopulateNodeData.
    bool throughput_available = false;

    PopulateNodeData(node_data, &params, &throughput_available);
    OutputDOTLine(CodeForNode(
        node_name,
        params,
        node.GetChildren().empty() ? LEAF_NODE : REGULAR_NODE,
        ""));

    params.clear();
    PopulateEdgeData(node_data, &params, throughput_available);
    OutputDOTLine(CodeForEdge(
        node_name,
        parent_name,
        params));
  }

  const PointerVector<BenchmarkTreeNode>& children = node.GetChildren();

  for (const_node_iterator child = children.begin();
       child != children.end(); ++child) {
    DrawBenchmarkSubtree(*(child->get()), draw_node ? node_name : parent_name);
  }
}

void DOTDrawer::DrawRootBenchmarkSubtree(const BenchmarkTreeNode& node) {
  vector<string> params;
  OutputDOTLine(CodeForNode(
          kRootNodeName,
          params,
          ROOT_NODE,
          ""));
  DrawBenchmarkSubtree(node, kRootNodeName);
}

void DOTDrawer::DrawGlobalStats(const BenchmarkTreeNode& node) {
  const BenchmarkData& data = node.GetStats().GetBenchmarkData();

  vector<string> params;
  params.push_back(StrCat("Total time: ",
                          ReadableTime(data.total_subtree_time())));

  OutputDOTLine(Substitute("{rank=max; $0}", kGlobalStatsNodeName));
  OutputDOTLine(CodeForNode(kGlobalStatsNodeName,
                            params,
                            REGULAR_NODE,
                            benchmark_name_));
}

namespace {

// DOTOutputWriter implementations.

// --------------------------- StringOutputWriter ------------------------------
class StringOutputWriter : public DOTOutputWriter {
 public:
  // Does not take ownership of the string argument.
  explicit StringOutputWriter(string* dot_output)
      : DOTOutputWriter(),
        dot_output_(dot_output) {}

  virtual void WriteDOT(const string& dot);

 private:
  string* dot_output_;
};

void StringOutputWriter::WriteDOT(const string& dot) {
  StrAppend(dot_output_, dot);
}

// ---------------------------- FileOutputWriter -------------------------------
class FileOutputWriter : public DOTOutputWriter {
 public:
  explicit FileOutputWriter(const string& file_name)
      : DOTOutputWriter(),
        file_name_(file_name) {}

  virtual void WriteDOT(const string& dot);

 private:
  string file_name_;
  std::unique_ptr<FileCloser> out_file_closer_;
};

void FileOutputWriter::WriteDOT(const string& dot) {
  if (file_name_.size() == 0) {
        LOG(FATAL) << "Empty file name provided!";
      }
      out_file_closer_.reset(new FileCloser(File::OpenOrDie(file_name_, "w")));
  CHECK_GT((*out_file_closer_)->Write(dot.c_str(), dot.size()), 0)
      << "Error writing to file: " << file_name_;
  CHECK(out_file_closer_->Close())
      << "Error while closing file: " << file_name_;
  LOG(INFO) << "Results written to: " << file_name_;
}

}  // namespace

DOTOutputWriter* CreateFileOutputWriter(const string& file_name) {
  return new FileOutputWriter(file_name);
}

DOTOutputWriter* CreateStringOutputWriter(string* dot_output) {
  return new StringOutputWriter(dot_output);
}

}  // namespace supersonic
