// Copyright 2010 Google Inc.  All Rights Reserved
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

#include "supersonic/base/exception/exception.h"

#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/utils/exception/stack_trace.pb.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include <google/protobuf/text_format.h>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "supersonic/testing/proto_matcher.h"
#include "supersonic/utils/walltime.h"


namespace supersonic {

using ::testing::EqualsProto;

class ExceptionTest : public testing::Test {};

TEST_F(ExceptionTest, BasicFunctionality) {
  int64 time1 = GetCurrentTimeMicros();
  Exception exception(ERROR_ATTRIBUTE_COUNT_MISMATCH, "foo");
  int64 time2 = GetCurrentTimeMicros();
  EXPECT_GE(exception.timestamp(), time1);
  EXPECT_LE(exception.timestamp(), time2);
  EXPECT_EQ("foo", exception.message());
  EXPECT_EQ(ERROR_ATTRIBUTE_COUNT_MISMATCH, exception.return_code());
  EXPECT_EQ(0, exception.stack_trace().element_size());
}

TEST_F(ExceptionTest, SetMessage) {
  Exception exception(ERROR_ATTRIBUTE_COUNT_MISMATCH, "foo");
  exception.set_message("bar");
  EXPECT_EQ("bar", exception.message());
}

TEST_F(ExceptionTest, StackTraces) {
  Exception exception(ERROR_ATTRIBUTE_COUNT_MISMATCH, "foo");
  exception.AddStackTraceElement("Foo", "foo.cc", 10, "foo_context");
  exception.AddStackTraceElement("Bar", "bar.cc", 15, "bar_context");
  ASSERT_EQ(2, exception.stack_trace().element_size());
  EXPECT_EQ("Foo",         exception.stack_trace().element(0).function());
  EXPECT_EQ("foo.cc",      exception.stack_trace().element(0).filename());
  EXPECT_EQ(10,            exception.stack_trace().element(0).line());
  EXPECT_EQ("foo_context", exception.stack_trace().element(0).context());
  EXPECT_EQ("Bar",         exception.stack_trace().element(1).function());
  EXPECT_EQ("bar.cc",      exception.stack_trace().element(1).filename());
  EXPECT_EQ(15,            exception.stack_trace().element(1).line());
  EXPECT_EQ("bar_context", exception.stack_trace().element(1).context());
}

TEST_F(ExceptionTest, PrintStackTrace) {
  Exception exception(ERROR_ATTRIBUTE_COUNT_MISMATCH, "foo");
  exception.AddStackTraceElement("Foo", "foo.cc", 10, "foo_context");
  exception.AddStackTraceElement("Bar", "bar.cc", 15, "bar_context");
  EXPECT_EQ(
      "ERROR_ATTRIBUTE_COUNT_MISMATCH: foo\n"
      "    at Foo(foo.cc:10) foo_context\n"
      "    at Bar(bar.cc:15) bar_context\n",
      exception.PrintStackTrace());
}

TEST_F(ExceptionTest, Serialization) {
  SerializedException serial;
  CHECK(google::protobuf::TextFormat::ParseFromString(
      "timestamp: 100 "
      "message: \"foo\" "
      "return_code: ERROR_ATTRIBUTE_COUNT_MISMATCH "
      "stack_trace: {"
      "  element {"
      "    function: \"Foo\" "
      "    filename: \"foo.cc\" "
      "    line: 10 "
      "    context: \"foo_context\" "
      "  }"
      "  element {"
      "    function: \"Bar\" "
      "    filename: \"bar.cc\" "
      "    line: 15 "
      "    context: \"bar_context\" "
      "  }"
      "}", &serial));
  scoped_ptr<Exception> deserialized(Exception::Deserialize(serial));
  EXPECT_EQ("foo", deserialized->message());
  EXPECT_EQ(2, deserialized->stack_trace().element_size());
  SerializedException reserialized;
  deserialized->Serialize(&reserialized);
  EXPECT_THAT(serial, EqualsProto(reserialized));
}

FailureOrVoid Throw(vector<int>* lines) {
  lines->insert(lines->begin(), __LINE__ + 1);
  THROW(new Exception(ERROR_ATTRIBUTE_EXISTS, "foo"));
}

FailureOrVoid Propagate(vector<int>* lines) {
  lines->insert(lines->begin(), __LINE__ + 1);
  PROPAGATE_ON_FAILURE(Throw(lines));
  return Success();
}

FailureOrVoid PropagateWithContext(vector<int>* lines) {
  lines->insert(lines->begin(), __LINE__ + 1);
  PROPAGATE_ON_FAILURE_WITH_CONTEXT(Propagate(lines), "bar", "outer_context");
  return Success();
}

TEST_F(ExceptionTest, ThrowMacro) {
  vector<int> lines;
  FailureOrVoid result = Throw(&lines);
  ASSERT_TRUE(result.is_failure());
  const Exception& exception = result.exception();
  EXPECT_EQ("foo", exception.message());
  EXPECT_EQ(ERROR_ATTRIBUTE_EXISTS, exception.return_code());
  ASSERT_EQ(1, exception.stack_trace().element_size());
  EXPECT_EQ("Throw", exception.stack_trace().element(0).function());
  EXPECT_EQ(lines[0], exception.stack_trace().element(0).line());
  EXPECT_EQ("(thrown here)", exception.stack_trace().element(0).context());
}

TEST_F(ExceptionTest, MacrosPropagation) {
  // Throw -> Propagate -> PropagateWithContext.
  vector<int> lines;
  FailureOrVoid result = PropagateWithContext(&lines);
  ASSERT_TRUE(result.is_failure());
  const Exception& exception = result.exception();
  EXPECT_EQ("bar: foo", exception.message());
  EXPECT_EQ(ERROR_ATTRIBUTE_EXISTS, exception.return_code());
  // Expecting stack frames from all 3 levels.
  ASSERT_EQ(3, exception.stack_trace().element_size());
  EXPECT_EQ("Throw", exception.stack_trace().element(0).function());
  EXPECT_EQ(lines[0], exception.stack_trace().element(0).line());
  EXPECT_EQ("(thrown here)", exception.stack_trace().element(0).context());
  EXPECT_EQ("Propagate", exception.stack_trace().element(1).function());
  EXPECT_EQ(lines[1], exception.stack_trace().element(1).line());
  EXPECT_EQ("Throw(lines)", exception.stack_trace().element(1).context());
  EXPECT_EQ("PropagateWithContext",
            exception.stack_trace().element(2).function());
  EXPECT_EQ(lines[2], exception.stack_trace().element(2).line());
  EXPECT_EQ("outer_context", exception.stack_trace().element(2).context());
}

}  // namespace supersonic

