// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/cursor/core/scan_view.h"

#include <stddef.h>

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/operation_testing.h"
#include "supersonic/utils/strings/join.h"
#include "gtest/gtest.h"

namespace supersonic {

namespace {

class ScanViewTest : public ::testing::Test {
 protected:
  ScanViewTest()
      : block_(BlockBuilder<INT32, INT64>()
               .AddRow(1, 1LL)  // Reading vertically, you get the birth and
               .AddRow(8, 9LL)  // death year of Mark Twain.
               .AddRow(3, 1LL)
               .AddRow(5, 0LL)
               .Build()) {}

  const View& view() { return block_->view(); }

 private:
  std::unique_ptr<Block> block_;
};

TEST_F(ScanViewTest, ScanViewScansView) {
  OperationTest test;
  test.SetExpectedResult(TestDataBuilder<INT32, INT64>()
                         .AddRow(1, 1LL)
                         .AddRow(8, 9LL)
                         .AddRow(3, 1LL)
                         .AddRow(5, 0LL)
                         .Build());
  test.Execute(ScanView(view()));
}

TEST_F(ScanViewTest, ScanViewScansWithSelection) {
  OperationTest test;
  int64 selection_vector[6] = {0LL, 1LL, 0LL, 3LL, 0LL, 1LL};
  test.SetExpectedResult(TestDataBuilder<INT32, INT64>()
                         .AddRow(1, 1LL)
                         .AddRow(8, 9LL)
                         .AddRow(1, 1LL)
                         .AddRow(5, 0LL)
                         .AddRow(1, 1LL)
                         .AddRow(8, 9LL)
                         .Build());
  for (size_t buffer_size = 1; buffer_size < 8; ++buffer_size) {
    test.Execute(ScanViewWithSelection(view(),
                                       6  /* row count */,
                                       selection_vector,
                                       buffer_size));
  }
}

TEST_F(ScanViewTest, ScanViewOperationFunctionality) {
  std::unique_ptr<Operation> scan(ScanView(view()));

  // Debug string testing.
  string debug = "(prefix)";
  scan->AppendDebugDescription(&debug);
  EXPECT_EQ(
      string("(prefix)ScanView(col0: INT32 NOT NULL, col1: INT64 NOT NULL)"),
      debug);

  // No-op, just to check nothing breaks.
  MemoryLimit limit(1);
  scan->SetBufferAllocator(&limit, true  /* cascade to children, irrelevant */);

  FailureOrOwned<Cursor> cursor(scan->CreateCursor());
  EXPECT_TRUE(cursor.is_success());
}

TEST_F(ScanViewTest, ScanViewWithSelectionOperationFunctionality) {
  int64 selection_vector[6] = {0LL, 1LL, 0LL, 3LL, 0LL, 1LL};
  std::unique_ptr<Operation> scan(ScanViewWithSelection(
      view(), 6 /* row count */, selection_vector, 4 /* buffer size */));
  // Debug string testing.
  string debug = "(prefix)";
  scan->AppendDebugDescription(&debug);
  EXPECT_EQ(
      StrCat("(prefix)ScanViewWithSelection(col0: INT32 NOT NULL, ",
             "col1: INT64 NOT NULL)"),
      debug);

  // Set a large memory limit, should work. Note the memory limit is in bytes -
  // the buffer size needed for 4 rows is 48 bytes (4 times 4 + 8).
  MemoryLimit limit(100);
  scan->SetBufferAllocator(&limit, true  /* cascade to children, irrelevant */);
  FailureOrOwned<Cursor> cursor(scan->CreateCursor());
  EXPECT_TRUE(cursor.is_success());

  MemoryLimit limit2(2);
  scan->SetBufferAllocator(&limit2, true  /* irrelevant again */);
  FailureOrOwned<Cursor> cursor2(scan->CreateCursor());
  // We expect a failure here, because the buffer size is denoted as 4, and we
  // have a memory limit way below it.
  EXPECT_TRUE(cursor2.is_failure());
}

}  // namespace

}  // namespace supersonic
