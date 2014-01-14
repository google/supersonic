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

#include "supersonic/cursor/infrastructure/row.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

TEST(RowTest, ReturnsCorrectData) {
  std::unique_ptr<Block> block(BlockBuilder<INT32, UINT32, INT64>()
                                   .AddRow(7, 9, __)
                                   .AddRow(8, __, 10LL)
                                   .Build());

  RowSourceAdapter adapter(block->view(), 1);
  EXPECT_TUPLE_SCHEMAS_EQUAL(block->view().schema(), adapter.schema());
  EXPECT_TRUE(adapter.type_info(1).name() == string("UINT32"));

  EXPECT_FALSE(adapter.is_null(0));
  EXPECT_TRUE(adapter.is_null(1));
  EXPECT_FALSE(adapter.is_null(2));

  EXPECT_EQ(8, *(adapter.typed_data<INT32>(0)));
  EXPECT_EQ(10LL, *(adapter.typed_data<INT64>(2)));
}

}  // namespace
}  // namespace supersonic
