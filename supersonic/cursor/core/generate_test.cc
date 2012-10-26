// Copyright 2010 Google Inc. All Rights Reserved.
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

#include "supersonic/cursor/core/generate.h"

#include "supersonic/utils/macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/base/cursor.h"
#include "gtest/gtest.h"

namespace supersonic {

class BoundGenerateCursorTest : public testing::Test {};

// Tests if BoundGenerateCursor(0) returns empty cursor.
TEST_F(BoundGenerateCursorTest, Zero) {
  FailureOrOwned<Cursor> cursor = BoundGenerate(0);
  ASSERT_TRUE(cursor.is_success());

  const TupleSchema& schema = cursor->schema();

  EXPECT_EQ(0, schema.attribute_count());

  ResultView result = cursor->Next(1);
  EXPECT_TRUE(result.is_eos());
  result = cursor->Next(1);
  EXPECT_TRUE(result.is_eos());
}

// Tests if BoundGenerateCursor(X) returns a cursor over X rows.
TEST_F(BoundGenerateCursorTest, Many) {
  static const int kRowCounts[] = { 1, 100, 1200, 65536 };

  for (int i = 0; i < arraysize(kRowCounts); i++) {
    FailureOrOwned<Cursor> cursor = BoundGenerate(kRowCounts[i]);
    ASSERT_TRUE(cursor.is_success());

    int row_count = 0;
    while (true) {
      ResultView result = cursor->Next(Cursor::kDefaultRowCount);
      if (result.is_eos()) {
        break;
      }
      ASSERT_TRUE(result.has_data());

      const int result_row_count = result.view().row_count();
      EXPECT_LT(0, result_row_count);
      EXPECT_GE(kRowCounts[i], result_row_count);
      row_count += result_row_count;
    }

    EXPECT_EQ(kRowCounts[i], row_count);
  }
}

}  // namespace supersonic
