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

#include "supersonic/testing/row.h"

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/utils/strings/util.h"

namespace supersonic {

Row::Row(const View& view, size_t row_id)
    : comparable_view_(View(view, row_id, view.row_count() > row_id ? 1 : 0)) {
  CHECK_LE(row_id, view.row_count());  // fails in opt mode, too.
}

testing::AssertionResult Row::operator==(const Row& other) const {
  testing::AssertionResult result =
      (comparable_view_ == other.comparable_view_);
  if (!result) {
    // Remove the "row 0, " part from ComparableView error.
    string comparable_view_error = result.message();
    GlobalReplaceSubstring("row 0, ", "", &comparable_view_error);
    return testing::AssertionFailure() << comparable_view_error;
  } else {
    return result;
  }
}

void Row::AppendToStream(std::ostream* s) const {
  comparable_view_.AppendRowToStream(0, s);
}

testing::AssertionResult RowsEqual(
    const char* a_str, const char* b_str,
    const Row& a, const Row& b) {
  testing::AssertionResult result = (a == b);
  if (!result) {
    result << "\nin RowsEqual(" << a_str << ", " << b_str << ")\n"
           << "\n" << a_str << " = " << a
           << "\n" << b_str << " = " << b;
  }
  return result;
}

}  // namespace supersonic
