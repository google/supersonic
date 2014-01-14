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

#include "supersonic/cursor/core/limit.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

namespace {

class LimitCursor : public BasicCursor {
 public:
  // Constructor taking both start_offset and number of rows.
  LimitCursor(rowcount_t start_offset,
              rowcount_t num_rows,
              Cursor* child)
      : BasicCursor(child->schema(), child),
        start_offset_(start_offset),
        num_rows_(num_rows) {}

  virtual ~LimitCursor() {}

  virtual ResultView Next(rowcount_t max_row_count) {
    if (num_rows_ == 0) {
      return ResultView::EOS();
    }
    while (start_offset_ > 0) {
      const rowcount_t skip_count = std::min(max_row_count, start_offset_);
      const ResultView result = child_at(0)->Next(skip_count);
      if (result.has_data()) {
        DCHECK_LE(result.view().row_count(), start_offset_);
        start_offset_ -= result.view().row_count();
      } else {
        return result;
      }
    }
    DCHECK_EQ(start_offset_, 0);
    const rowcount_t row_count = std::min(num_rows_, max_row_count);
    const ResultView result = child_at(0)->Next(row_count);
    if (result.has_data()) {
      DCHECK_LE(result.view().row_count(), num_rows_);
      num_rows_ -= result.view().row_count();
    }
    return result;
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void AppendDebugDescription(string* target) const {
    StrAppend(target,
              "LimitCursor with current limit ",
              start_offset_,
              ", offset ",
              num_rows_);
  }

  virtual CursorId GetCursorId() const { return LIMIT; }

 private:
  rowcount_t start_offset_;
  rowcount_t num_rows_;
  DISALLOW_COPY_AND_ASSIGN(LimitCursor);
};

class LimitOperation : public BasicOperation {
 public:
  // Constructor taking both start_offset and number of rows.
  LimitOperation(rowcount_t start_offset,
                 rowcount_t num_rows,
                 Operation* child)
      : BasicOperation(child),
        start_offset_(start_offset),
        num_rows_(num_rows) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child_cursor = child()->CreateCursor();
    PROPAGATE_ON_FAILURE(child_cursor);
    return Success(
        BoundLimit(start_offset_, num_rows_, child_cursor.release()));
  }

 private:
  rowcount_t start_offset_;
  rowcount_t num_rows_;
  DISALLOW_COPY_AND_ASSIGN(LimitOperation);
};

}  // namespace

Operation* Limit(rowcount_t offset, rowcount_t limit, Operation* child) {
  return new LimitOperation(offset, limit, child);
}

Cursor* BoundLimit(rowcount_t offset, rowcount_t limit, Cursor* child) {
  return new LimitCursor(offset, limit, child);
}

}  // namespace supersonic
