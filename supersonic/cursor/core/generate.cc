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
//
// A cursor over the specified number rows with an empty schema.

#include "supersonic/cursor/core/generate.h"

#include <stddef.h>

#include "supersonic/utils/macros.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"

namespace supersonic {

class Operation;

namespace {

class GenerateOperation : public BasicOperation {
 public:
  explicit GenerateOperation(rowcount_t count) : count_(count) {}

  virtual ~GenerateOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    return BoundGenerate(count_);
  }

 private:
  rowcount_t count_;

  DISALLOW_COPY_AND_ASSIGN(GenerateOperation);
};

class GenerateCursor : public BasicCursor {
 public:
  explicit GenerateCursor(rowcount_t count)
      : BasicCursor(TupleSchema()),
        count_(count) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    const size_t count = count_;
    if (count == 0) {
      return ResultView::EOS();
    }
    if (max_row_count > count) {
      max_row_count = count;
    }
    count_ -= max_row_count;
    my_view()->set_row_count(max_row_count);
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual CursorId GetCursorId() const { return GENERATE; }

 private:
  rowcount_t count_;

  DISALLOW_COPY_AND_ASSIGN(GenerateCursor);
};

}  // namespace

Operation* Generate(rowcount_t count) {
  return new GenerateOperation(count);
}

FailureOrOwned<Cursor> BoundGenerate(rowcount_t count) {
  return Success(new GenerateCursor(count));
}

}  // namespace supersonic
