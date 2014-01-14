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

#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"

#include <stddef.h>

#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/elementary_bound_const_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/utils/mathlimits.h"
#include "supersonic/utils/random.h"

// TODO(onufry): this file should probably be merged with
// elementary_bound_const_expressions.h.

namespace supersonic {

class BufferAllocator;

namespace {

class BoundNullExpression : public BasicBoundNoArgumentExpression {
 public:
  explicit BoundNullExpression(const TupleSchema& result_schema,
                               BufferAllocator* const allocator)
      : BasicBoundNoArgumentExpression(result_schema, allocator) {}

  virtual bool is_constant() const { return true; }
  virtual bool can_be_resolved() const { return false; }
  virtual rowcount_t row_capacity() const {
    return MathLimits<rowcount_t>::kMax;
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_pointers) {
    CHECK_EQ(1, skip_pointers.column_count());
    bit_pointer::FillWithTrue(skip_pointers.column(0), input.row_count());
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_pointers.column(0));
    // We could actually not allocate the block for the results, as we do not
    // use it at all, but we do not want somebody to read random memory (and the
    // contract does not guarantee that nobody will read this memory.
    return Success(*my_view());
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(BoundNullExpression);
};

class BoundSequenceExpression : public BasicBoundNoArgumentExpression {
 public:
  explicit BoundSequenceExpression(const TupleSchema& result_schema,
                                   BufferAllocator* const allocator)
      : BasicBoundNoArgumentExpression(result_schema, allocator),
        current_(0) {}

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    int64* data = my_block()->mutable_column(0)->mutable_typed_data<INT64>();
    size_t count = input.row_count();
    while (count-- > 0) *data++ = current_++;
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  bool is_constant() const { return false; }
  bool can_be_resolved() const { return false; }

 private:
  int64 current_;

  DISALLOW_COPY_AND_ASSIGN(BoundSequenceExpression);
};

class BoundRandInt32Expression : public BasicBoundNoArgumentExpression {
 public:
  explicit BoundRandInt32Expression(RandomBase* random_generator,
                                    BufferAllocator* const allocator)
      : BasicBoundNoArgumentExpression(
            CreateSchema("RANDINT32", INT32, NOT_NULLABLE),
            allocator),
        random_generator_(random_generator) {}

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_pointers) {
    CHECK_EQ(1, skip_pointers.column_count());
    int32* data = my_block()->mutable_column(0)->mutable_typed_data<INT32>();
    size_t count = input.row_count();
    // TODO(onufry): Consider skipping with selectivity_level around 10/20.
    while (count-- > 0) *data++ = random_generator_->Rand32();
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_pointers.column(0));
    return Success(*my_view());
  }

  bool is_constant() const { return false; }
  bool can_be_resolved() const { return false; }

 private:
  std::unique_ptr<RandomBase> random_generator_;
  DISALLOW_COPY_AND_ASSIGN(BoundRandInt32Expression);
};


}  // namespace

FailureOrOwned<BoundExpression> BoundNull(DataType type,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return InitBasicExpression(
      max_row_count,
      new BoundNullExpression(TupleSchema::Singleton("NULL", type, NULLABLE),
                              allocator),
      allocator);
}

FailureOrOwned<BoundExpression> BoundSequence(BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  return InitBasicExpression(
      max_row_count,
      new BoundSequenceExpression(
          TupleSchema::Singleton("SEQUENCE", INT64, NOT_NULLABLE), allocator),
      allocator);
}

FailureOrOwned<BoundExpression> BoundConstInt32(const int32& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<INT32>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstInt64(const int64& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<INT64>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstUInt32(const uint32& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<UINT32>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstUInt64(const uint64& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<UINT64>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstFloat(const float& value,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<FLOAT>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstDouble(const double& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<DOUBLE>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstBool(const bool& value,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<BOOL>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstDate(const int32& value,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<DATE>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstDateTime(const int64& value,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<DATETIME>(allocator,
                                                                value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstString(const StringPiece& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<STRING>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstBinary(const StringPiece& value,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<BINARY>(allocator, value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundConstDataType(const DataType& value,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count) {
  return InitBasicExpression(max_row_count,
                             new BoundConstExpression<DATA_TYPE>(allocator,
                                                                 value),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundRandInt32(RandomBase* random_generator,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return InitBasicExpression(
      max_row_count,
      new BoundRandInt32Expression(
          random_generator,
          allocator),
      allocator);
}

}  // namespace supersonic
