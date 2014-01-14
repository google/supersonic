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
// Author:  onufry@google.com (Jakub Onufry Wojtaszczyk)

#include "supersonic/expression/infrastructure/terminal_expressions.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/elementary_const_expressions.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/random.h"

namespace supersonic {

// ----------------------------------------------------------------------------
// Terminal Expressions.

class BufferAllocator;
class TupleSchema;

namespace {

class NullExpression : public Expression {
 public:
  explicit NullExpression(DataType type) : type_(type) {}
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    return BoundNull(type_, allocator, max_row_count);
  }

  string ToString(bool verbose) const {
    if (verbose)
      return StrCat("<", DataType_Name(type_), ">NULL");
    else
      return "NULL";
  }

 private:
  const DataType type_;
  friend class BuildExpressionFromProtoTest;
  DISALLOW_COPY_AND_ASSIGN(NullExpression);
};

class SequenceExpression : public Expression {
 public:
  SequenceExpression() {}
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    return BoundSequence(allocator, max_row_count);
  }

  string ToString(bool verbose) const { return "SEQUENCE()"; }

 private:
  DISALLOW_COPY_AND_ASSIGN(SequenceExpression);
};

class RandInt32Expression : public Expression {
 public:
  explicit RandInt32Expression(RandomBase* random_generator)
    : random_generator_(random_generator) {}
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    RandomBase* generator_clone = random_generator_->Clone();
    CHECK_NOTNULL(generator_clone);
    return BoundRandInt32(generator_clone, allocator, max_row_count);
  }

  string ToString(bool verbose) const { return "RANDINT32()"; }

 private:
  std::unique_ptr<RandomBase> random_generator_;
  DISALLOW_COPY_AND_ASSIGN(RandInt32Expression);
};

}  // namespace

const Expression* Null(DataType type) {
  return new NullExpression(type);
}

const Expression* Sequence() {
  return new SequenceExpression();
}

const Expression* RandInt32(RandomBase* random_generator) {
  return new RandInt32Expression(random_generator);
}

const Expression* RandInt32() {
  return RandInt32(new MTRandom());
}

const Expression* ConstInt32(const int32& value) {
  return new ConstExpression<INT32>(value);
}

const Expression* ConstInt64(const int64& value) {
  return new ConstExpression<INT64>(value);
}

const Expression* ConstUint32(const uint32& value) {
  return new ConstExpression<UINT32>(value);
}

const Expression* ConstUint64(const uint64& value) {
  return new ConstExpression<UINT64>(value);
}

const Expression* ConstFloat(const float& value) {
  return new ConstExpression<FLOAT>(value);
}

const Expression* ConstDouble(const double& value) {
  return new ConstExpression<DOUBLE>(value);
}

const Expression* ConstBool(const bool& value) {
  return new ConstExpression<BOOL>(value);
}

const Expression* ConstDate(const int32& value) {
  return new ConstExpression<DATE>(value);
}

const Expression* ConstDateTime(const int64& value) {
  return new ConstExpression<DATETIME>(value);
}

const Expression* ConstString(const StringPiece& value) {
  return new ConstExpression<STRING>(value);
}

const Expression* ConstBinary(const StringPiece& value) {
  return new ConstExpression<BINARY>(value);
}

const Expression* ConstDataType(const DataType& value) {
  return new ConstExpression<DATA_TYPE>(value);
}

}  // namespace supersonic
