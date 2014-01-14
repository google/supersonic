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

#include "supersonic/expression/templated/cast_expression.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/templated/cast_bound_expression.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

// A cast expression holder. Does nothing special except holding the data,
// and calling BoundInternalCast at binding time.
class CastExpression : public UnaryExpression {
 public:
  CastExpression(DataType to_type,
                 const Expression* const source,
                 bool is_implicit)
      : UnaryExpression(source),
        to_type_(to_type),
        is_implicit_(is_implicit) {}

  virtual ~CastExpression() {}

  virtual string ToString(bool verbose) const {
    if (is_implicit_) {
      // TODO(onufry): Settle on some distinction here.
      return StringPrintf("%s(%s)",
                          GetTypeInfo(to_type_).name().c_str(),
                          child_expression_->ToString(verbose).c_str());
    } else {
      return StringPrintf("CAST_TO_%s(%s)",
                          GetTypeInfo(to_type_).name().c_str(),
                          child_expression_->ToString(verbose).c_str());
    }
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* bound_child) const {
    return BoundInternalCast(allocator, row_capacity, bound_child,
                             to_type_, is_implicit_);
  }

  DataType to_type_;
  bool is_implicit_;
  DISALLOW_COPY_AND_ASSIGN(CastExpression);
};

}  // namespace

const Expression* InternalCast(DataType to_type,
                               const Expression* const source,
                               bool is_implicit) {
  return new CastExpression(to_type, source, is_implicit);
}

}  // namespace supersonic
