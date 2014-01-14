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

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_ELEMENTARY_CONST_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_ELEMENTARY_CONST_EXPRESSIONS_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/elementary_bound_const_expressions.h"

namespace supersonic {

template <DataType type,
          bool is_variable_length = TypeTraits<type>::is_variable_length>
class ConstExpression: public Expression {
 public:
  typedef typename TypeTraits<type>::cpp_type cpp_type;
  explicit ConstExpression(const cpp_type& value)
      : value_(value) {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    return InitBasicExpression(
        max_row_count,
        new BoundConstExpression<type>(allocator, value_),
        allocator);
  }

  virtual string ToString(bool verbose) const {
    string result_description;
    if (verbose)
      result_description = StringPrintf("<%s>", TypeTraits<type>::name());
    else
      result_description = "";

    PrintTyped<type>(value_, &result_description);
    return result_description;
  }

 private:
  const cpp_type value_;
};

// Partial specialization for variable-length types (STRING and BINARY)
template <DataType type>
class ConstExpression<type, true>: public Expression {
 public:
  typedef typename TypeTraits<type>::cpp_type cpp_type;  // StringPiece
  explicit ConstExpression(const cpp_type& value)
      : value_(value.data(), value.size()) {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    return InitBasicExpression(
        max_row_count,
        new BoundConstExpression<type>(allocator, StringPiece(value_)),
        allocator);
  }

  virtual string ToString(bool verbose) const {
    string prefix = "";
    if (verbose)
      prefix = type == STRING ? "<STRING>" : "<BINARY>";

    return StringPrintf("%s'%s'", prefix.c_str(), value_.c_str());
  }

 private:
  // Holds a copy of input data.
  const string value_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_ELEMENTARY_CONST_EXPRESSIONS_H_
