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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/core/regexp_expressions.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/regexp_bound_expressions.h"
#include "supersonic/expression/core/regexp_bound_expressions_internal.h"
#include "supersonic/expression/core/regexp_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

// The RegExp expressions. They differ from the standard abstract setting in
// having a state - the regexp pattern. This is not an argument (that is - its
// not an expression, as we do not want to compile the pattern each time), but
// really a state.
// TODO(onufry): This could be done through expressions traits, by making them
// stateful and passing an instance of the traits through the building of an
// expression. But that looks like a giant piece of work, and probably isn't
// worth it unless we hit a large number of stateful expressions.
template<OperatorId operation_type>
class RegexpExpression : public UnaryExpression {
 public:
  RegexpExpression(const Expression* const arg,
                   const StringPiece& pattern)
      : UnaryExpression(arg),
        pattern_(pattern.data(), pattern.length()) {}

  virtual string ToString(bool verbose) const {
    return UnaryExpressionTraits<operation_type>::FormatDescription(
        child_expression_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const {
    scoped_ptr<BoundExpression> child_ptr(child);
    DataType child_type = GetExpressionType(child);
    if (child_type != STRING) {
      THROW(new Exception(ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StringPrintf("Invalid argument type (%s) to %s, STRING expected",
                       GetTypeInfo(child_type).name().c_str(),
                       ToString(true).c_str())));
    }
    return BoundGeneralRegexp<operation_type>(
        child_ptr.release(), pattern_, allocator, row_capacity);
  }

  string pattern_;

  DISALLOW_COPY_AND_ASSIGN(RegexpExpression);
};

class RegexpExtractExpression : public UnaryExpression {
 public:
  RegexpExtractExpression(const Expression* const arg,
                          const StringPiece& pattern)
      : UnaryExpression(arg),
        pattern_(pattern.data(), pattern.length()) {}

  virtual string ToString(bool verbose) const {
    return StringPrintf("REGEXP_EXTRACT(%s)",
                        child_expression_->ToString(verbose).c_str());
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const {
    scoped_ptr<BoundExpression> child_ptr(child);
    DataType child_type = GetExpressionType(child);
    if (child_type != STRING) {
      THROW(new Exception(ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StringPrintf("Invalid argument type (%s) to %s, STRING expected",
                       GetTypeInfo(child_type).name().c_str(),
                       ToString(true).c_str())));
    }
    return BoundRegexpExtract(child_ptr.release(), pattern_, allocator,
                              row_capacity);
  }

  string pattern_;

  DISALLOW_COPY_AND_ASSIGN(RegexpExtractExpression);
};

class RegexpReplaceExpression : public BinaryExpression {
 public:
  RegexpReplaceExpression(const Expression* const haystack,
                          const StringPiece& needle,
                          const Expression* const substitute)
      : BinaryExpression(haystack, substitute),
        pattern_(needle.data(), needle.length()) {}

  virtual string ToString(bool verbose) const {
    return BinaryExpressionTraits<OPERATOR_REGEXP_REPLACE>::FormatDescription(
        left_->ToString(verbose), right_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    scoped_ptr<BoundExpression> left_ptr(left);
    scoped_ptr<BoundExpression> right_ptr(right);
    DataType left_type = GetExpressionType(left);
    if (left_type != STRING) {
      THROW(new Exception(ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StringPrintf("Invalid argument type (%s) as first argument to %s, "
                       "STRING expected",
                       GetTypeInfo(left_type).name().c_str(),
                       ToString(true).c_str())));
    }
    DataType right_type = GetExpressionType(right);
    if (right_type != STRING) {
      THROW(new Exception(ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StringPrintf("Invalid argument type (%s) as last argument to %s, "
                       "STRING expected",
                       GetTypeInfo(right_type).name().c_str(),
                       ToString(true).c_str())));
    }
    return BoundRegexpReplace(left_ptr.release(), pattern_, right_ptr.release(),
                              allocator, row_capacity);
  }

  string pattern_;

  DISALLOW_COPY_AND_ASSIGN(RegexpReplaceExpression);
};

}  // namespace

const Expression* RegexpPartialMatch(const Expression* const str,
                                     const StringPiece& pattern) {
  return new RegexpExpression<OPERATOR_REGEXP_PARTIAL>(str, pattern);
}

const Expression* RegexpFullMatch(const Expression* const str,
                                  const StringPiece& pattern) {
  return new RegexpExpression<OPERATOR_REGEXP_FULL>(str, pattern);
}

const Expression* RegexpExtract(const Expression* const str,
                                const StringPiece& pattern) {
  return new RegexpExtractExpression(str, pattern);
}

const Expression* RegexpReplace(const Expression* const haystack,
                                const StringPiece& needle,
                                const Expression* const substitute) {
  return new RegexpReplaceExpression(haystack, needle, substitute);
}

}  // namespace supersonic
