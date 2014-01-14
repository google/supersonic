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

#include "supersonic/expression/core/string_expressions.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/string_bound_expressions.h"
#include "supersonic/expression/core/string_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/utils/strings/strcat.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

// The concatenation expression, extending expression. It doesn't fit into the
// general scheme of abstract_expressions.h as it has an arbitrary number of
// arguments.
class ConcatExpression : public Expression {
 public:
  explicit ConcatExpression(const ExpressionList* const list) : args_(list) {}

 private:
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    FailureOrOwned<BoundExpressionList> args = args_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(args);
    return BoundConcat(args.release(), allocator, max_row_count);
  }

  virtual string ToString(bool verbose) const {
    return StrCat("CONCAT(", args_.get()->ToString(verbose), ")");
  }

  const scoped_ptr<const ExpressionList> args_;
};

}  // namespace

const Expression* Concat(const ExpressionList* const args) {
  return new ConcatExpression(args);
}

const Expression* Length(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundLength,
                                                 "LENGTH($0)");
}

const Expression* Ltrim(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundLtrim, "LTRIM($0)");
}

const Expression* Rtrim(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundRtrim, "RTRIM($0)");
}

const Expression* StringContains(const Expression* const haystack,
                                 const Expression* const needle) {
  return CreateExpressionForExistingBoundFactory(haystack, needle,
      &BoundContains, "CONTAINS($0, $1)");
}

const Expression* StringContainsCI(const Expression* const haystack,
                                   const Expression* const needle) {
  return CreateExpressionForExistingBoundFactory(haystack, needle,
      &BoundContainsCI, "CONTAINS_CI($0, $1)");
}

const Expression* StringOffset(const Expression* const haystack,
                               const Expression* const needle) {
  return CreateExpressionForExistingBoundFactory(
      haystack, needle, &BoundStringOffset, "STRING_OFFSET($0, $1)");
}

const Expression* StringReplace(const Expression* const haystack,
                                const Expression* const needle,
                                const Expression* const substitute) {
  return CreateExpressionForExistingBoundFactory(
      haystack, needle, substitute, &BoundStringReplace,
      "STRING_REPLACE($0, $1, $2)");
}

const Expression* Substring(const Expression* const str,
                            const Expression* const pos,
                            const Expression* const length) {
  return CreateExpressionForExistingBoundFactory(
      str, pos, length, &BoundSubstring, "SUBSTRING($0, $1, $2)");
}

const Expression* ToLower(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundToLower,
                                                 "TO_LOWER($0)");
}

const Expression* ToString(const Expression* const expr) {
  return CreateExpressionForExistingBoundFactory(expr, &BoundToString,
                                                 "TO_STRING($0)");
}

const Expression* ToUpper(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundToUpper,
                                                 "TO_UPPER($0)");
}

const Expression* TrailingSubstring(const Expression* const str,
                                    const Expression* const pos) {
  return CreateExpressionForExistingBoundFactory(
      str, pos, &BoundTrailingSubstring, "SUBSTRING($0, $1)");
}

const Expression* Trim(const Expression* const str) {
  return CreateExpressionForExistingBoundFactory(str, &BoundTrim, "TRIM($0)");
}

}  // namespace supersonic
