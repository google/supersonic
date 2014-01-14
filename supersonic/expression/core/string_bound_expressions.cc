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

#include "supersonic/expression/core/string_bound_expressions.h"

#include <stddef.h>
#include <string.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/comparison_bound_expressions.h"
#include "supersonic/expression/core/string_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

class BufferAllocator;

namespace {

class BoundConcatExpression : public BasicBoundExpression {
 public:
  BoundConcatExpression(const string& name,
                        Nullability nullability,
                        BufferAllocator* allocator,
                        BoundExpressionList* arguments)
      : BasicBoundExpression(CreateSchema(name, STRING, nullability),
                             allocator),
        arguments_(arguments) {}

  rowcount_t row_capacity() const {
    rowcount_t capacity = my_const_block()->row_capacity();
    for (int i = 0; i < arguments_->size(); ++i) {
      capacity = std::min(capacity, arguments_->get(i)->row_capacity());
    }
    return capacity;
  }

  bool can_be_resolved() const {
    for (int i = 0; i < arguments_->size(); ++i) {
      if (!arguments_->get(i)->is_constant()) return false;
    }
    return true;
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    my_block()->ResetArenas();
    bool_ptr skip_vector = skip_vectors.column(0);
    vector<EvaluationResult> results;
    for (int n = 0; n < arguments_->size(); ++n) {
      EvaluationResult result
          = arguments_->get(n)->DoEvaluate(input, skip_vectors);
      PROPAGATE_ON_FAILURE(result);
      results.push_back(result);
    }

    StringPiece* destination =
        my_block()->mutable_column(0)->mutable_typed_data<STRING>();

    Arena* arena = my_block()->mutable_column(0)->arena();
    vector<const StringPiece*> sources;
    for (int n = 0; n < arguments_->size(); ++n) {
      PROPAGATE_ON_FAILURE(results[n]);
      sources.push_back(results[n].get().column(0).typed_data<STRING>());
    }

    // We set the selectivity threshold = 100 - this is an unsafe operation.
    if (!SelectivityIsGreaterThan(skip_vector, input.row_count(), 100)) {
      for (int i = 0; i < input.row_count(); ++i) {
        size_t length = 0;
        for (int n = 0; n < arguments_->size(); ++n)
          length += sources[n]->size();
        char* new_str = static_cast<char *>(arena->AllocateBytes(length));
        char* current_position = new_str;
        for (int n = 0; n < arguments_->size(); ++n) {
          strncpy(current_position, sources[n]->data(), sources[n]->size());
          current_position += sources[n]->size();
          ++sources[n];
        }
        *destination++ = StringPiece(new_str, length);
      }
    } else {
      for (int i = 0; i < input.row_count(); ++i) {
        if (*skip_vector == false) {
          size_t length = 0;
          for (int n = 0; n < arguments_->size(); ++n)
            length += sources[n]->size();
          char* new_str = static_cast<char *>(arena->AllocateBytes(length));
          char* current_position = new_str;
          for (int n = 0; n < arguments_->size(); ++n) {
            strncpy(current_position, sources[n]->data(), sources[n]->size());
            current_position += sources[n]->size();
          }
          *destination = StringPiece(new_str, length);
        }
        for (int n = 0; n < arguments_->size(); ++n) ++sources[n];
        ++skip_vector;
        ++destination;
      }
    }
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    arguments_->CollectReferredAttributeNames(referred_attribute_names);
  }

 private:
  const scoped_ptr<BoundExpressionList> arguments_;

  DISALLOW_COPY_AND_ASSIGN(BoundConcatExpression);
};

}  // namespace

// --------- Implementation of functions from public header --------------------

FailureOrOwned<BoundExpression> BoundToString(BoundExpression* arg,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  if (GetExpressionType(arg) == STRING) return Success(arg);
  return CreateUnaryArbitraryInputExpression<OPERATOR_TOSTRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundStringOffset(BoundExpression* haystack,
                                                  BoundExpression* needle,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_STRING_OFFSET, STRING,
      STRING, INT32>(allocator, max_row_count, haystack, needle);
}

FailureOrOwned<BoundExpression> BoundContains(BoundExpression* haystack,
                                              BoundExpression* needle,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  FailureOrOwned<BoundExpression> bound_offset(
      BoundStringOffset(haystack, needle, allocator, max_row_count));
  PROPAGATE_ON_FAILURE(bound_offset);
  return BoundGreater(bound_offset.release(),
      SucceedOrDie(BoundConstUInt32(0, allocator, max_row_count)),
      allocator, max_row_count);
}

// TODO(ptab): Could be implemented without conversion to_lower of both
// parameters.
FailureOrOwned<BoundExpression> BoundContainsCI(BoundExpression* haystack,
                                                BoundExpression* needle,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  FailureOrOwned<BoundExpression> lowered_haystack(
      BoundToLower(haystack, allocator, max_row_count));
  FailureOrOwned<BoundExpression> lowered_needle(
      BoundToLower(needle, allocator, max_row_count));
  PROPAGATE_ON_FAILURE(lowered_haystack);
  PROPAGATE_ON_FAILURE(lowered_needle);
  return BoundContains(lowered_haystack.release(), lowered_needle.release(),
                       allocator, max_row_count);
}

FailureOrOwned<BoundExpression> BoundConcat(BoundExpressionList* args,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  scoped_ptr<BoundExpressionList> arglist(args);
  // We will need a place to hold expressions after converting to strings.
  scoped_ptr<BoundExpressionList> stringified_args(new BoundExpressionList());
  for (int i = 0; i < arglist.get()->size(); ++i) {
  string name = StringPrintf("The %dth element on the concat list", i);
    BoundExpression* child = arglist.get()->get(i);

    PROPAGATE_ON_FAILURE(
        CheckAttributeCount(name, child->result_schema(), 1));
    FailureOrOwned<BoundExpression> stringed = BoundToString(
        arglist.get()->release(i), allocator, max_row_count);
    PROPAGATE_ON_FAILURE(stringed);
    stringified_args->add(stringed.release());
  }
  string name = "CONCAT(";
  Nullability nullability = NOT_NULLABLE;
  for (int i = 0; i < stringified_args->size(); ++i) {
    if (i > 0) name += ", ";
    name += stringified_args->get(i)->result_schema().attribute(0).name();
    if (stringified_args->get(i)->result_schema().attribute(0).is_nullable()) {
      nullability = NULLABLE;
    }
  }
  name += ")";
  return InitBasicExpression(
      max_row_count,
      new BoundConcatExpression(name, nullability, allocator,
                                stringified_args.release()),
      allocator);
}

FailureOrOwned<BoundExpression> BoundLength(BoundExpression* arg,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LENGTH, STRING, UINT32>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundLtrim(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_LTRIM, STRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundRtrim(BoundExpression* arg,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_RTRIM, STRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundTrim(BoundExpression* arg,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_TRIM, STRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundToUpper(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_TOUPPER, STRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundToLower(BoundExpression* arg,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  return CreateTypedBoundUnaryExpression<OPERATOR_TOLOWER, STRING, STRING>(
      allocator, max_row_count, arg);
}

FailureOrOwned<BoundExpression> BoundTrailingSubstring(
    BoundExpression* str,
    BoundExpression* pos,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_SUBSTRING_SIGNALING,
      STRING, INT64, STRING>(allocator, max_row_count, str, pos);
}

FailureOrOwned<BoundExpression> BoundStringReplace(
    BoundExpression* haystack,
    BoundExpression* needle,
    BoundExpression* substitute,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundTernaryExpression<OPERATOR_STRING_REPLACE,
      STRING, STRING, STRING, STRING>(allocator, max_row_count, haystack,
                                      needle, substitute);
}

FailureOrOwned<BoundExpression> BoundSubstring(
    BoundExpression* str,
    BoundExpression* pos,
    BoundExpression* length,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundTernaryExpression<OPERATOR_SUBSTRING_SIGNALING,
      STRING, INT64, INT64, STRING>(allocator, max_row_count, str, pos, length);
}

}  // namespace supersonic
