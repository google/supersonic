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
#include "supersonic/expression/core/string_bound_expressions_internal.h"

#include <stddef.h>
#include <string.h>
#include <set>
using std::multiset;
using std::set;
#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <string>
using std::string;
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
#include "supersonic/expression/vector/binary_column_computers.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"
#include <re2/re2.h>

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

template<OperatorId op>
class BoundRegexpExpression : public BoundUnaryExpression {
 public:
  // Takes over the ownership of the pattern.
  BoundRegexpExpression(const string& output_name,
                        BufferAllocator* const allocator,
                        BoundExpression* arg,
                        const RE2* pattern)
      : BoundUnaryExpression(
            CreateSchema(output_name, BOOL, arg,
                         UnaryExpressionTraits<op>::can_return_null
                             ? NULLABLE
                             : NOT_NULLABLE),
            allocator, arg, STRING),
        pattern_(pattern) {}

 private:
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    my_block()->ResetArenas();
    bool_ptr skip_vector = skip_vectors.column(0);
    EvaluationResult result = argument()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(result);

    bool* destination =
        my_block()->mutable_column(0)->template mutable_typed_data<BOOL>();

    const StringPiece* source = result.get().column(0).typed_data<STRING>();
    typename UnaryExpressionTraits<op>::basic_operator operation;

    bool selective_evaluate = SelectivityIsGreaterThan(
        skip_vector, input.row_count(),
        UnaryExpressionTraits<op>::selectivity_threshold);
    if (selective_evaluate) {
      for (int i = 0; i < input.row_count(); ++i) {
        if (!*skip_vector) {
          destination[i] = operation(*pattern_, source[i]);
        }
        ++skip_vector;
      }
    } else {
      for (int i = 0; i < input.row_count(); ++i) {
        destination[i] = operation(*pattern_, source[i]);
      }
    }
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  scoped_ptr<const RE2> pattern_;

  DISALLOW_COPY_AND_ASSIGN(BoundRegexpExpression);
};

class BoundRegexpExtractExpression : public BoundUnaryExpression {
 public:
  // Takes over the ownership of the pattern.
  BoundRegexpExtractExpression(const string& output_name,
                               BufferAllocator* const allocator,
                               BoundExpression* arg,
                               const RE2* pattern)
      : BoundUnaryExpression(CreateSchema(output_name, STRING, NULLABLE),
                             allocator, arg, STRING),
        pattern_(pattern) {}

 private:
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    my_block()->ResetArenas();
    CHECK_EQ(1, skip_vectors.column_count());
    bool_ptr skip_vector = skip_vectors.column(0);
    EvaluationResult result = argument()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(result);

    StringPiece* destination =
        my_block()->mutable_column(0)->mutable_typed_data<STRING>();

    const StringPiece* source = result.get().column(0).typed_data<STRING>();

    for (int i = 0; i < input.row_count(); ++i) {
      if (!*skip_vector) {
        re2::StringPiece re2_source(source[i].data(), source[i].length());
        re2::StringPiece re2_destination;
        *skip_vector |=
            !RE2::PartialMatch(re2_source,
                               *pattern_,
                               &re2_destination);
        destination[i].set(re2_destination.data(), re2_destination.length());
      }
      ++skip_vector;
    }

    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  scoped_ptr<const RE2> pattern_;

  DISALLOW_COPY_AND_ASSIGN(BoundRegexpExtractExpression);
};

class BoundRegexpReplaceExpression : public BoundBinaryExpression {
 public:
  // Takes over the ownership of the pattern.
  BoundRegexpReplaceExpression(const string& output_name,
                               BufferAllocator* const allocator,
                               BoundExpression* left,
                               BoundExpression* right,
                               const RE2* pattern)
      : BoundBinaryExpression(CreateSchema(output_name, STRING, left, right),
                              allocator, left, STRING, right, STRING),
        pattern_(pattern) {}

 private:
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    my_block()->ResetArenas();
    CHECK_EQ(1, skip_vectors.column_count());
    bool_ptr skip_vector = skip_vectors.column(0);
    EvaluationResult left_result = left()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(left_result);
    EvaluationResult right_result = right()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(right_result);

    StringPiece* destination =
        my_block()->mutable_column(0)->mutable_typed_data<STRING>();

    binary_column_computers::CheckAndNull<OPERATOR_REGEXP_REPLACE, STRING,
        STRING, STRING> nuller;
    FailureOrVoid nulling_result = nuller(left_result.get().column(0),
                                          right_result.get().column(0),
                                          skip_vector,
                                          input.row_count());
    PROPAGATE_ON_FAILURE(nulling_result);

    const StringPiece* haystack =
        left_result.get().column(0).typed_data<STRING>();
    const StringPiece* substitute =
        right_result.get().column(0).typed_data<STRING>();
    string temp;
    operators::RegexpReplace replace_operator;

    if (!SelectivityIsGreaterThan(
        skip_vector, input.row_count(),
        BinaryExpressionTraits<
            OPERATOR_REGEXP_REPLACE>::selectivity_threshold)) {
      for (int i = 0; i < input.row_count(); ++i) {
        destination[i] = replace_operator(
            haystack[i], *pattern_, substitute[i], temp,
            my_block()->mutable_column(0)->arena());
      }
    } else {
      for (int i = 0; i < input.row_count(); ++i) {
        if (!*skip_vector) {
          destination[i] = replace_operator(
              haystack[i], *pattern_, substitute[i], temp,
              my_block()->mutable_column(0)->arena());
        }
        ++skip_vector;
      }
    }
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  scoped_ptr<const RE2> pattern_;

  DISALLOW_COPY_AND_ASSIGN(BoundRegexpReplaceExpression);
};

}  // namespace

// ------------------------ Internal -------------------------------------------

template<OperatorId operation_type>
FailureOrOwned<BoundExpression> BoundGeneralRegexp(BoundExpression* child_ptr,
                                                   const StringPiece& pattern,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> child(child_ptr);
  string name = UnaryExpressionTraits<operation_type>::FormatDescription(
      child->result_schema().attribute(0).name());

  DataType input_type = GetExpressionType(child.get());
  if (input_type != STRING) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Invalid input type (", GetTypeInfo(input_type).name(),
               "), STRING expected in ", name)));
  }
  scoped_ptr<const RE2> pattern_(new RE2(pattern.ToString()));
  if (!pattern_->ok()) {
    string message = StrCat("Malformed regexp: ", pattern, ", parse error: ",
                            pattern_->error());
    THROW(new Exception(ERROR_INVALID_ARGUMENT_VALUE, message));
  }
  return InitBasicExpression(
      max_row_count,
      new BoundRegexpExpression<operation_type>(name, allocator,
                                                child.release(),
                                                pattern_.release()),
      allocator);
}

template FailureOrOwned<BoundExpression>
BoundGeneralRegexp<OPERATOR_REGEXP_PARTIAL>(BoundExpression* child_ptr,
                                            const StringPiece& pattern,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

template FailureOrOwned<BoundExpression>
BoundGeneralRegexp<OPERATOR_REGEXP_FULL>(BoundExpression* child_ptr,
                                         const StringPiece& pattern,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

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

FailureOrOwned<BoundExpression> BoundRegexpPartialMatch(
    BoundExpression* str,
    const StringPiece& pattern,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundGeneralRegexp<OPERATOR_REGEXP_PARTIAL>(str, pattern, allocator,
                                                     max_row_count);
}

FailureOrOwned<BoundExpression> BoundRegexpFullMatch(
    BoundExpression* str,
    const StringPiece& pattern,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundGeneralRegexp<OPERATOR_REGEXP_FULL>(str, pattern, allocator,
                                                  max_row_count);
}

FailureOrOwned<BoundExpression> BoundRegexpExtract(
    BoundExpression* child_ptr,
    const StringPiece& pattern,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> child(child_ptr);
  string name =
      UnaryExpressionTraits<OPERATOR_REGEXP_EXTRACT>::FormatDescription(
          GetExpressionName(child.get()));

  FailureOrVoid input_check = CheckExpressionType(STRING, child.get());
  PROPAGATE_ON_FAILURE(input_check);
  scoped_ptr<const RE2> pattern_(new RE2(pattern.ToString()));
  if (!pattern_->ok()) {
    string message = StrCat("Malformed regexp: ", pattern, ", parse error: ",
                            pattern_->error(), " in ", name);
    THROW(new Exception(ERROR_INVALID_ARGUMENT_VALUE, message));
  }
  return InitBasicExpression(
      max_row_count,
      new BoundRegexpExtractExpression(name, allocator, child.release(),
                                       pattern_.release()),
      allocator);
}

FailureOrOwned<BoundExpression> BoundRegexpReplace(
    BoundExpression* haystack_ptr,
    const StringPiece& pattern,
    BoundExpression* substitute_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> haystack(haystack_ptr);
  scoped_ptr<BoundExpression> substitute(substitute_ptr);
  string name =
      BinaryExpressionTraits<OPERATOR_REGEXP_REPLACE>::FormatDescription(
          GetExpressionName(haystack_ptr), GetExpressionName(substitute_ptr));

  FailureOrVoid haystack_check = CheckExpressionType(STRING, haystack.get());
  PROPAGATE_ON_FAILURE(haystack_check);
  FailureOrVoid sub_check = CheckExpressionType(STRING, substitute.get());
  PROPAGATE_ON_FAILURE(sub_check);
  scoped_ptr<const RE2> pattern_(new RE2(pattern.ToString()));
  if (!pattern_->ok()) {
    string message = StrCat("Malformed regexp: ", pattern, ", parse error: ",
                            pattern_->error(), " in ", name);
    THROW(new Exception(ERROR_INVALID_ARGUMENT_VALUE, message));
  }
  return InitBasicExpression(
      max_row_count,
      new BoundRegexpReplaceExpression(name, allocator, haystack.release(),
                                       substitute.release(),
                                       pattern_.release()),
      allocator);
}

}  // namespace supersonic
