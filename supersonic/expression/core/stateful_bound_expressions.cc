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

#include "supersonic/expression/core/stateful_bound_expressions.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/aggregation_operators.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/vector/vector_logic.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

using aggregations::AssignmentOperator;
using aggregations::AggregationOperator;

namespace {
// General implementation remark - the stateful expressions basically do not
// cooperate well with the short circuit mechanism, due to the fact that their
// state could change on rows which the short circuit would tell us to skip.
// Thus, all implementations actually ignore the skip vector, in the sense of
// evaluating all the inputs and then OR-ing the null vector of the result (if
// it exists) onto the original skip vector.

// The implementation of the Changed BoundExpression. See stateful_expressions.h
// for comments on semantics, ../base/expression.h for the BoundExpression
// parent class, ../infrastructure/basic_bound_expression.h for the
// BoundUnaryExpression class this inherits from.
// This expression does not fall under the abstract templated expressions
// framework of Supersonic (../templated), as it is a stateful expression, and
// the abstract expressions do not support state.
// We will need to iterate over the input, which is of unspecified type, so we
// need to template here.
template<DataType input_type>
class BoundChangedExpression : public BoundUnaryExpression {
 public:
  BoundChangedExpression(BufferAllocator* const allocator,
                         BoundExpression* argument)
      : BoundUnaryExpression(CreateChangedSchema(argument),
                             allocator, argument, input_type),
        initialized_(false),
        local_skip_vector_storage_(1, allocator),
        assignment_operator_(allocator) {}

 private:
  typedef typename TypeTraits<input_type>::cpp_type CppType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    bit_pointer::FillWithFalse(local_skip_vector_storage_.view().column(0),
                               my_block()->row_capacity());
    return Success();
  }

  FailureOrVoid RememberValue(const CppType& value) {
    if (!assignment_operator_(value, &state_, &buffer)) {
      THROW(new Exception(
          ERROR_MEMORY_EXCEEDED,
          StrCat("Not enough buffer memory space for buffering the state ",
                 "between blocks. State: ", state_, ", Expression: ",
                 result_schema().GetHumanReadableSpecification())));
    }
    return Success();
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    // We are ignoring the skip vector.
    // TODO(onufry): In the case of this expression, what we need is just the
    // previous expression to the one that we currently evaluate, so we could
    // actually skip all but the last expression in a sequence of expressions
    // rows to skip. Not sure it's worth it, though - check.
    CHECK_EQ(1, skip_vectors.column_count());
    // Typically we would run ResetArenas here, but we know the output block to
    // be boolean (and thus have no arena).
    rowcount_t row_count = input.row_count();

    // As the child expression is not nullable, the skip vector should not be
    // modified - we should actually get all the values calculated in the child.
    // We'll check for this in debug mode.
    DCHECK(bit_pointer::PopCount(
        local_skip_vector_storage_.view().column(0), row_count) == 0);
    // This means we do not have to refill the vector with false values every
    // iteration, that's nice.

    EvaluationResult child_result =
        argument()->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(child_result);
    const CppType* argument_data =
        child_result.get().column(0).typed_data<input_type>();
    bool* result_data =
        my_block()->mutable_column(0)->template mutable_typed_data<BOOL>();

    // We do not want to check the value of initialized_ at every single step of
    // the evaluation loop. Thus, we will instead check it at the beginning, and
    // deal with the first row of the input separately. This requires the first
    // row to exist, so we resolve this corner case first.
    if (row_count == 0) {
      my_view()->set_row_count(0);
      return Success(*my_view());
    }
    if (!initialized_) {
      initialized_ = true;
      *result_data = true;
    } else {
      // The contract is that if initialized_ is true, then state_ is set.
      *result_data = (*argument_data != state_);
    }
    // Now we process the remaining row_count - 1 rows, remembering to begin
    // with incrementing the pointers and update the state.
    for (rowid_t row = 1; row < row_count; ++row) {
      state_ = *argument_data;
      ++result_data;
      ++argument_data;
      // It's probably cheaper to copy always than to branch on this.
      // TODO(onufry): benchmark and verify.
      *result_data = (*argument_data != state_);
    }
    // In the unfortunate case of variable length data we need to store the last
    // value somewhere so it will persist. The "somewhere" is a buffer that is
    // managed by the appropriate assignment operator. This is done by the
    // RememberValue function. In the case of constant-length types
    // RememberValue does not allocate memory.
    PROPAGATE_ON_FAILURE(RememberValue(*argument_data));

    my_view()->set_row_count(row_count);
    return Success(*my_view());
  }

  TupleSchema CreateChangedSchema(BoundExpression* argument) {
    return CreateSchema(StrCat("CHANGED(", GetExpressionName(argument), ")"),
                       /* output_type = */ BOOL,
                       /* output is nullable = */ false);
  }

  // Does the expression have any state set yet?
  bool initialized_;
  // The storage for the local skip vector (preinitialized and always
  // set to "all false").
  BoolBlock local_skip_vector_storage_;
  // An operator that manages the memory for storing the state between the calls
  // to evaluate.
  AssignmentOperator<input_type, input_type> assignment_operator_;
  // The state, meaning the last seen value.
  CppType state_;
  // A buffer to hold the state if it is variable length.
  std::unique_ptr<Buffer> buffer;

  DISALLOW_COPY_AND_ASSIGN(BoundChangedExpression);
};

// This is a part of the TypeSpecialization pattern. See types_infrastructure.h.
struct BoundChangedFactory {
  BoundChangedFactory(BufferAllocator* allocator,
                      BoundExpression* argument,
                      rowcount_t max_row_count)
      : allocator_(allocator),
        argument_(argument),
        max_row_count_(max_row_count) {}

  template<DataType type>
  FailureOrOwned<BoundExpression> operator()() const {
    return InitBasicExpression(
        max_row_count_,
        new BoundChangedExpression<type>(allocator_, argument_),
        allocator_);
  }

  BufferAllocator* allocator_;
  BoundExpression* argument_;
  rowcount_t max_row_count_;
};

// -----------------------------------------------------------------------------
// Implementation of the BoundRunningMinWithFlush expression.
template<DataType input_type>
class BoundRunningMinWithFlushExpression : public BoundBinaryExpression {
 public:
  BoundRunningMinWithFlushExpression(BufferAllocator* const allocator,
                                     BoundExpression* flush,
                                     BoundExpression* input)
      : BoundBinaryExpression(CreateRunningMinWithFlushSchema(flush, input),
                              allocator, flush, BOOL, input, input_type),
        local_skip_vector_storage_(1, allocator), is_null_(true) {
    COMPILE_ASSERT(TypeTraits<input_type>::is_integer,
                   RunningMinWithFlush_may_only_be_used_on_integer_types);
  }

 private:
  typedef typename TypeTraits<input_type>::cpp_type CppType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    return Success();
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    my_block()->ResetArenas();
    rowcount_t row_count = input.row_count();

    // We reuse the same local_skip_vector for both child DoEvalute calls
    // without refilling it because we have already checked that the left
    // child (flush expression) is not nullable. We verify this is the case with
    // a DCHECK again below.
    bit_pointer::FillWithFalse(local_skip_vector_storage_.view().column(0),
                               row_count);
    EvaluationResult flush_result =
        left()->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(flush_result);
    DCHECK(bit_pointer::PopCount(
           local_skip_vector_storage_.view().column(0), row_count) == 0);
    EvaluationResult input_result =
        right()->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(input_result);

    const bool* flush_data =
        flush_result.get().column(0).typed_data<BOOL>();
    const CppType* input_data =
        input_result.get().column(0).typed_data<input_type>();
    bool_const_ptr input_is_null = input_result.get().column(0).is_null();
    CppType* result_data = my_block()->mutable_column(0)->
        template mutable_typed_data<input_type>();

    // We branch on input_is_null outside the for loop so the code can be as
    // efficient as possible inside the loop. This leads to a bit of code
    // duplication but this slight overhead seems worthwhile.
    if (input_is_null != NULL) {
      bool_ptr skip_data = skip_vectors.column(0);
      for (rowid_t row = 0; row < row_count; ++row) {
        if (!*input_is_null) {
          ResetState(*input_data);
        }
        *skip_data |= is_null_;
        *result_data = current_min_;
        is_null_ |= *flush_data;

        ++input_data;
        ++input_is_null;
        ++flush_data;
        ++result_data;
        ++skip_data;
      }
    } else {
      for (rowid_t row = 0; row < row_count; ++row) {
        ResetState(*input_data);
        *result_data = current_min_;
        is_null_ |= *flush_data;

        ++input_data;
        ++flush_data;
        ++result_data;
      }
    }
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    my_view()->set_row_count(row_count);
    return Success(*my_view());
  }

  TupleSchema CreateRunningMinWithFlushSchema(const BoundExpression* flush,
                                              const BoundExpression* input) {
    return CreateSchema(StrCat("RUNNING_MIN_WITH_FLUSH(",
                               GetExpressionName(flush), ", ",
                               GetExpressionName(input), ")"),
                        input_type, GetExpressionNullability(input));
  }

  inline void ResetState(const CppType& input_data) {
    if (is_null_) {
      current_min_ = input_data;
      is_null_ = false;
    } else {
      current_min_ = min(current_min_, input_data);
    }
  }

  // The storage and view for the local skip vector (preinitialized and always
  // set to "all false" inside DoEvaluate).
  BoolBlock local_skip_vector_storage_;
  // Variable for maintaining expression state. Starts as NULL (i.e., is_null_
  // == true) and reset to NULL each time the result of the flush (left)
  // expression is true. Remains NULL until a non-NULL value is received from
  // the input (right) expression and maintains the minimum value seen,
  // disregarding subsequent NULL values.
  bool is_null_;
  CppType current_min_;

  DISALLOW_COPY_AND_ASSIGN(BoundRunningMinWithFlushExpression);
};

// This is a part of the TypeSpecialization pattern. See types_infrastructure.h.
struct BoundRunningMinWithFlushFactory {
  BoundRunningMinWithFlushFactory(BufferAllocator* allocator,
                                  BoundExpression* flush,
                                  BoundExpression* input,
                                  rowcount_t max_row_count)
      : allocator_(allocator),
        flush_(flush),
        input_(input),
        max_row_count_(max_row_count) {}

  template<DataType type>
  FailureOrOwned<BoundExpression> operator()() const {
    return InitBasicExpression(
        max_row_count_,
        new BoundRunningMinWithFlushExpression<type>(
            allocator_, flush_, input_),
        allocator_);
  }

  BufferAllocator* allocator_;
  BoundExpression* flush_;
  BoundExpression* input_;
  rowcount_t max_row_count_;
};

// -----------------------------------------------------------------------------
// The implementation of running aggregations.

// A persister is an object that is able to store a variable length piece of
// data - it is able to persist the state to its internal buffer, and to
// Fetch the state from its buffer to the arena.
// If persist is set to false, both methods are no-ops, and take arbitrary
// pointers as arguments, so that the Persister can be freely used in
// specializations for types other than StringPiece.
template<bool persist>
struct Persister {
  Persister(BufferAllocator* allocator,
            Arena* arena,
            const string& description) {}

  FailureOrVoid Persist(bool initialized, void* state) { return Success(); }
  FailureOrVoid FetchToArena(bool initialized, void* state) {
    return Success();
  }
};

template<>
struct Persister<true> {
  Persister(BufferAllocator* allocator,
            Arena* arena,
            const string& description)
      : arena_(CHECK_NOTNULL(arena)),
        description_(description),
        assigner_(allocator) {}

  // If initialized is set to false, we assume there is nothing to persist, and
  // perform a no-op. This is pulled into here (instead of having the if before
  // the function call) to avoid the branching in the case the whole Persister
  // is a no-op.
  // The state after this call will point to persisted content (which is equal
  // to the old content, but lies in the internal buffer of the persister).
  FailureOrVoid Persist(bool initialized, StringPiece* state) {
    if (!initialized) return Success();
    if (!assigner_(*state, state, &buffer_)) {
       THROW(new Exception(ERROR_MEMORY_EXCEEDED, StrCat(
          "Unable to persist state to buffer in: ", description_)));
    }
    return Success();
  }

  // The state has to point to the result of a previous call to Persist,
  // otherwise undefined behavior occurs.
  FailureOrVoid FetchToArena(bool initialized, StringPiece* state) {
    if (initialized) {
      const char* data_in_arena = arena_->AddStringPieceContent(*state);
      if (data_in_arena == NULL) {
        THROW(new Exception(ERROR_MEMORY_EXCEEDED, StrCat(
              "Unable to copy state from buffer to the arena in: ",
              description_)));
      }
      *state = StringPiece(data_in_arena, state->length());
    }
    return Success();
  }

 private:
  Arena* arena_;
  string description_;
  AssignmentOperator<STRING, STRING, true> assigner_;
  std::unique_ptr<Buffer> buffer_;
};


// The semantics of a running aggregation are that the input column is
// aggregated row by row, and the output in a particular row is the value of the
// aggregator after aggregating this row. NULL inputs do not change the value of
// the aggregator; if no non-NULL values appeared yet, a NULL is returned.
template<Aggregation aggregation_type, DataType input_type>
class BoundRunningAggregationExpression : public BoundUnaryExpression {
 public:
  BoundRunningAggregationExpression(BufferAllocator* const allocator,
                                    BoundExpression* argument)
      : BoundUnaryExpression(CreateRunningAggregationSchema(argument),
                             allocator, argument, input_type),
        initialized_(false),
        local_skip_vector_storage_(1, allocator),
        aggregation_operator_(allocator),
        persister_(
            allocator,
            my_block()->mutable_column(0)->arena(),
            my_view()->schema().GetHumanReadableSpecification()) {}

  // I mean, c'mon, RunningSum(ConstInt32(1)) is not constant.
  virtual bool can_be_resolved() const { return false; }

 private:
  typedef typename TypeTraits<input_type>::cpp_type CppType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "",
        result_schema().GetHumanReadableSpecification());
    bit_pointer::FillWithFalse(local_skip_vector_storage_.view().column(0),
                               my_block()->row_capacity());
    return Success();
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    // If we are storing any variable-length data, clear the arenas.
    if (TypeTraits<input_type>::is_variable_length) {
      my_block()->ResetArenas();
    }
    PROPAGATE_ON_FAILURE(persister_.FetchToArena(initialized_, &state_));

    CHECK_EQ(1, skip_vectors.column_count());
    rowcount_t row_count = input.row_count();
    bool child_is_nullable =
        argument()->result_schema().attribute(0).nullability() == NULLABLE;

    // If the child expression is nullable, it might have modified the skip
    // vector, thus we reset it to all false. If it is not nullable, it should
    // not have modified the skip vector, we DCHECK for this just in case.
    if (child_is_nullable) {
      bit_pointer::FillWithFalse(local_skip_vector_storage_.view().column(0),
                                 my_block()->row_capacity());
    }
    DCHECK(bit_pointer::PopCount(
        local_skip_vector_storage_.view().column(0), row_count) == 0);

    EvaluationResult child_result =
        argument()->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(child_result);
    const CppType* argument_data =
        child_result.get().column(0).typed_data<input_type>();
    bool_const_ptr argument_nulls = child_result.get().column(0).is_null();
    CppType* result_data = my_block()->mutable_column(0)->
        template mutable_typed_data<input_type>();

    rowid_t row = 0;
    // The leading NULLs - if we are not initialized, we skip the leading NULLs
    // (writing NULLs on the output).
    while (child_is_nullable && !initialized_ && row < row_count
           && argument_nulls[row]) {
      skip_vectors.column(0)[row] |= true;
      ++row;
    }
    if (row == row_count) {
      my_view()->set_row_count(row_count);
      my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
      return Success(*my_view());
    }

    // If we were not initialized, we have the first non-NULL value in our hands
    // at this point, so we store it.
    if (!initialized_) {
      state_ = argument_data[row];
      result_data[row] = state_;
      initialized_ = true;
      row++;
    }

    // Now we are assured to have a state prepared, and we can process further
    // rows.
    if (!child_is_nullable) {
      for (; row < row_count; ++row) {
        // We can pass NULL in as the Buffer argument, as the
        // aggregation_operator is shallow-copying.
        aggregation_operator_(argument_data[row], &state_, NULL);
        result_data[row] = state_;
      }
    } else {
      for (; row < row_count; ++row) {
        if (!argument_nulls[row]) {
          // We can pass NULL in as the Buffer argument, as the
          // aggregation_operator is shallow-copying.
          aggregation_operator_(argument_data[row], &state_, NULL);
        }
        result_data[row] = state_;
      }
    }
    // We have to persist the state somewhere, as it might disappear (or, to be
    // more precise, be overwritten) by a call to Evaluate to our child.
    PROPAGATE_ON_FAILURE(persister_.Persist(initialized_, &state_));

    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    my_view()->set_row_count(row_count);
    return Success(*my_view());
  }

  TupleSchema CreateRunningAggregationSchema(BoundExpression* argument) {
    return CreateSchema(StrCat("RUNNING_",
                               Aggregation_Name(aggregation_type),
                               "(", GetExpressionName(argument), ")"),
                        /* output_type = */ input_type,
                        GetExpressionNullability(argument) == NULLABLE);
  }

  // Does the expression have any state set yet?
  bool initialized_;
  // The storage for the local skip vector (set to "all false" on each
  // evaluation call).
  BoolBlock local_skip_vector_storage_;
  // An aggregation operator that handles all subsequent values.
  AggregationOperator<aggregation_type, input_type,
      input_type, /* deep copy = */ false> aggregation_operator_;
  // The state, meaning the last seen value.
  CppType state_;
  // A Persister used to store the state between evaluate calls. If the input
  // type is not variable length, the persister is a dummy variable.
  typename base::if_<TypeTraits<input_type>::is_variable_length,
           Persister<true>, Persister<false> >::type persister_;

  DISALLOW_COPY_AND_ASSIGN(BoundRunningAggregationExpression);
};


// This is a part of the TypeSpecialization pattern. See types_infrastructure.h.
template<Aggregation aggregation_type>
struct BoundRunningAggregationFactory {
  BoundRunningAggregationFactory(BufferAllocator* allocator,
                                 BoundExpression* argument,
                                 rowcount_t max_row_count)
      : allocator_(allocator),
        argument_(argument),
        max_row_count_(max_row_count) {}

  template<DataType type>
  FailureOrOwned<BoundExpression> operator()() const {
    return InitBasicExpression(
        max_row_count_,
        new BoundRunningAggregationExpression<aggregation_type, type>(
            allocator_, argument_),
        allocator_);
  }

  BufferAllocator* allocator_;
  BoundExpression* argument_;
  rowcount_t max_row_count_;
};

// -----------------------------------------------------------------------------
// Implementation of the SmudgeIf expression.
template<DataType data_type>
class BoundSmudgeIfExpression : public BoundBinaryExpression {
 public:
  BoundSmudgeIfExpression(BoundExpression* argument,
                          BoundExpression* condition,
                          BufferAllocator* const allocator)
      : BoundBinaryExpression(CreateSmudgeIfSchema(argument, condition),
                              allocator, argument, data_type,
                              condition, /* right_expression_type = */ BOOL),
        local_skip_vector_storage_(1, allocator),
        local_skip_vectors_(1),
        last_output_value_id_(0),
        has_output_any_value_(false) {}

 private:
  typedef typename TypeTraits<data_type>::cpp_type CppType;
  typedef typename TypeTraits<data_type>::hold_type HoldType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    local_skip_vectors_.ResetColumn(
        0, local_skip_vector_storage_.view().column(0));
    return Success();
  }

  // 1) Evaluate both expressions with a skip vector that consists entirely of
  // false.
  // 2) Assign the value of the first row:
  // 2a) If this is not the first input received or if condition is false,
  // assign it the same value as the the first row in argument.
  // 2b) Otherwise, assign it to the last output value.
  // 3) Iterate over the remaining rows. Assign it as the corresponding value
  // in the evaluation result of argument unless the condition resolves to true,
  // in which case the value is assigned as the last output's value which we
  // store. Then, we also update the resulting skip vector.
  // 4) Memorize the last output value. We cannot discard the previous value
  // until next call to next because it may get referenced by the current view.
  // 5) Reset the isnull column.
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    my_block()->ResetArenas();
    rowcount_t row_count = input.row_count();
    CHECK_LE(row_count, local_skip_vector_storage_.row_capacity());

    // 1) Evaluate both expressions. We reuse the memory from local_skip_vector_
    // for both condition and argument since condition is not nullable.
    bit_pointer::FillWithFalse(condition_skip_vector(),
                               row_count);
    local_skip_vectors_.set_row_count(row_count);
    EvaluationResult condition_result =
        right()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(condition_result);

    // Argument skip vector uses the same memory as condition skip vector since
    // it's not used (because condition is not nullable). Also because it's not
    // nullable, the contract states that condition should not modify the skip
    // vector feeded into it.
    DCHECK_EQ(bit_pointer::PopCount(argument_skip_vector(), row_count), 0);
    EvaluationResult argument_result =
        left()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(argument_result);

    // Note that argument may be not nullable, we use the skip vector instead.
    bool_const_ptr argument_skip_vector_iterator =
        argument_skip_vector();
    const bool* condition_data =
        condition_result.get().column(0).typed_data<BOOL>();
    const CppType* argument_data =
        argument_result.get().column(0).typed_data<data_type>();

    CppType* result_data = my_block()->mutable_column(0)->
        template mutable_typed_data<data_type>();
    bool_ptr skip_vector = skip_vectors.column(0);

    // 2) Assign the value for first row, unless...
    if (row_count == 0) {
      return argument_result;
    }
    // We keep another pointer to skip vector and result data that lags by
    // one iteration to smudge it.
    const CppType* lagged_result_data = result_data;

    // We assign the value of first row.
    bool previous_result_is_null;
    if (!has_output_any_value_ || !*condition_data) {
      // Case 2a) happens.
      *skip_vector |= *argument_skip_vector_iterator;
      previous_result_is_null = *argument_skip_vector_iterator;
      *result_data = *argument_data;
    } else {
      // Case 2b) happens.
      *skip_vector |= last_skip_vector_value_;
      previous_result_is_null = last_skip_vector_value_;
      if (last_skip_vector_value_ != true) {
        *result_data = TypeTraits<data_type>::HoldTypeToCppType(
            last_output_values_[last_output_value_id_]);
      }
    }
    ++skip_vector;
    ++argument_data;
    ++argument_skip_vector_iterator;
    ++result_data;
    ++condition_data;

    // 3) Iterate over all other rows.
    for (rowcount_t i = 1; i < row_count; ++i) {
      if (*condition_data) {
        // Smudge it.
        *skip_vector |= previous_result_is_null;
        *result_data = *lagged_result_data;
      } else {
        // Copy it from argument.
        *skip_vector = *argument_skip_vector_iterator;
        previous_result_is_null = *argument_skip_vector_iterator;
        *result_data = *argument_data;
      }
      ++skip_vector;
      ++argument_data;
      ++argument_skip_vector_iterator;
      ++result_data;
      ++condition_data;
      ++lagged_result_data;
    }

    // 4) Memorize the last output value.
    has_output_any_value_ = true;
    // Since the lagged_* is lagged by 1 iteration, they should point to the
    // last row at the end of the iteration.
    last_skip_vector_value_ = previous_result_is_null;
    // Last output value id should alternate between 0 and 1 to indicate which
    // one of the last output values actually point to the last value pointed
    // by the last call to Next().
    last_output_value_id_ ^= 1;
    if (last_skip_vector_value_ != true) {
      last_output_values_[last_output_value_id_] =
          TypeTraits<data_type>::CppTypeToHoldType(*lagged_result_data);
    }
    // 5) Reset nullability column.
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    my_view()->set_row_count(row_count);
    return Success(*my_view());
  }

  TupleSchema CreateSmudgeIfSchema(const BoundExpression* argument,
                                   const BoundExpression* condition) {
    return CreateSchema(StrCat("SMUDGE_IF(",
                               GetExpressionName(argument), ", ",
                               GetExpressionName(condition), ")"),
                        data_type,
                        GetExpressionNullability(argument));
  }

  bool_ptr condition_skip_vector() {
    return local_skip_vector_storage_.view().column(0);
  }

  bool_ptr argument_skip_vector() {
    return local_skip_vector_storage_.view().column(0);
  }

  // The storage and view for the skip vector to be passed to condition and
  // argument (preinitialized and always set to "all false" inside DoEvaluate).
  // This will not be used in condition since it is not nullable, and at the
  // end this will contain the skip vector from argument.
  BoolBlock local_skip_vector_storage_;
  BoolView local_skip_vectors_;

  // Stores the two last values returned by the two last calls to Next().
  HoldType last_output_values_[2];
  // last_output_values_[last_output_value_id_] is the last value pointed by
  // the last call to Next().
  int last_output_value_id_;
  bool last_skip_vector_value_;
  bool has_output_any_value_;

  DISALLOW_COPY_AND_ASSIGN(BoundSmudgeIfExpression);
};

// Functor used in the TypeSpecialization setup (see types_infrastructure.h).
struct BoundSmudgeIfResolver {
  BoundSmudgeIfResolver(BoundExpression* argument,
                        BoundExpression* condition,
                        BufferAllocator* allocator,
                        rowcount_t max_row_count)
      : allocator_(allocator),
        argument_(argument),
        condition_(condition),
        max_row_count_(max_row_count) {}

  template<DataType data_type>
  FailureOrOwned<BoundExpression> operator()() const {
    return InitBasicExpression(
        max_row_count_,
        new BoundSmudgeIfExpression<data_type>(
            argument_, condition_, allocator_),
        allocator_);
  }
  BufferAllocator* allocator_;
  BoundExpression* argument_;
  BoundExpression* condition_;
  rowcount_t max_row_count_;
};

}  // namespace

FailureOrOwned<BoundExpression> BoundChanged(BoundExpression* argument_ptr,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(argument.get()), argument->result_schema(), 1));
  if (GetExpressionNullability(argument.get()) == NULLABLE) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_IS_NULLABLE,
        StrCat("The CHANGED expression is not implemented for nullable "
               "arguments, while an attempt to create it was made to create it "
               "for: ", GetExpressionName(argument.get()))));
  }
  // TypeSpecialization application.
  BoundChangedFactory factory(allocator, argument.release(), max_row_count);
  FailureOrOwned<BoundExpression> result = TypeSpecialization<
      FailureOrOwned<BoundExpression>, BoundChangedFactory>(
          GetExpressionType(factory.argument_), factory);
  PROPAGATE_ON_FAILURE(result);
  return Success(result.release());
}

FailureOrOwned<BoundExpression> BoundRunningSum(BoundExpression* argument_ptr,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(argument.get()), argument->result_schema(), 1));
  const TypeInfo& type_info = GetTypeInfo(GetExpressionType(argument.get()));
  if (!type_info.is_numeric()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("The RUNNING_SUM expression is not viable for "
               "non-numeric input: ", GetExpressionName(argument.get()))));
  }
  BoundRunningAggregationFactory<SUM> factory(
      allocator, argument.release(), max_row_count);
  FailureOrOwned<BoundExpression> result = NumericTypeSpecialization<
      FailureOrOwned<BoundExpression>, BoundRunningAggregationFactory<SUM> >(
          GetExpressionType(factory.argument_), factory);
  PROPAGATE_ON_FAILURE(result);
  return Success(result.release());
}

FailureOrOwned<BoundExpression> BoundSmudge(BoundExpression* argument_ptr,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(argument.get()), argument->result_schema(), 1));
  BoundRunningAggregationFactory<LAST> factory(
      allocator, argument.release(), max_row_count);
  FailureOrOwned<BoundExpression> result = TypeSpecialization<
      FailureOrOwned<BoundExpression>, BoundRunningAggregationFactory<LAST> >(
          GetExpressionType(factory.argument_), factory);
  PROPAGATE_ON_FAILURE(result);
  return Success(result.release());
}

FailureOrOwned<BoundExpression> BoundRunningMinWithFlush(
    BoundExpression* flush_ptr,
    BoundExpression* input_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> flush(flush_ptr);
  scoped_ptr<BoundExpression> input(input_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(flush.get()), flush->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(input.get()), input->result_schema(), 1));
  if (GetExpressionNullability(flush.get()) == NULLABLE) {
    THROW(new Exception(
        ERROR_NOT_IMPLEMENTED,
        StrCat("The RUNNING_MIN_WITH_FLUSH expression is not implemented for "
               "nullable flush values: ", GetExpressionName(flush.get()),
               ".")));
  }
  if (GetExpressionType(flush.get()) != BOOL) {
    THROW(new Exception(
        ERROR_INVALID_ARGUMENT_TYPE,
        StrCat("The flush expression passed to RUNNING_MIN_WITH_FLUSH must be "
               "of type BOOL: ", GetExpressionName(flush.get()))));
  }
  const TypeInfo& type_info = GetTypeInfo(GetExpressionType(input.get()));
  if (!type_info.is_integer()) {
    THROW(new Exception(
        ERROR_NOT_IMPLEMENTED,
        StrCat("The RUNNING_MIN_WITH_FLUSH expression is not implemented for "
               "non-integer input: ", GetExpressionName(input.get()),
               ".")));
  }

  BoundRunningMinWithFlushFactory factory(
      allocator, flush.release(), input.release(), max_row_count);
  FailureOrOwned<BoundExpression> result =
      IntegerTypeSpecialization<FailureOrOwned<BoundExpression>,
                                BoundRunningMinWithFlushFactory>(
                                    type_info.type(), factory);
  PROPAGATE_ON_FAILURE(result);
  return result;
}

FailureOrOwned<BoundExpression> BoundSmudgeIf(
    BoundExpression* argument_raw,
    BoundExpression* condition_raw,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_raw);
  scoped_ptr<BoundExpression> condition(condition_raw);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(condition.get()),
      condition->result_schema(),
      1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      GetExpressionName(argument.get()), argument->result_schema(), 1));
  if (GetExpressionType(condition.get()) != BOOL) {
    THROW(new Exception(
        ERROR_INVALID_ARGUMENT_TYPE,
        StrCat("The condition expression passed to SMUDGE_IF must be "
               "of type BOOL: ", GetExpressionName(condition.get()))));
  }
  if (GetExpressionNullability(condition.get()) != NOT_NULLABLE) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_IS_NULLABLE,
        StrCat("The condition expression passed to SMUDGE_IF must not be "
               "NULLABLE: ", GetExpressionName(condition.get()))));
  }
  DataType argument_type = GetExpressionType(argument.get());
  BoundSmudgeIfResolver resolver(
      argument.release(), condition.release(), allocator, max_row_count);
  FailureOrOwned<BoundExpression> result =
      TypeSpecialization<FailureOrOwned<BoundExpression>,
          BoundSmudgeIfResolver>(argument_type, resolver);
  PROPAGATE_ON_FAILURE(result);
  return result;
}

}  // namespace supersonic
