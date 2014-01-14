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

#include "supersonic/expression/core/elementary_bound_expressions.h"

#include <stddef.h>
#include <string.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/operators.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/abstract_bound_expressions.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/expression/templated/cast_bound_expression.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/expression/vector/vector_logic.h"
#include "supersonic/expression/vector/vector_primitives.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/fixedarray.h"

namespace supersonic {

namespace {
class BoundIsNullExpression : public BoundUnaryExpression {
 public:
  BoundIsNullExpression(BufferAllocator* const allocator,
                        BoundExpression* arg)
      : BoundUnaryExpression(
            TupleSchema::Singleton(
                "ISNULL(" + arg->result_schema().attribute(0).name() + ")",
                BOOL,
                NOT_NULLABLE),
            allocator,
            arg,
            arg->result_schema().attribute(0).type()),
        local_skip_vector_storage_(1, allocator) {}

  // DoEvaluate does the following:
  // 1) Make a copy of the incoming skip vector, because the IsNull expression
  // does not follow viral NULL semantics - the child being NULL does not cause
  // the output to be NULL, so we do not want to let the child to modify the
  // skip vector.
  // 2) Evaluate the child with the copy skip vector.
  // 3) Copy the skip vector modified by the child to our results - if the child
  // is NULL (so the skip vector is set to true), we return true, otherwise we
  // return false. Note that it doesn't matter what we return for the rows for
  // which the incoming skip vector was set to true (we happen to return true).
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    // Normally, we would run ResetArenas here, but in this case we know the
    // output block is boolean (and so has no Arena to reset).
    bool_ptr skip_vector = skip_vectors.column(0);
    size_t row_count = input.row_count();

    // This realizes 1) - we copy the incoming skip vector to a local copy.
    bit_pointer::FillFrom(child_skip_vector(), skip_vector, row_count);
    // We set local_skip_vectors_[0] to child_skip_vector in PostInit.
    // This realizes 2). As always, we need to pass a vector<bool_ptr> of
    // skip_vectors, we keep it preallocated and pre-prepared at binding time.
    EvaluationResult argument_result =
        argument()->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(argument_result);

    // This realizes 3) - we copy data from the child_skip_vector to our result
    // data block.
    bool* destination =
        my_block()->mutable_column(0)->mutable_typed_data<BOOL>();
    bit_pointer::FillFrom(destination, child_skip_vector(), row_count);

    my_view()->set_row_count(input.row_count());
    // The expression is guaranteed to be not-null, so we do not reset the
    // is_null vector.
    return Success(*my_view());
  }

 private:
  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    return Success();
  }

  BoolBlock local_skip_vector_storage_;
  bool_ptr child_skip_vector() {
    return local_skip_vector_storage_.view().column(0);
  }

  DISALLOW_COPY_AND_ASSIGN(BoundIsNullExpression);
};

template<DataType type>
class BoundIfNullExpression : public BoundBinaryExpression {
 public:
  BoundIfNullExpression(BufferAllocator* allocator,
                        BoundExpression* expression,
                        BoundExpression* substitute)
      : BoundBinaryExpression(
            TupleSchema::Singleton(
                StrCat("IFNULL(",
                       expression->result_schema().attribute(0).name(),
                       ", ",
                       substitute->result_schema().attribute(0).name(),
                       ")"),
                type,
                substitute->result_schema().attribute(0).nullability()),
            allocator,
            expression,
            type,
            substitute,
            type),
        local_skip_vector_storage_(2, allocator),
        local_skip_vectors_(1) {}

  // 1) Make a copy of the incoming skip vector, because the IfNull expression
  // does not follow viral NULL semantics - one child being NULL does not cause
  // the output to be NULL, so we do not want to let the children to modify the
  // skip vector.
  // 2) Evaluate the left child (expression) with the copied skip_vector.
  // 3) We prepare another skip vector for the right child (substitute). The
  // substitute should be evaluated only if a) we are interested in the result
  // (that is, the incoming skip vector is not set to true), and b) we will use
  // the substitute (that is, the expression _did_ evaluate to NULL). So, we
  // skip a row of the substitute iff
  // !(!parent_skip_vector && left_child_skip_vector).
  // 4) We evaluate the substitute with the new skip vector.
  // 5) We mark the original skip vector as true if the result is NULL, which
  // happens if substitute was evaluated (so the expression three lines above is
  // false) and the resulting skip vector is true.
  // 6) Copy the data from the appropriate column (expression or substitute)
  // into our result block.
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    // Normally we would run ResetArenas here, but IfNull is guaranteed not to
    // create any new strings.
    bool_ptr skip_vector = skip_vectors.column(0);
    rowcount_t row_count = input.row_count();

    // Step 1) - copy the skip vector.
    bit_pointer::FillFrom(child_skip_vector(), skip_vector, row_count);
    // We set local_skip_vectors to point to child_skip_vector at PostInit.
    // Step 2) - evaluation. We need to pass a vector<bool_ptr> containing the
    // single skip vector we created, we pre-prepare it during binding to spare
    // time during evaluation.
    EvaluationResult expression_result =
        left()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(expression_result);

    // Step 3) - prepare the skip vector for the substitute. We evaluate the
    // substitute if the skip_vector was not set and the expression is null.
    vector_logic::AndNot(skip_vector, child_skip_vector(), row_count, temp());
    vector_logic::Not(temp(), row_count, child_skip_vector());
    // We set local_skip_vectors to point to child_skip_vector at PostInit.
    // Step 4) - evaluation. Again we put the new skip vector in the same place,
    // so we pass the same pre-prepared vector<bool_ptr>.
    EvaluationResult substitute_result =
        right()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(substitute_result);

    // Step 5) - modifying the parent skip vector.
    // The output is null if the substitute was evaluated (temp() is true on
    // these rows), and returned a set bit in the skip_vector.
    vector_logic::And(temp(), child_skip_vector(), row_count, temp());
    vector_logic::Or(skip_vector, temp(), row_count, skip_vector);

    // Step 6) Copying the data.
    // We now copy the data from the children to our block. Note that in the
    // case when the parent skip_vector is set it is totally irrelevant what
    // data are copied. We have child_skip_vector in our hands, and for the
    // places where it is set we can copy expression (because either we should
    // copy expression, or the result is null anyway - only where
    // child_skip_vector is false we care about the substitute value).
    const CppType* expression_data =
        expression_result.get().column(0).typed_data<type>();
    const CppType* substitute_data =
        substitute_result.get().column(0).typed_data<type>();
    CppType* result_data =
        my_block()->mutable_column(0)->template mutable_typed_data<type>();
    bool_ptr child_skip_iterator = child_skip_vector();
    for (rowid_t row = 0; row < row_count; ++row) {
      // After compilation and optimization its assembler equivalent of:
      // *result_data = (*child_skip_iterator) ? *expression_data
      //                                       : *substitute_data;
      // but doesn't suffer from lvalue-to-rvalue on uninitialized variable
      // undefined behavior.
      memcpy(result_data,
             (*child_skip_iterator) ? expression_data : substitute_data,
             sizeof(CppType));
      ++result_data;
      ++expression_data;
      ++substitute_data;
      ++child_skip_iterator;
    }

    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    return Success(*my_view());
  }

 private:
  typedef typename TypeTraits<type>::cpp_type CppType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    local_skip_vectors_.ResetColumn(0, child_skip_vector());
    return Success();
  }

  // The skip vector that is going to be passed to the children.
  bool_ptr child_skip_vector() {
    return local_skip_vector_storage_.view().column(0);
  }
  // A temporary placeholder for the calculation results.
  bool_ptr temp() { return local_skip_vector_storage_.view().column(1); }
  // Storage space for the locally used skip vectors.
  BoolBlock local_skip_vector_storage_;
  // The vector that is going to be passed to the children, always contains
  // exactly one element: the child_skip_vector() pointer.
  BoolView local_skip_vectors_;

  DISALLOW_COPY_AND_ASSIGN(BoundIfNullExpression);
};

// Determine the right_skip_vector - set true for the rows we don't have to
// evaluate. This is used for boolean expressions (OR, AND, ANDNOT), and behaves
// differently for each of them. The contract is that the right_skip_vector is
// modified to have a true value on the fields for which we do not need to
// evaluate the right column, and to false if we do need to evaluate it. The
// temp_skip_vector is a pointer to an additional space that can store
// intermediate results, it can be modified and nothing can be assumed about its
// contents. All the vectors are assumed to hold row_count entries.
template<OperatorId op>
void MakeRightSkipVector(bool_const_ptr left,
                         bool_const_ptr left_skip_vector,
                         bool_const_ptr parent_skip_vector,
                         bool_ptr right_skip_vector,
                         size_t row_count,
                         bool_ptr temp_skip_vector) {}

template<>
void MakeRightSkipVector<OPERATOR_OR>(bool_const_ptr left,
                                      bool_const_ptr left_skip_vector,
                                      bool_const_ptr parent_skip_vector,
                                      bool_ptr right_skip_vector,
                                      size_t row_count,
                                      bool_ptr temp_skip_vector) {
  // The right vector does not have to be evaluated in two cases: one is when
  // the parent skip vector is set for a given row, the second is when the
  // left vector is true (and not NULL). Thus the following logic:
  vector_logic::AndNot(left_skip_vector, left, row_count, right_skip_vector);
  vector_logic::Or(right_skip_vector, parent_skip_vector,
                   row_count, right_skip_vector);
}

template<>
void MakeRightSkipVector<OPERATOR_AND>(bool_const_ptr left,
                                       bool_const_ptr left_skip_vector,
                                       bool_const_ptr parent_skip_vector,
                                       bool_ptr right_skip_vector,
                                       size_t row_count,
                                       bool_ptr temp_skip_vector) {
  // Here the right vector does not need to be evaluated if the parent skip
  // vector is set for a given row, or if the left is false (and not null). This
  // is unfortunately more difficult to evaluate.
  // The code below is pretty bad, as the vector_logic::Not is not SIMDed. But
  // we want to return true if all the three inputs are set to false, and this
  // is impossible to do without using negation. The solution, obviously, is
  // to implement a SIMD not.
  vector_logic::Not(left_skip_vector, row_count, temp_skip_vector);
  vector_logic::AndNot(left, temp_skip_vector, row_count, temp_skip_vector);
  vector_logic::Or(temp_skip_vector, parent_skip_vector,
                   row_count, right_skip_vector);
}

template<>
void MakeRightSkipVector<OPERATOR_AND_NOT>(bool_const_ptr left,
                                           bool_const_ptr left_skip_vector,
                                           bool_const_ptr parent_skip_vector,
                                           bool_ptr right_skip_vector,
                                           size_t row_count,
                                           bool_ptr temp_skip_vector) {
  // The condition (and code) here is the same as for OR: we do not evaluate
  // right when when the parent skip vector is set for a given row, or when the
  // left vector is true (and not NULL).
  vector_logic::AndNot(left_skip_vector, left, row_count, right_skip_vector);
  vector_logic::Or(right_skip_vector, parent_skip_vector,
                   row_count, right_skip_vector);
}

// Set the parent skip vector using the left and right results and skip vectors.
// As in the previous function, this is used for the boolean logical
// expressions. Again the contract is that the skip_vector is modified as needed
// (that is - it is set to true if the resulting expression is NULL, while the
// original value is kept otherwise), temp_skip_vector can be overwritten and
// nothing can be assumed about its contents, and all vectors have to hold
// row_count rows.
template<OperatorId op>
void MakeResultSkipVector(bool_const_ptr left,
                          bool_const_ptr left_skip_vector,
                          bool_const_ptr right,
                          bool_const_ptr right_skip_vector,
                          bool_ptr skip_vector,
                          size_t row_count,
                          bool_ptr temp_skip_vector) {}

template<>
void MakeResultSkipVector<OPERATOR_OR>(bool_const_ptr left,
                                       bool_const_ptr left_skip_vector,
                                       bool_const_ptr right,
                                       bool_const_ptr right_skip_vector,
                                       bool_ptr skip_vector,
                                       size_t row_count,
                                       bool_ptr temp_skip_vector) {
  // There are three cases in which the output is NULL: false OR NULL, NULL OR
  // false and NULL OR NULL. This amounts to the logical formula:
  // (left_skip && right_skip) || (left_skip && !right) || (right_skip && !left)
  // which is equivalent to
  // ((!right && left_skip) || (!(!left_skip && left) && right))
  // which we will calculate (using the temporary column).
  // Note that we can safely overwrite the parent skip vector, as all the rows
  // for which parent_skip_vector was set also have the left and right skip
  // vectors set, so we are guaranteed to recalculate such rows to true.
  vector_logic::AndNot(left_skip_vector, left, row_count, temp_skip_vector);
  vector_logic::AndNot(temp_skip_vector, right_skip_vector, row_count,
                       skip_vector);
  vector_logic::AndNot(right, left_skip_vector, row_count, temp_skip_vector);
  vector_logic::Or(temp_skip_vector, skip_vector, row_count, skip_vector);
}

template<>
void MakeResultSkipVector<OPERATOR_AND>(bool_const_ptr left,
                                        bool_const_ptr left_skip_vector,
                                        bool_const_ptr right,
                                        bool_const_ptr right_skip_vector,
                                        bool_ptr skip_vector,
                                        size_t row_count,
                                        bool_ptr temp_skip_vector) {
  // Again there are three cases where the result should be NULL: true AND NULL,
  // NULL and true, NULL and NULL; and
  // (left_skip && right_skip) || (left_skip && right) || (left && right_skip)
  // is equivalent to
  // ((left_skip || left) && ((left_skip && right) || right_skip)).
  vector_logic::And(left_skip_vector, right, row_count, temp_skip_vector);
  vector_logic::Or(temp_skip_vector, right_skip_vector, row_count, skip_vector);
  vector_logic::Or(left_skip_vector, left, row_count, temp_skip_vector);
  vector_logic::And(temp_skip_vector, skip_vector, row_count, skip_vector);
}

template<>
void MakeResultSkipVector<OPERATOR_AND_NOT>(bool_const_ptr left,
                                            bool_const_ptr left_skip_vector,
                                            bool_const_ptr right,
                                            bool_const_ptr right_skip_vector,
                                            bool_ptr skip_vector,
                                            size_t row_count,
                                            bool_ptr temp_skip_vector) {
  // Again there are three cases where the result should be NULL: false ANDNOT
  // NULL, NULL ANDNOT true, NULL ANDNOT NULL; and
  // (left_skip && right_skip) || (left_skip && right) || (!left && right_skip)
  // is equivalent to
  // (!left && right_skip) || ((right_skip || right) && left_skip).
  vector_logic::Or(right_skip_vector, right, row_count, temp_skip_vector);
  vector_logic::And(temp_skip_vector, left_skip_vector, row_count, skip_vector);
  vector_logic::AndNot(left, right_skip_vector, row_count, temp_skip_vector);
  vector_logic::Or(temp_skip_vector, skip_vector, row_count, skip_vector);
}

template<OperatorId op>
class BoundBooleanBinaryExpression : public BoundBinaryExpression {
 public:
  explicit BoundBooleanBinaryExpression(const TupleSchema& result_schema,
                                        BufferAllocator* const allocator,
                                        BoundExpression* left,
                                        BoundExpression* right)
      : BoundBinaryExpression(result_schema, allocator, left, BOOL,
                              right, BOOL),
        local_skip_vector_storage_(5, allocator),
        local_skip_vectors_(1) {
    CHECK_EQ(1, result_schema.attribute_count());
  }

  // DoEvaluate does the following:
  // 1) Make a copy of the incoming skip vector, because the boolean expressions
  // do not follow viral NULL semantics - one child being NULL does not have to
  // cause the output to be NULL, so we do not want to let the children to
  // modify the skip vector.
  // 2) Evaluate the left child with the copied skip_vector.
  // 3) We prepare another skip vector for the right child (substitute). The
  // exact form of the skip vector (which depends on the incoming skip vector
  // (it is always true if the incoming skip vector is true), the left_child
  // skip vector and the left_child result column (we can skip the
  // right-hand-side evaluation if the left-hand-side evaluated to a specific
  // logical value, and not NULL - for instance in the case of OR, if the left
  // child evaluated to TRUE, not NULL, we could skip the evaluation of the
  // right child, while if it evaluated to NULL - we still don't know and have
  // to check if the right child is true)).
  // This is delegated to a helper MakeRightSkipVector template function.
  // 4) We evaluate the right child with the new skip vector.
  // 5) We fill out the the parent skip vector, adding true values when the
  // result turn out to be NULL. This depends on both the left and the right
  // side, both on results and on skip vectors. We delegate this to the
  // MakeResultSkipVector template function. Again, the details depend on the
  // particular operator.
  // 6) Evaluate the result column. Note that we will not use the skip_vectors
  // here - if any of the children is NULL, the result will either be NULL (in
  // which case it is irrelevant what we put in the result column), or it will
  // be indpenedent of the value of this child (for instance TRUE OR NULL
  // evaluates to TRUE, because both TRUE OR TRUE and TRUE OR FALSE evaluate to
  // TRUE, so we have a guarantee that a correct value is output).

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    // Typically, we would run ResetArenas here, but we know the block is
    // actually boolean.
    bool_ptr skip_vector = skip_vectors.column(0);
    size_t row_count = input.row_count();
    // Step 1) The first input is evaluated with the same skip list as the we
    // received. We do not want the skip information from the children to be
    // propagated, though, so we copy the skip_vector.
    bit_pointer::FillFrom(left_skip_vector(), skip_vector, row_count);
    // Step 2) We need to pass a vector<bool_ptr> to the child, so we create it,
    // and evaluate the child.
    local_skip_vectors_.ResetColumn(0, left_skip_vector());
    EvaluationResult left_result =
        left()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(left_result);

    // It is convenient to work with bool_ptrs, so we copy the data from the
    // left result column to a bool_ptr copy. Note that we copy data also from
    // the fields which are NULL, and so can be uninitialized, thus we need to
    // use a safe copy.
    const bool* left = left_result.get().column(0).typed_data<BOOL>();
    bit_pointer::SafeFillFrom(left_result_copy(), left, row_count);

    // Step 3). The logic for creating the right skip vector is
    // operator-specific, encapsulated in a helper template function.
    MakeRightSkipVector<op>(left_result_copy(), left_skip_vector(), skip_vector,
                            right_skip_vector(), row_count, temp_skip_vector());
    // Step 4). Again we prepare a vector<bool_ptr>.
    local_skip_vectors_.ResetColumn(0, right_skip_vector());
    EvaluationResult right_result =
        right()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(right_result);

    const bool* right = right_result.get().column(0).typed_data<BOOL>();
    bit_pointer::SafeFillFrom(right_result_copy(), right, row_count);

    // Step 5). The logic for creating the output skip vector is also
    // operator-specific.
    MakeResultSkipVector<op>(left_result_copy(), left_skip_vector(),
                             right_result_copy(), right_skip_vector(),
                             skip_vector, row_count, temp_skip_vector());
    // Step 6). Evaluate result column.
    bool* result =
        my_block()->mutable_column(0)->template mutable_typed_data<BOOL>();
    // We aren't making a call to vector_logic here, as we want to have a
    // templated application. This is SIMD-ed and cheap, so we ignore the
    // skip vector locally.
    VectorBinaryPrimitive<op, DirectIndexResolver, DirectIndexResolver,
        BOOL, BOOL, BOOL, false> vector_primitive;
    bool res = vector_primitive(left_result_copy(), right_result_copy(), NULL,
                                NULL, input.row_count(), result, NULL);
    CHECK(res) << "Error on boolean vector primitive calculation.";
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    return Success(*my_view());
  }

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    return Success();
  }

 private:
  inline bool_ptr left_skip_vector() {
    return local_skip_vector_storage_.view().column(0);
  }
  inline bool_ptr right_skip_vector() {
    return local_skip_vector_storage_.view().column(1);
  }
  // A spare temporary space that will be needed for calculations.
  inline bool_ptr temp_skip_vector() {
    return local_skip_vector_storage_.view().column(2);
  }
  // At the moment we cannot guarantee that bool* and bool_ptr describe the
  // same type. To avoid mutliple copying of data from bool* to bool_ptr, we
  // are going to store copies of the results in bool_arrays, copying each once.
  // TODO(onufry): if we decide to drop the bit_ptr path and use bool_ptr to be
  // a bool*, we can remove these arrays.
  inline bool_ptr left_result_copy() {
    return local_skip_vector_storage_.view().column(3);
  }
  inline bool_ptr right_result_copy() {
    return local_skip_vector_storage_.view().column(4);
  }
  BoolBlock local_skip_vector_storage_;
  BoolView local_skip_vectors_;
};

template <DataType test_type, DataType output_type>
class BoundCaseExpression : public BasicBoundExpression {
 public:
  explicit BoundCaseExpression(BoundExpressionList* arguments,
                               BufferAllocator* allocator)
      : BasicBoundExpression(
          TupleSchema::Singleton(
              StrCat("CASE(", arguments->ToString(false), ")"),
              output_type,
              DetermineNullability(arguments)),
              allocator),
        data_pointers_(arguments->size() / 2),
        arguments_(arguments),
        local_skip_vector_storage_(5, allocator),
        local_skip_vectors_(1) {}

  // DoEvaluate does the following:
  // 1) Allocate a integer column, which will inform us from which of the THEN
  // expressions to copy the data. Prefill it with 0s, meaning "from the
  // OTHERWISE expression".
  // 2) Make a copy of the incoming skip vector, because the case expression
  // does not follow viral NULL semantics - one child being NULL does not have
  // to cause the output to be NULL, so we do not want to let the children to
  // modify the skip vector.
  // 3) Evaluate the expression upon which we switch (the CASE expression) with
  // the copied skip vector. Put the resultant skip vector somewhere safe.
  // 4) Also prepare a bool vector "already written" - marking true for the rows
  // for which we already know where is the input coming from. Fill it with true
  // for the rows for which: a) the parent skip vector is set (we don't care for
  // these), b) the CASE expression evaluated to NULL (we know we want the
  // OTHERWISE expression in such a case).
  // 5) For each WHEN/THEN pair:
  // 5a) prepare a skip vector copy for the WHEN clause, equal to the current
  // "already written" state - we skip the rows for which we already know what
  // input to take
  // 5b) evaluate the WHEN expression with the skip vector copy
  // 5c) check which rows are to be copied from the THEN expression by comparing
  // the results of the WHEN with the CASE.
  // 5d) prepare a skip vector for the THEN expression - we evaluate only the
  // rows for which the when expression was evaluated, and returned a result
  // equal to the switch expression. Also mark these rows in the integer column
  // informing us about the data source.
  // 5e) update the "already written" vector.
  // 5f) evaluate the THEN expression.
  // 5g) add the NULLs in the THEN column which correspond to fields which were
  // really evaluated to the parent skip vector.
  // 6) Prepare a copy of the skip vector for the OTHERWISE expression. This is
  // somewhat subtle, as we need to evaluate for two groups of rows: one for
  // which "already written" is not set, and the second being the rows for which
  // "already written" was set because the CASE expression evaluated to NULL.
  // 7) Evaluate the OTHERWISE expression with the skip vector.
  // 8) Update the NULLs of the parent skip vector.
  // 9) Copy the data (from all the results) to our result block, according to
  // the data source column we created (note that, again, it is irrelevant what
  // we copy in the case of the fields where the skip vector is set).
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    // Typically we would run ResetArenas here, but Case is guaranteed not to
    // create any new strings (just point to strings in child arenas).
    bool_ptr skip_vector = skip_vectors.column(0);
    const size_t row_count = input.row_count();

    // Step 1: Prepare the source vector. Initially - copy everything from the
    // OTHERWISE column.
    memset(source(), 0, row_count * sizeof(*source()));

    // Step 2: Copy the skip vector.
    bit_pointer::FillFrom(case_skip(), skip_vector, row_count);
    // Step 3: Evaluate the CASE expression. We need to pass a BoolView
    // containing a single skip vector to the child, we have this BoolView
    // prepared and preallocated in the binding phase.
    local_skip_vectors_.ResetColumn(0, case_skip());
    EvaluationResult case_result =
        case_expression()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(case_result);

    // Step 4: set the expressions we don't need to evaluate in the THEN
    // clauses.
    bit_pointer::FillFrom(written(), case_skip(), row_count);
    // Currently case_skip() contains the NULLs produced by the CASE expression
    // and the rows skipped because the parent told us to skip. We'd like to
    // remember only the first set (as we will have to evaluate the ELSE
    // expression for them). (this is the "put it somewhere safe" part of step
    // 3).
    vector_logic::AndNot(skip_vector, case_skip(), row_count, case_skip());

    // Step 5: Iterate through all WHEN/THEN pairs.
    for (size_t when_index = 0;
         when_index < num_when_expressions();
         ++when_index) {
      // Step 5a: reate skip vector for the WHEN clause.
      bit_pointer::FillFrom(when_skip(), written(), row_count);
      // Step 5b: Evaluate WHEN column (only on the rows not written yet).
      local_skip_vectors_.ResetColumn(0, when_skip());
      EvaluationResult when_result =
          when_expression(when_index)->DoEvaluate(input, local_skip_vectors_);
      PROPAGATE_ON_FAILURE(when_result);

      // Convenience shortcuts.
      const CppTestType* when =
          when_result.get().column(0).typed_data<test_type> ();
      const CppTestType* test =
          case_result.get().column(0).typed_data<test_type>();
      bool_const_ptr when_skip_iterator = when_skip();
      bool_ptr then_skip_iterator = then_skip();
      // Step 5c and 5d: We initially mark that no fields are to be copied from
      // THEN. When the WHEN was evaluated and equal to CASE, we mark the row to
      // be evaluated on the then_skip_vector.
      bit_pointer::FillWithTrue(then_skip_iterator, row_count);
      operators::Equal equal;
      for (size_t row_id = 0; row_id < row_count; ++row_id) {
        if (!*when_skip_iterator && equal(*test, *when)) {
          // We will copy the data from this then vector.
          source()[row_id] = when_index + 1;
          *then_skip_iterator = false;
        }
        ++then_skip_iterator;
        ++when_skip_iterator;
        ++when;
        ++test;
      }
      // Step 5e: Mark the rows to be evaluated as already written.
      vector_logic::Not(then_skip(), row_count, temp());
      vector_logic::Or(temp(), written(), row_count, written());

      // Step 5f: Evaluate the THEN column.
      local_skip_vectors_.ResetColumn(0, then_skip());
      EvaluationResult then_result =
          then_expression(when_index)->DoEvaluate(input, local_skip_vectors_);
      PROPAGATE_ON_FAILURE(then_result);
      data_pointers_[when_index+1] = then_result.get().column(0)
          .typed_data<output_type>();

      // Step 5g: NULL propagation.
      // The nulls in the THEN column need to be propagated to the output
      // vector. The temp() vector currently contains a true if the
      // corresponding row was evaluated in the then expression, so an AND of
      // the temp() vector and the current then_skip() will give exactly the
      // nulls produced by the then expression.
      vector_logic::And(temp(), then_skip(), row_count, temp());
      vector_logic::Or(temp(), skip_vector, row_count, skip_vector);
    }
    // Now we have to evaluate the ELSE column. We want to skip the already
    // written rows, except for the ones which we marked as such due to NULLs in
    // the CASE clause - thus, we need to AndNot those.
    // Step 6: OTHERWISE skip vector preparation.
    vector_logic::AndNot(case_skip(), written(), row_count, when_skip());
    // We'll need a copy of this vector to recreate the NULLs produced by else
    // (and copy them onto the output).
    bit_pointer::FillFrom(temp(), when_skip(), row_count);
    // Step 7: OTHERWISE evaluation.
    local_skip_vectors_.ResetColumn(0, when_skip());
    EvaluationResult else_result =
        else_expression()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(else_result);
    data_pointers_[0] = else_result.get().column(0).typed_data<output_type>();

    // Step 8: We need to copy the NULLs produced by the ELSE. We identify them
    // as those which were false when passed into the child, but true coming
    // back.
    vector_logic::AndNot(temp(), when_skip(), row_count, temp());
    vector_logic::Or(temp(), skip_vector, row_count, skip_vector);

    // Step 9: Copy the data into the result column. This is a shallow copy, and
    // data copying is cheap, so we are copying all data, needed or not.
    CppOutputType* result =
        my_block()->mutable_column(0)->
            template mutable_typed_data<output_type>();
    size_t* source_iterator = source();
    for (size_t row = 0; row < row_count; ++row) {
      *result = data_pointers_[*source_iterator][row];
      ++result;
      ++source_iterator;
    }

    my_view()->set_row_count(row_count);
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    return Success(*my_view());
  }

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

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    arguments_->CollectReferredAttributeNames(referred_attribute_names);
  }

 private:
  typedef typename TypeTraits<test_type>::cpp_type CppTestType;
  typedef typename TypeTraits<output_type>::cpp_type CppOutputType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    source_.reset(my_block()->allocator()->Allocate(
        my_block()->row_capacity() * sizeof(size_t)));                 // NOLINT
    if (source_ == NULL) THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        StrCat("Failed to allocate space for CASE expression buffer in ",
               "expression: ",
               result_schema().GetHumanReadableSpecification())));
    return Success();
  }

  size_t num_when_expressions() const { return (arguments_->size() - 2) / 2; }

  BoundExpression* case_expression() const { return arguments_->get(0); }
  BoundExpression* else_expression() const { return arguments_->get(1); }
  BoundExpression* when_expression(size_t index) const {
    return arguments_->get(2 + index * 2);
  }
  BoundExpression* then_expression(size_t index) const {
    return arguments_->get(2 + index * 2 + 1);
  }

  // Returns NULLABLE if result of CASE expression can be null, i.e. if ELSE or
  // any THEN column is nullable.
  static Nullability DetermineNullability(BoundExpressionList* args) {
    for (int i = 1; i < args->size(); i += 2) {
      if (args->get(i)->result_schema().attribute(0).is_nullable()) {
        return NULLABLE;
      }
    }
    return NOT_NULLABLE;
  }

  FixedArray<const CppOutputType*> data_pointers_;

  scoped_ptr<BoundExpressionList> arguments_;

  // Marks the rows which we do not have to caluclate any more.
  bool_ptr written() { return local_skip_vector_storage_.view().column(0); }
  // The skip-vector for the case (the expression we switch upon).
  bool_ptr case_skip() { return local_skip_vector_storage_.view().column(1); }
  // The skip-vector for the when clauses.
  bool_ptr when_skip() { return local_skip_vector_storage_.view().column(2); }
  // The skip-vector for the then clauses.
  bool_ptr then_skip() { return local_skip_vector_storage_.view().column(3); }
  bool_ptr temp() { return local_skip_vector_storage_.view().column(4); }
  // From which input should the output be copied? A buffer of size_t elements.
  scoped_ptr<Buffer> source_;
  inline size_t* source() { return reinterpret_cast<size_t*>(source_->data()); }
  BoolBlock local_skip_vector_storage_;
  // Local storage for passing the skip_vectors to the children.
  BoolView local_skip_vectors_;
  // Local storage for the pointers to the data of the children.
  DISALLOW_COPY_AND_ASSIGN(BoundCaseExpression);
};

// Functor used in the TypeSpecialization setup (see types_infrastructure.h).
template<DataType test_type>
struct CaseExpressionResolver2ndType {
  CaseExpressionResolver2ndType(BufferAllocator* allocator,
                                BoundExpressionList* arguments)
      : allocator(allocator),
        arguments(arguments) {}

  template<DataType type>
  BasicBoundExpression* operator()() const {
    return new BoundCaseExpression<test_type, type>(arguments, allocator);
  }

  BufferAllocator* allocator;
  BoundExpressionList* arguments;
};

// Functor used in the TypeSpecialization setup (see types_infrastructure.h).
struct CaseExpressionResolver {
  CaseExpressionResolver(BufferAllocator* allocator,
                         BoundExpressionList* arguments)
      : allocator(allocator),
        arguments(arguments) {}

  template<DataType type>
  BasicBoundExpression* operator()() const {
    CaseExpressionResolver2ndType<type> resolver(allocator, arguments);
    return TypeSpecialization<
        BasicBoundExpression*,
        CaseExpressionResolver2ndType<type> >(
            arguments->get(1)->result_schema().attribute(0).type(),
            resolver);
  }

  BufferAllocator* allocator;
  BoundExpressionList* arguments;
};

// A "nulling" if will return NULL if the condition is null. A "not nulling" if
// will return "otherwise" in such a case.
template<DataType output_type, bool is_nulling>
class BoundIfExpression : public BoundTernaryExpression {
 public:
  BoundIfExpression(BoundExpression* condition,
                    BoundExpression* then,
                    BoundExpression* otherwise,
                    BufferAllocator* allocator)
      : BoundTernaryExpression(CreateIfSchema(condition, then, otherwise),
                               allocator, condition, BOOL, then,
                               GetExpressionType(then), otherwise,
                               GetExpressionType(otherwise)),
        local_skip_vector_storage_(is_nulling ? 3 : 4, allocator),
        local_skip_vectors_(1) {}

  virtual ~BoundIfExpression() {}

  // DoEvaluate does the following:
  // 1) If the IF is not nulling, copy the skip vector for the CONDITION
  // expression (as the not-nulling if does not behave virally on NULLs - a NULL
  // on the input does not necessarily produce a NULL on the output, so we do
  // not want the child to modify the parent skip vector). In the case of the
  // NULLING if the CONDITION expression has viral NULLs, so we do not make a
  // copy.
  // 2) Evaluate the CONDITION.
  // 3) Prepare a "choice" vector, which informs us whether to evaluate the THEN
  // or the OTHERWISE expression (evaluate THEN if true, OTHERWISE if false).
  // Note that it is irrelevant what we set the choice vector to be if the
  // result is to be NULL.
  // 4) Prepare a skip vector for THEN (based on the choice vector and the
  // parent skip vector).
  // 5) Evaluate THEN.
  // 6) Prepare a skip vector for OTHERWISE (based on the same things).
  // 7) Evaluate OTHERWISE.
  // 8) Propagate NULLs from the children to the parents. If choice was set,
  // propagate NULLs from THEN, if not - the NULLs from OTHERWISE.
  // 9) Copy the data from the children, with the same rules.
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    // TODO(onufry): At the moment every single vector-logic expression we run
    // costs around 4% of the total evaluation cost. I'm pretty sure one or two
    // of these could be shaved off with some careful tweaking.
    CHECK_EQ(1, skip_vectors.column_count());
    // Typically we would run ResetArenas() here, but If is guaranteed not to
    // create any strings in the arena (all the stringpieces will point to
    // strings created by children).
    bool_ptr skip_vector = skip_vectors.column(0);
    size_t row_count = input.row_count();

    // Step 1)
    // We evaluate the condition on every input row, except for those that we
    // skip in the whole calculation due to skip_vector being set. In the
    // not-nulling case we do not propagate NULLs from the condition upwards
    // (we evaluate otherwise for them), so we have to operate on a copy of the
    // skip_vector. In the nulling case we simply pass the skip_vector along.
    if (is_nulling) {
      local_skip_vectors_.ResetColumn(0, skip_vector);
    } else {
      bit_pointer::FillFrom(condition_skip(), skip_vector, row_count);
      local_skip_vectors_.ResetColumn(0, condition_skip());
    }
    // Step 2: Evaluate CONDITION.
    EvaluationResult condition_result =
        condition()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(condition_result);
    const bool* condition_value =
        condition_result.get().column(0).typed_data<BOOL>();

    // Step 3: Prepare choice vector.
    // Choice is the vector which tells us which branch to go into. If choice
    // is true, we evaluate the left branch (then). If choice is false, we
    // either do not evaluate at all (if skip_vector is set), or evaluate
    // the right branch.
    // If we are not nulling, we evaluate the left branch only when the
    // condition is true and not null. If we are nulling, we can also use the
    // same choice vector, because it will make further code more similar.
    // This is an expensive operation, but it has to be done - it may have
    // happened there are NULLs in the output, and in that case there may be
    // incorrect boolean data within condition_value, which would spoil all our
    // SIMD operations further on.
    if (condition_result.get().column(0).is_null() != NULL) {
      bit_pointer::SafeFillFrom(choice(), condition_value, row_count);
    } else {
      // In this case we have the guarantee of all values being correct, so we
      // use the quicker FillFrom function.
      bit_pointer::FillFrom(choice(), condition_value, row_count);
    }
    if (!is_nulling) {
      vector_logic::AndNot(condition_skip(), choice(), row_count, choice());
    } else {
      // Except here condition_skip is not used, we use skip_vector instead.
      vector_logic::AndNot(skip_vector, choice(), row_count, choice());
    }

    // Step 4: Prepare skip vector for THEN.
    vector_logic::Not(choice(), row_count, then_skip());
    // Step 5: Evaluate THEN.
    local_skip_vectors_.ResetColumn(0, then_skip());
    EvaluationResult then_result =
        then()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(then_result);
    const CppType* then_value =
        then_result.get().column(0).typed_data<output_type>();

    // Step 6: Evaluate skip vector for OTHERWISE. We evaluate it if and only if
    // choice is set to false and the skip_vector is set to false.
    vector_logic::Or(choice(), skip_vector, row_count, otherwise_skip());
    // Step 7: Evaluate OTHERWISE.
    local_skip_vectors_.ResetColumn(0, otherwise_skip());
    EvaluationResult otherwise_result =
        otherwise()->DoEvaluate(input, local_skip_vectors_);
    PROPAGATE_ON_FAILURE(otherwise_result);
    const CppType* otherwise_value =
        otherwise_result.get().column(0).typed_data<output_type>();

    // Step 8: Propagate NULLs upwards.
    // Now we fill in the parent skip vector. We propagate skips from then_skip
    // when choice is true, and from otherwise_skip when choice is false (note
    // that in this way we will reconstruct the data from the original skip
    // vector, which was propagated into the appropriate branch (actually, it
    // was propagated into both branches).
    vector_logic::And(choice(), then_skip(), row_count, skip_vector);
    // We need a temporary memory space. We won't use then_skip() any more, so
    // let's overwrite it.
    vector_logic::AndNot(choice(), otherwise_skip(), row_count, then_skip());
    vector_logic::Or(skip_vector, then_skip(), row_count, skip_vector);

    // Step 9: Copy the data.
    // Now we write the appropriate data to the result block.
    // TODO(onufry): there are three ways to do it:
    // - write depending on choice, either copying the left or the right data.
    // - either write left, or write right, or do not copy anything (if we skip)
    // - memcpy one set of data, and then copy the other where needed.
    // I would guess the first choice is the fastest, and is implemented below,
    // but probably we could benchmark it sometime.
    CppType* result_value = my_block()->
        mutable_column(0)->template mutable_typed_data<output_type>();
    {
      bool_ptr it = choice();
      for (size_t i = 0; i < row_count; ++i, ++it) {
        result_value[i] = *it ? then_value[i] : otherwise_value[i];
      }
    }
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    my_view()->set_row_count(row_count);
    return Success(*my_view());
  }

 private:
  typedef typename TypeTraits<output_type>::cpp_type CppType;

  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    return Success();
  }

  string description(const BoundExpression* condition,
                     const BoundExpression* then,
                     const BoundExpression* otherwise) {
    return StrCat("IF ", GetExpressionName(condition), " THEN ",
                  GetExpressionName(then), " ELSE ",
                  GetExpressionName(otherwise));
  }

  TupleSchema CreateIfSchema(BoundExpression* condition,
                             BoundExpression* then,
                             BoundExpression* otherwise) {
    if (is_nulling) return CreateSchema(
        description(condition, then, otherwise),
        GetExpressionType(then),
        condition, then, otherwise);
    // else.
    Nullability nullability =
        (GetExpressionNullability(then) == NULLABLE ||
         GetExpressionNullability(otherwise) == NULLABLE) ? NULLABLE :
        NOT_NULLABLE;
    return TupleSchema::Singleton(description(condition, then, otherwise),
                                  GetExpressionType(then),
                                  nullability);
  }

  inline BoundExpression* condition() { return left_.get(); }
  inline BoundExpression* then() { return middle_.get(); }
  inline BoundExpression* otherwise() {return right_.get(); }

  // The skip vectors that will be passed to appropriate children.
  inline bool_ptr condition_skip() {
    DCHECK(!is_nulling) << "The condition_skip vector should not be used for "
                        << "the nulling if, we use skip_vector instead.";
    return local_skip_vector_storage_.view().column(3);
  }
  bool_ptr then_skip() { return local_skip_vector_storage_.view().column(0); }
  bool_ptr otherwise_skip() {
    return local_skip_vector_storage_.view().column(1);
  }
  // The choice vector, which will dictate whether to use the then or otherwise
  // value as the result.
  bool_ptr choice() { return local_skip_vector_storage_.view().column(2); }

  BoolBlock local_skip_vector_storage_;
  BoolView local_skip_vectors_;
};

// A functor for creating If expressions, used in the TypeSpecialization setup
// (see types_infrastructure.h).
template<bool is_nulling>
struct BoundIfFactory {
  BoundIfFactory(BoundExpression* condition,
                 BoundExpression* then,
                 BoundExpression* otherwise,
                 BufferAllocator* allocator,
                 rowcount_t row_capacity)
      : condition_(condition),
        then_(then),
        otherwise_(otherwise),
        allocator_(allocator),
        row_capacity_(row_capacity) {}

  template <DataType data_type>
  FailureOrOwned<BoundExpression> operator()() const {
    FailureOrOwned<BoundExpression> result = InitBasicExpression(
        row_capacity_,
        new BoundIfExpression<data_type, is_nulling>(
            condition_, then_, otherwise_, allocator_),
        allocator_);
    PROPAGATE_ON_FAILURE(result);
    return Success(result.release());
  }

  BoundExpression* condition_;
  BoundExpression* then_;
  BoundExpression* otherwise_;
  BufferAllocator* allocator_;
  rowcount_t row_capacity_;
};

// Create a Bound If expression.
template<bool is_nulling>
FailureOrOwned<BoundExpression> BoundIfInternal(
    BoundExpression* condition,
    BoundExpression* then,
    BoundExpression* otherwise,
    BufferAllocator* const allocator,
    rowcount_t row_capacity) {
  scoped_ptr<BoundExpression> condition_ptr(condition);
  scoped_ptr<BoundExpression> then_ptr(then);
  scoped_ptr<BoundExpression> otherwise_ptr(otherwise);

  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "IF", condition->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount("IF", then->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "IF", otherwise->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(BOOL, condition));
  // Type reconciliation for the inputs.
  FailureOr<DataType> output_type =
      CalculateCommonExpressionType(then, otherwise);
  PROPAGATE_ON_FAILURE(output_type);
  FailureOrOwned<BoundExpression> resolved_then = BoundInternalCast(
      allocator, row_capacity, then_ptr.release(), output_type.get(), true);
  PROPAGATE_ON_FAILURE(resolved_then);
  FailureOrOwned<BoundExpression> resolved_otherwise = BoundInternalCast(
      allocator, row_capacity, otherwise_ptr.release(), output_type.get(),
      true);
  PROPAGATE_ON_FAILURE(resolved_otherwise);
  // TypeSpecialization application.
  BoundIfFactory<is_nulling> factory(
      condition_ptr.release(), resolved_then.release(),
      resolved_otherwise.release(), allocator, row_capacity);
  FailureOrOwned<BoundExpression> result = TypeSpecialization<
      FailureOrOwned<BoundExpression>, BoundIfFactory<is_nulling> >(
          output_type.get(), factory);
  PROPAGATE_ON_FAILURE(result);
  return Success(result.release());
}

template <OperatorId op>
FailureOrOwned<BoundExpression> BoundBooleanBinary(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* const allocator,
    rowcount_t row_capacity) {
  PROPAGATE_ON_FAILURE(CheckAttributeCount(BinaryExpressionTraits<op>::name(),
                                           left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(BinaryExpressionTraits<op>::name(),
                                           right->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(BOOL, left));
  PROPAGATE_ON_FAILURE(CheckExpressionType(BOOL, right));

  string description = BinaryExpressionTraits<op>::FormatBoundDescription(
      left->result_schema().attribute(0).name(), BOOL,
      right->result_schema().attribute(0).name(), BOOL, BOOL);
  bool output_is_nullable =
      left->result_schema().attribute(0).is_nullable() ||
      right->result_schema().attribute(0).is_nullable();
  TupleSchema result_schema =
      CreateSchema(description, BOOL, output_is_nullable);

  BoundBooleanBinaryExpression<op>* result =
      new BoundBooleanBinaryExpression<op>(result_schema, allocator,
                                           left, right);
  return InitBasicExpression(row_capacity, result, allocator);
}

// Generic.
template<OperatorId op, DataType type>
struct BoundStringParserResolverFn {
  FailureOrOwned<BoundExpression> operator()(BoundExpression* child,
                                             BufferAllocator* allocator,
                                             rowcount_t row_capacity) const {
    SpecializedUnaryFactory<op, STRING, type> factory;
    return factory.create_expression(allocator, row_capacity, child);
  }
};

// Specialization for STRING, since parsing isn't defined for it.
template<OperatorId op>
struct BoundStringParserResolverFn<op, STRING> {
  FailureOrOwned<BoundExpression> operator()(BoundExpression* child,
                                             BufferAllocator* allocator,
                                             rowcount_t row_capacity) const {
    return CheckTypeAndPassAlong(child, STRING);
  }
};

// Specialization for BINARY, since parsing isn't defined for it.
template<OperatorId op>
struct BoundStringParserResolverFn<op, BINARY> {
  FailureOrOwned<BoundExpression> operator()(BoundExpression* child,
                                             BufferAllocator* allocator,
                                             rowcount_t row_capacity) const {
    return CreateTypedBoundUnaryExpression<OPERATOR_COPY, STRING, BINARY>(
        allocator, row_capacity, child);
  }
};

template<OperatorId op>
struct BoundStringParserResolver {
  BoundStringParserResolver(BoundExpression* child,
                            BufferAllocator* allocator,
                            rowcount_t row_capacity)
      : child(child),
        allocator(allocator),
        row_capacity(row_capacity) {}
  template<DataType type> FailureOrOwned<BoundExpression> operator()() const {
    BoundStringParserResolverFn<op, type> resolver;
    return resolver(child, allocator, row_capacity);
  }
  BoundExpression* child;
  BufferAllocator* allocator;
  rowcount_t row_capacity;
};

}  // namespace

// ------------------------ Control expressions --------------------

FailureOrOwned<BoundExpression> BoundCastTo(DataType to_type,
                                            BoundExpression* source,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  return BoundInternalCast(allocator, max_row_count, source, to_type, false);
}

FailureOrOwned<BoundExpression> BoundParseStringQuiet(
    DataType to_type,
    BoundExpression* child,
    BufferAllocator* allocator,
    rowcount_t row_capacity) {
  BoundStringParserResolver<OPERATOR_PARSE_STRING_QUIET> resolver(
      child, allocator, row_capacity);
  return TypeSpecialization<
      FailureOrOwned<BoundExpression>,
      BoundStringParserResolver<OPERATOR_PARSE_STRING_QUIET> >(
          to_type, resolver);
}

FailureOrOwned<BoundExpression> BoundParseStringNulling(
    DataType to_type,
    BoundExpression* child,
    BufferAllocator* allocator,
    rowcount_t row_capacity) {
  BoundStringParserResolver<OPERATOR_PARSE_STRING_NULLING> resolver(
      child, allocator, row_capacity);
  return TypeSpecialization<
      FailureOrOwned<BoundExpression>,
      BoundStringParserResolver<OPERATOR_PARSE_STRING_NULLING> >(
          to_type, resolver);
}

// This is a part of the TypeSpecialization pattern.
struct BoundIfNullFactory {
  BoundIfNullFactory(BufferAllocator* allocator,
                     BoundExpression* expression,
                     BoundExpression* substitute)
      : allocator_(allocator),
        expression_(expression),
        substitute_(substitute) {}

  template<DataType type>
  BasicBoundExpression* operator()() const {
    return new BoundIfNullExpression<type>(allocator_, expression_,
                                           substitute_);
  }

  BufferAllocator* allocator_;
  BoundExpression* expression_;
  BoundExpression* substitute_;
};

FailureOrOwned<BoundExpression> BoundIfNull(BoundExpression* expression_ptr,
                                            BoundExpression* substitute_ptr,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> expression(expression_ptr);
  scoped_ptr<BoundExpression> substitute(substitute_ptr);

  // We have to convert both inputs to a common type.
  FailureOr<DataType> common_type =
      CalculateCommonExpressionType(expression.get(), substitute.get());
  PROPAGATE_ON_FAILURE(common_type);

  FailureOrOwned<BoundExpression> cast_expression =
      BoundInternalCast(allocator, max_row_count, expression.release(),
                        common_type.get(), true);
  PROPAGATE_ON_FAILURE(cast_expression);

  FailureOrOwned<BoundExpression> cast_substitute =
      BoundInternalCast(allocator, max_row_count, substitute.release(),
                        common_type.get(), true);
  PROPAGATE_ON_FAILURE(cast_substitute);

  // If expression is not nullable, we don't need substitute. Note that we
  // perform the above checks earlier, as we guess the user wants to be informed
  // about a type-matching failure if it occurs, even if the left-hand-side
  // happens to be not-nullable.
  if (!cast_expression->result_schema().attribute(0).is_nullable()) {
    return Success(cast_expression.release());
  }

  // Now we specialize depending on the type: create, initialize and return a
  // BoundIfNull expression.
  BoundIfNullFactory factory(allocator, cast_expression.release(),
                             cast_substitute.release());
  return InitBasicExpression(
      max_row_count,
      TypeSpecialization<BasicBoundExpression*, BoundIfNullFactory>(
          common_type.get(), factory),
      allocator);
}

FailureOrOwned<BoundExpression> BoundCase(BoundExpressionList* bound_arguments,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  scoped_ptr<BoundExpressionList> args_ptr(bound_arguments);

  if (args_ptr->size() < 2) {
    THROW(new Exception(
        supersonic::ERROR_INVALID_ARGUMENT_VALUE,
        "Case expects at least 2 arguments (make sense from 4 arguments)."));
  }
  if (args_ptr->size() % 2 != 0) {
    THROW(new Exception(
        supersonic::ERROR_INVALID_ARGUMENT_VALUE,
        "Case expects odd number of arguments."));
  }

  DataType test_type = args_ptr->get(0)->result_schema().attribute(0).type();
  DataType output_type = args_ptr->get(1)->result_schema().attribute(0).type();
  for (size_t i = 2; i < args_ptr->size(); ++i) {
    const TupleSchema& schema = args_ptr->get(i)->result_schema();
    DataType* expected_type_ptr = (i % 2 == 0) ? &test_type : &output_type;
    DCHECK(expected_type_ptr != NULL);
    if (schema.attribute(0).type() != *expected_type_ptr) {
      if (!GetTypeInfo(schema.attribute(0).type()).is_numeric()
          || !GetTypeInfo(*expected_type_ptr).is_numeric()) {
        THROW(new Exception(
            ERROR_ATTRIBUTE_TYPE_MISMATCH,
            StringPrintf(
                "Bind failed: Case: Cannot cast attribute %zd (%s to %s)",
                i,
                GetTypeInfo(schema.attribute(0).type()).name().c_str(),
                GetTypeInfo(*expected_type_ptr).name().c_str())));
      } else {
        FailureOr<DataType> common_type = CalculateCommonType(
            schema.attribute(0).type(), *expected_type_ptr);
        PROPAGATE_ON_FAILURE(common_type);
        *expected_type_ptr = common_type.get();
      }
    }
  }

  // After we have calculated common test_type and output_type, do another pass
  // and create new BoundExpressionList.
  scoped_ptr<BoundExpressionList> new_list(new BoundExpressionList());
  for (size_t i = 0; i < args_ptr->size(); ++i) {
    DataType wanted_type = i % 2 == 0 ? test_type : output_type;
    FailureOrOwned<BoundExpression> cast_result =
        BoundInternalCast(allocator, max_row_count, args_ptr->release(i),
                          wanted_type, true);
    PROPAGATE_ON_FAILURE(cast_result);
    new_list->add(cast_result.release());
  }

  CaseExpressionResolver resolver(allocator, new_list.release());
  scoped_ptr<BasicBoundExpression> case_expression(
      TypeSpecialization<BasicBoundExpression*, CaseExpressionResolver>(
          test_type, resolver));
  return InitBasicExpression(max_row_count, case_expression.release(),
                             allocator);
}

FailureOrOwned<BoundExpression> BoundIf(BoundExpression* condition,
                                        BoundExpression* then,
                                        BoundExpression* otherwise,
                                        BufferAllocator* allocator,
                                        rowcount_t max_row_count) {
  return BoundIfInternal<false>(condition, then, otherwise, allocator,
                                max_row_count);
}

FailureOrOwned<BoundExpression> BoundIfNulling(BoundExpression* condition,
                                               BoundExpression* if_true,
                                               BoundExpression* if_false,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return BoundIfInternal<true>(condition, if_true, if_false, allocator,
                               max_row_count);
}

// --------------------------- Logic --------------------------------
FailureOrOwned<BoundExpression> BoundNot(BoundExpression* arg,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count) {
  return AbstractBoundUnary<OPERATOR_NOT, BOOL, BOOL>(arg, allocator,
                                                      max_row_count);
}

FailureOrOwned<BoundExpression> BoundOr(BoundExpression* left,
                                        BoundExpression* right,
                                        BufferAllocator* const allocator,
                                        rowcount_t row_capacity) {
  return BoundBooleanBinary<OPERATOR_OR>(left, right, allocator, row_capacity);
}

FailureOrOwned<BoundExpression> BoundAnd(BoundExpression* left,
                                         BoundExpression* right,
                                         BufferAllocator* const allocator,
                                         rowcount_t row_capacity) {
  return BoundBooleanBinary<OPERATOR_AND>(left, right, allocator, row_capacity);
}

FailureOrOwned<BoundExpression> BoundAndNot(BoundExpression* left,
                                            BoundExpression* right,
                                            BufferAllocator* const allocator,
                                            rowcount_t row_capacity) {
  return BoundBooleanBinary<OPERATOR_AND_NOT>(left, right,
                                              allocator, row_capacity);
}

FailureOrOwned<BoundExpression> BoundXor(BoundExpression* left,
                                         BoundExpression* right,
                                         BufferAllocator* const allocator,
                                         rowcount_t row_capacity) {
  return CreateTypedBoundBinaryExpression<OPERATOR_XOR, BOOL, BOOL, BOOL>(
      allocator, row_capacity, left, right);
}

// ----------------------- Unary comparisons and checks -------------------
FailureOrOwned<BoundExpression> BoundIsNull(BoundExpression* arg,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  PROPAGATE_ON_FAILURE(CheckAttributeCount("ISNULL", arg->result_schema(), 1));
  if (!arg->result_schema().attribute(0).is_nullable()) {
    scoped_ptr<BoundExpression> arg_deleter(arg);
    scoped_ptr<const Expression> const_bool(ConstBool(false));
    return const_bool->DoBind(arg->result_schema(),
                              allocator, max_row_count);
  } else {
    return InitBasicExpression(max_row_count,
                               new BoundIsNullExpression(allocator, arg),
                               allocator);
  }
}

// ----------------------- Bitwise operators ------------------------------

// The ExpressionFactory and RunFactory design is explained in
// expression/templated/bound_expression_factory.h.
template<OperatorId op>
UnaryExpressionFactory* CreateIntegerUnaryFactory(DataType type) {
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, INT32, INT32>();
    case INT64: return new SpecializedUnaryFactory<op, INT64, INT64>();
    case UINT32: return new SpecializedUnaryFactory<op, UINT32, UINT32>();
    case UINT64: return new SpecializedUnaryFactory<op, UINT64, UINT64>();
    default: return NULL;
  }
}

template<OperatorId op, DataType left, DataType result>
BinaryExpressionFactory* CreateInsertRightTypeFactory(DataType right) {
  switch (right) {
    case INT32: return new SpecializedBinaryFactory<op, left, INT32, result>();
    case UINT32:
                return new SpecializedBinaryFactory<op, left, UINT32, result>();
    case INT64: return new SpecializedBinaryFactory<op, left, INT64, result>();
    case UINT64:
                return new SpecializedBinaryFactory<op, left, UINT64, result>();
    default: return NULL;
  }
}

template<OperatorId op>
BinaryExpressionFactory* CreateIntegerInheritLeftBinaryFactory(DataType left,
                                                               DataType right) {
  switch (left) {
    case INT32: return CreateInsertRightTypeFactory<op, INT32, INT32>(right);
    case INT64: return CreateInsertRightTypeFactory<op, INT64, INT64>(right);
    case UINT32: return CreateInsertRightTypeFactory<op, UINT32, UINT32>(right);
    case UINT64: return CreateInsertRightTypeFactory<op, UINT64, UINT64>(right);
    default: return NULL;
  }
}

template<OperatorId op>
FailureOrOwned<BoundExpression> CreateShiftExpression(
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> left(left_ptr);
  scoped_ptr<BoundExpression> right(right_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      BinaryExpressionTraits<op>::name(), left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      BinaryExpressionTraits<op>::name(), right->result_schema(), 1));
  BinaryExpressionFactory* factory =
      CreateIntegerInheritLeftBinaryFactory<op>(GetExpressionType(left.get()),
                                                GetExpressionType(right.get()));
  return RunBinaryFactory(factory, allocator, max_row_count, left.release(),
                          right.release(), BinaryExpressionTraits<op>::name());
}

FailureOrOwned<BoundExpression> BoundBitwiseNot(BoundExpression* argument_ptr,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> argument(argument_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      UnaryExpressionTraits<OPERATOR_BITWISE_NOT>::name(),
      argument->result_schema(),
      1));
  UnaryExpressionFactory* factory =
      CreateIntegerUnaryFactory<OPERATOR_BITWISE_NOT>(
          GetExpressionType(argument.get()));
  return RunUnaryFactory(factory, allocator, max_row_count, argument.release(),
                         UnaryExpressionTraits<OPERATOR_BITWISE_NOT>::name());
}

FailureOrOwned<BoundExpression> BoundBitwiseAnd(BoundExpression* left,
                                                BoundExpression* right,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_BITWISE_AND>(
      allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundBitwiseAndNot(BoundExpression* left,
                                                   BoundExpression* right,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_BITWISE_ANDNOT>(
      allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundBitwiseOr(BoundExpression* left,
                                               BoundExpression* right,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_BITWISE_OR>(
      allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundBitwiseXor(BoundExpression* left,
                                                BoundExpression* right,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_BITWISE_XOR>(
      allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundShiftLeft(BoundExpression* argument,
                                               BoundExpression* shift,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count) {
  return CreateShiftExpression<OPERATOR_SHIFT_LEFT>(
      argument, shift, allocator, max_row_count);
}

FailureOrOwned<BoundExpression> BoundShiftRight(BoundExpression* argument,
                                                BoundExpression* shift,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count) {
  return CreateShiftExpression<OPERATOR_SHIFT_RIGHT>(
      argument, shift, allocator, max_row_count);
}

}  // namespace supersonic
