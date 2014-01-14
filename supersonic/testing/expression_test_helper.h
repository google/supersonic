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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)

#ifndef SUPERSONIC_TESTING_EXPRESSION_TEST_HELPER_H_
#define SUPERSONIC_TESTING_EXPRESSION_TEST_HELPER_H_

#include <vector>
using std::vector;

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/infrastructure/bound_expression_creators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "gtest/gtest.h"

namespace supersonic {

class BoundExpression;
class Expression;
class ExpressionList;
class TupleSchema;
class View;
class Block;
class BoundExpressionTree;

// Deletes the original Expression, returns the Bound version (or failure).
FailureOrOwned<BoundExpressionTree> StandardBind(const TupleSchema& schema,
                                                 rowcount_t max_row_count,
                                                 const Expression* expression);

// Assumes (and checks for) success, and deletes the original Expression.
BoundExpressionTree* DefaultBind(const TupleSchema& schema,
                                 rowcount_t max_row_count,
                                 const Expression* expression);

// Assumes (and checks for) success, and deletes the original Expression.
BoundExpression* DefaultDoBind(const TupleSchema& schema,
                               rowcount_t max_row_count,
                               const Expression* expression);

// Assumes (and checks for) success, and verifies the number of result rows.
const View& DefaultEvaluate(BoundExpressionTree* expression, const View& input);

// Creates ExpressionList from vector of expressions.
ExpressionList* MakeExpressionList(
    const vector<const Expression*>& expressions);

// DEPRECATED. Use TupleSchema::AreEqual instead.
void ExpectResultType(const TupleSchema& schema,
                      const DataType type,
                      const StringPiece& name,
                      bool is_nullable);

// -------------------------- Evaluation Tests ---------------------------

// Testers of expression evaluation. Each row of the input block contains:
//   - the test data (the first n columns for n-ary expressions)
//   - the expected output for this data (the last column).
// The second argument is the factory function of the expression being tested.
//
// Sample usage:
//
// TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
//    .AddRow( 0.25,    0.5)
//    .AddRow( 400.,    20.)
//    .AddRow(-1.,      __)
//    .Build(), &Sqrt);
// This code evaluates the Sqrt function against the first column, and expects
// the results to be equal to the values from the second column. The tests are
// performed for:
// - the original block
// - the block split into 1-row views
// - the original block's rows replicated to form a bigger block (16 and 1024
//   rows).
void TestEvaluation(const Block* block, ConstExpressionCreator factory);
void TestEvaluation(const Block* block, UnaryExpressionCreator factory);
void TestEvaluation(const Block* block, BinaryExpressionCreator factory);
void TestEvaluation(const Block* block, TernaryExpressionCreator factory);
void TestEvaluation(const Block* block, QuaternaryExpressionCreator factory);
void TestEvaluation(const Block* block, QuinaryExpressionCreator factory);
void TestEvaluation(const Block* block, SenaryExpressionCreator factory);

// Testers for evaluation of stateful expressions. Unlike TestEvaluation they
// don't replicate or reorder rows in the block. It still tests the expression
// over block boundaries, which can be very useful for stateful expressions.
void TestStatefulEvaluation(const Block* block, ConstExpressionCreator factory);
void TestStatefulEvaluation(const Block* block, UnaryExpressionCreator factory);
void TestStatefulEvaluation(const Block* block,
                            BinaryExpressionCreator factory);
void TestStatefulEvaluation(const Block* block,
                            TernaryExpressionCreator factory);
void TestStatefulEvaluation(const Block* block,
                            QuaternaryExpressionCreator factory);
void TestStatefulEvaluation(const Block* block,
                            QuinaryExpressionCreator factory);
void TestStatefulEvaluation(const Block* block,
                            SenaryExpressionCreator factory);

// A tester that takes an expression instead of an expression factory. This is
// expected to be used in other testing functions, not in tests themselves.
// TODO(user): transform Creators to functors (so that they can be nested),
// and then remove this function from the header file.
void TestEvaluationCommon(const Block* block, bool stateful_expression,
                          const Expression* expression_ptr);

// Testers for failure scenarios (we expect binding to succeed and evaluation
// to fail). The format is same as above, except that the block is one column
// shorter (we don't care about the output), and we expect the evaluation to
// fail on each single line of the input.
//
// Sample usage:
//
// TestEvaluationFalure(BlockBuilder<DOUBLE, DOUBLE, DOUBLE>()
//    .AddRow(1.0,   0.0)
//    .AddRow(0.0,   0.0)
//    .AddRow(-1.0,  0.0)
//    .Build(), &Divide);
// This code checks that the Divide operation indeed fails on all three of
// the given divisions (1/0, 0/0, -1/0).
void TestEvaluationFailure(const Block* block, ConstExpressionCreator factory);
void TestEvaluationFailure(const Block* block, UnaryExpressionCreator factory);
void TestEvaluationFailure(const Block* block, BinaryExpressionCreator factory);
void TestEvaluationFailure(const Block* block,
                           TernaryExpressionCreator factory);
void TestEvaluationFailure(const Block* block,
                           QuaternaryExpressionCreator factory);
void TestEvaluationFailure(const Block* block,
                           QuinaryExpressionCreator factory);
void TestEvaluationFailure(const Block* block, SenaryExpressionCreator factory);

// ------------------------- Binding Tests -------------------------------

// Testers of binding. We pass the input types of the columns (they will be
// created non-nullable), the schema name we expect (where the argument names
// are denoted by $0, $1, and so on), the expected output type and the
// expected nullability of the output column.
//
// Sample usage:
//
// TestUnaryBinding(&Sqrt, DOUBLE, "SQRT($0)", DOUBLE, true);
// TestUnaryBinding(
//     &Sqrt, INT32, "SQRT(CAST_INT32_TO_DOUBLE($0))", DOUBLE, true);
::testing::AssertionResult TestUnaryBindingFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    UnaryExpressionCreator factory,
    DataType in_type,
    const char* expected_name_schema,
    DataType out_type,
    bool out_nullable);

#define TestUnaryBinding(creator, in_type, e_template_name, ret_type, nullable)\
    EXPECT_PRED_FORMAT5(TestUnaryBindingFunction, creator,                     \
                        in_type, e_template_name, ret_type, nullable)

// The "Nullable" and "NotNullable" in the BinaryBinding testers refer to the
// expected nullability of the output.
::testing::AssertionResult TestBinaryBindingFunctionNullable(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    BinaryExpressionCreator factory,
    DataType left_type,
    DataType right_type,
    const char* expected_name_schema,
    DataType out_type);

#define TestBinaryBindingNullable(                                             \
    creator, left_t, right_t, e_template_name, ret_type)                       \
    EXPECT_PRED_FORMAT5(TestBinaryBindingFunctionNullable, creator,            \
                        left_t, right_t, e_template_name, ret_type)

// TODO(user): Add EXPECT_PRED_FORMAT(N) where N <6, 10> and revert this
// change passing null into argument list instead of passing in function name.
::testing::AssertionResult TestBinaryBindingFunctionNotNullable(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    BinaryExpressionCreator creator,
    DataType left_type,
    DataType right_type,
    const char* expected_name_template,
    DataType return_type);

#define TestBinaryBindingNotNullable(                                          \
    creator, left_t, right_t, e_template_name, ret_type)                       \
    EXPECT_PRED_FORMAT5(TestBinaryBindingFunctionNotNullable, creator,         \
                        left_t, right_t, e_template_name, ret_type)

// TODO(user): Expose a macro when EXPECT_PRED_FORMAT has more arguments.
void TestTernaryBinding(TernaryExpressionCreator factory,
                        DataType left_type,
                        DataType middle_type,
                        DataType right_type,
                        const char* expected_name_schema,
                        DataType out_type,
                        bool out_nullable);

void TestBindingWithNull(UnaryExpressionCreator factory,
                         DataType in_type,
                         bool in_nullable,
                         const char* expected_name_schema,
                         DataType out_type,
                         bool out_nullable);
void TestBindingWithNull(BinaryExpressionCreator factory,
                         DataType left_type,
                         bool left_nullable,
                         DataType right_type,
                         bool right_nullable,
                         const char* expected_name_schema,
                         DataType out_type,
                         bool out_nullable);
void TestBindingWithNull(TernaryExpressionCreator factory,
                         DataType left_type,
                         bool left_nullable,
                         DataType middle_type,
                         bool middle_nullable,
                         DataType right_type,
                         bool right_nullable,
                         const char* expected_name_schema,
                         DataType out_type,
                         bool out_nullable);

// Testers for failure during binding (we expect the binding to fail). The
// format is as above, except we do not need to pass any expectations on the
// output.
void TestBindingFailure(UnaryExpressionCreator factory,
                        DataType in_type);
void TestBindingFailure(BinaryExpressionCreator factory,
                        DataType left_type,
                        DataType right_type);
void TestBindingFailure(TernaryExpressionCreator factory,
                        DataType left_type,
                        DataType middle_type,
                        DataType right_type);

// Testers for failure during binding (we expect the binding to fail). The
// format is as above, except we do not need to pass any expectations on the
// output. We give the nullability of the inputs.
void TestUnaryBindingFailureWithNulls(UnaryExpressionCreator factory,
                                      DataType in_type,
                                      bool in_is_nullable);
void TestBinaryBindingFailureWithNulls(BinaryExpressionCreator factory,
                                       DataType left_type,
                                       bool left_is_nullable,
                                       DataType right_type,
                                       bool right_is_nullable);
void TestTernaryBindingFailureWithNulls(TernaryExpressionCreator factory,
                                        DataType left_type,
                                        bool left_is_nullable,
                                        DataType middle_type,
                                        bool middle_is_nullable,
                                        DataType right_type,
                                        bool right_is_nullable);

// Testers of bound factories. The assumption is that the behaviour of the
// expression is tested at the Expresion level, by ExpressionCreator
// EvaluationTesters, and EvaluationFailureTesters. This serves almost only
// to assure that the Bound* functions generate the appropriate bound
// expression, the behaviour of which we already believe to be tested.
//
// Sample usage:
//
// TestBinding(&BoundPlus, INT32, INT64, "CAST_INT32_TO_INT64($0) + $1");
//
// We do not test output types and nullability, as we only want to check
// whether the binding is performed to the correct expression.
//
// TODO(onufry): perhaps a way to distinguish expressions that do not differ
// in their schema output (like a "description" method for BoundExpression)
// could be used here. The problem occurs, for instance, for various failure
// policies for, e.g., division.

::testing::AssertionResult TestBoundFactoryFunction(
    const char* s1,
    const char* s2,
    BoundConstExpressionFactory factory,
    const char* expected_name_template);

#define TestBoundConstant(factory, name_template)                              \
    EXPECT_PRED_FORMAT2(TestBoundFactoryFunction,                              \
                        factory, name_template)

::testing::AssertionResult TestBoundUnaryFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    BoundUnaryExpressionFactory factory,
    DataType input_type,
    const char* expected_name_template);

#define TestBoundUnary(factory, input_type, name_template)                     \
    EXPECT_PRED_FORMAT3(TestBoundUnaryFunction,                                \
                        factory, input_type, name_template)

::testing::AssertionResult TestBoundBinaryFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    BoundBinaryExpressionFactory factory,
    DataType left_type,
    DataType right_type,
    const char* expected_name_template);

#define TestBoundBinary(factory, left_type, right_type, name_template)         \
    EXPECT_PRED_FORMAT4(TestBoundBinaryFunction,                               \
                        factory, left_type, right_type, name_template)


::testing::AssertionResult TestBoundTernaryFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    BoundTernaryExpressionFactory factory,
    DataType left_type,
    DataType middle_type,
    DataType right_type,
    const char* expected_name_template);

#define TestBoundTernary(factory, left_type, mid_name, right_type, name_temp)  \
    EXPECT_PRED_FORMAT5(TestBoundTernaryFunction,                              \
                        factory, left_type, mid_name, right_type, name_temp)


// TODO(user): Change those functions to return AssertionResult and make,
// a macro when EXPECT_PRED_FORMAT has enough parameters.
void TestBoundQuaternary(BoundQuaternaryExpressionFactory factory,
                         DataType left_type,
                         DataType middle_left_type,
                         DataType middle_right_type,
                         DataType right_type,
                         const char* expected_name_template);
void TestBoundExpressionList(BoundExpressionListExpressionFactory factory,
                             vector<DataType> data_types,
                             const char* expected_name_template);


// We may also check expression if it failt to bind.
// It make especially sense to check if there is no memory leak.
void TestBoundFactoryFailure(BoundUnaryExpressionFactory factory,
                             DataType input_type);
void TestBoundFactoryFailure(BoundBinaryExpressionFactory factory,
                             DataType left_type,
                             DataType right_type);
void TestBoundFactoryFailure(BoundTernaryExpressionFactory factory,
                             DataType left_type,
                             DataType middle_type,
                             DataType right_type);
void TestBoundFactoryFailure(BoundExpressionListExpressionFactory factory,
                             vector<DataType> data_types);


// There is also a version where we can directly specify the nullability of
// particular fields. This goes as follows:
//
// TestBoundFactoryWithNulls(&BoundIfNull, INT32, true, INT32, false,
//                           "IFNULL($0, $1)");
// TestBoundFactoryWithNulls(&BoundIfNull, INT32, false, INT32, true, "$0");
//
// (this checks that the IFNULL functions ignores the second argument if the
// first column is not nullable).

void TestBoundFactoryWithNulls(BoundUnaryExpressionFactory factory,
                               DataType input_type,
                               bool input_nullable,
                               const char* expected_name_schema);
void TestBoundFactoryWithNulls(BoundBinaryExpressionFactory factory,
                               DataType left_type,
                               bool left_nullable,
                               DataType right_type,
                               bool right_nullable,
                               const char* expected_name_schema);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_EXPRESSION_TEST_HELPER_H_
