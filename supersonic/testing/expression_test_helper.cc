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

#include "supersonic/testing/expression_test_helper.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/testing/comparable_tuple_schema.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/repeating_block.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/strings/numbers.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/strings/substitute.h"
#include "supersonic/utils/strings/util.h"
#include "gtest/gtest.h"

namespace supersonic {

// We have to give a block size at binding time, and it limits the number
// of rows that any test supports. Thus, if any test would use more than
// a hundred rows, this constant should be increased.
static const int kMaxNumberOfRowsInTest = 100;

FailureOrOwned<BoundExpressionTree> StandardBind(const TupleSchema& schema,
                                                 rowcount_t max_row_count,
                                                 const Expression* expression) {
  std::unique_ptr<const Expression> deleter(expression);
  return expression->Bind(schema, HeapBufferAllocator::Get(), max_row_count);
}

BoundExpressionTree* DefaultBind(const TupleSchema& schema,
                                 rowcount_t max_row_count,
                                 const Expression* expression) {
  return SucceedOrDie(StandardBind(schema, max_row_count, expression));
}

BoundExpression* DefaultDoBind(const TupleSchema& schema,
                               rowcount_t max_row_count,
                               const Expression* expression) {
  std::unique_ptr<const Expression> deleter(expression);
  return SucceedOrDie(
      expression->DoBind(schema, HeapBufferAllocator::Get(), max_row_count));
}

const View& DefaultEvaluate(BoundExpressionTree* expression,
                            const View& input) {
  return SucceedOrDie(expression->Evaluate(input));
}

ExpressionList* MakeExpressionList(
    const vector<const Expression*>& expressions) {
  std::unique_ptr<ExpressionList> list(new ExpressionList());
  for (int i = 0; i < expressions.size(); ++i) {
    list->add(expressions[i]);
  }
  return list.release();
}

void ExpectResultType(const TupleSchema& schema,
                      const DataType type,
                      const StringPiece& name,
                      bool is_nullable) {
  ASSERT_EQ(1, schema.attribute_count())
      << schema.GetHumanReadableSpecification();
  EXPECT_EQ(type, schema.attribute(0).type())
      << schema.GetHumanReadableSpecification();
  EXPECT_EQ(name, schema.attribute(0).name())
      << schema.GetHumanReadableSpecification();
  EXPECT_EQ(is_nullable, schema.attribute(0).is_nullable())
      << schema.GetHumanReadableSpecification();
}

// ------------------------ Evaluation Tests ---------------------------

const Expression* CreateTestedExpression(ConstExpressionCreator factory) {
  return (*factory)();
}
const Expression* CreateTestedExpression(UnaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0));
}
const Expression* CreateTestedExpression(BinaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0), AttributeAt(1));
}
const Expression* CreateTestedExpression(TernaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0), AttributeAt(1), AttributeAt(2));
}
const Expression* CreateTestedExpression(QuaternaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0), AttributeAt(1), AttributeAt(2),
                    AttributeAt(3));
}
const Expression* CreateTestedExpression(QuinaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0), AttributeAt(1),
                    AttributeAt(2), AttributeAt(3),
                    AttributeAt(4));
}
const Expression* CreateTestedExpression(SenaryExpressionCreator factory) {
  return (*factory)(AttributeAt(0), AttributeAt(1),
                    AttributeAt(2), AttributeAt(3),
                    AttributeAt(4), AttributeAt(5));
}

BufferAllocator* CreateAligningHeapBufferAllocator() {
  return new HeapBufferAllocator(true);
}

void TestEvaluationForAlignedResizedBlock(const Block& input_block,
                                          const Expression& expr,
                                          rowcount_t size) {
  std::unique_ptr<BufferAllocator> aligning_allocator(
      CreateAligningHeapBufferAllocator());
  std::unique_ptr<const Block> block(
      ReplicateBlock(input_block, size, aligning_allocator.get()));
  // Check if columns are nicely aligned.
  for (int i = 0; i < block->column_count(); ++i) {
    CHECK_EQ(0, reinterpret_cast<uint64>(block->column(i).data().raw()) % 16);
  }
  CHECK_EQ(size, block->row_capacity());
  const TupleSchema& schema = block->view().schema();
  FailureOrOwned<BoundExpressionTree> bound_expr =
      expr.Bind(schema, aligning_allocator.get(), size);
  ASSERT_TRUE(bound_expr.is_success()) << bound_expr.exception().message();
  FailureOrReference<const View> result_view_result =
      bound_expr->Evaluate(block->view());
  ASSERT_TRUE(result_view_result.is_success())
      << result_view_result.exception().message()
      << "\nsize = " << size << " "
      << (size == input_block.row_capacity()
          ? "(original)"
          : "(changed by TestEvaluationCommon)");
  const View& result_view = result_view_result.get();

  // Obtaining the expected output view from the block by a Projector.
  int last_attribute_position = schema.attribute_count() - 1;
  const Attribute& last_attribute(schema.attribute(last_attribute_position));
  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectAttributeAt(last_attribute_position));
  FailureOrOwned<const BoundSingleSourceProjector> bound_projector =
      projector->Bind(schema);
  ASSERT_TRUE(bound_projector.is_success())
      << bound_projector.exception().message();
  View expected_view(TupleSchema::Singleton("col0",
                                            last_attribute.type(),
                                            last_attribute.nullability()));
  bound_projector->Project(block->view(), &expected_view);
  expected_view.set_row_count(result_view.row_count());

  ASSERT_VIEWS_EQUAL(expected_view, result_view)
      << "size = " << size << " "
      << (size == input_block.row_capacity()
          ? "(original)"
          : "(changed by TestEvaluationCommon)");
}

// Tests whether the expression does not leave any memory allocated (for
// instance in Arenas) after an Evaluate call. We do this by:
// - binding the expression.
// - running the evaluation three times.
// - measuring the memory usage.
// - running the evaluation again seven times, and making sure the memory usage
// is the same after each run.
//
// The place where memory could get allocated and left behind would be the
// arenas. We need the first three runs to let the arena size stabilize. The
// behaviour of the arena is that when ResetArenas is called, the last block is
// left behind (while the others are deallocated), and when the block size is
// exceeded, the next block allocated is twice as large.
// Thus, the first run will see many allocations. The last block will be left
// behind, and we have the guarantee that it is large enough to hold half the
// data generated. Then in the second run, it either holds all the data (where
// the arena is stable and will not change over next calls), or it doesn't, in
// which a block twice as large is allocated - and this new block is sure to be
// large enough to hold all the data. Thus, after the second call, when the
// arenas are reset the second time, the block that remains will be large
// enough to hold all the data - and so memory usage will not change at
// subsequent calls, if the arenas are reset.
// On the other hand we have the guarantee that after each incrementation of the
// arena we use up at least 1/3 of the space available. So after seven runs we
// will fill up all the memory - and if the arenas are not cleared, they will
// grow.
void TestEvaluationMemoryManagement(const Block& block,
                                    const Expression& expression) {
  std::unique_ptr<MemoryLimit> memory_limit(new MemoryLimit(1000000));
  const TupleSchema& schema = block.view().schema();
  FailureOrOwned<BoundExpressionTree> bound_expression =
      expression.Bind(schema, memory_limit.get(), block.row_capacity());
  ASSERT_TRUE(bound_expression.is_success())
      << bound_expression.exception().message();

  // See comment above function.
  for (int i = 0; i < 3; ++i) {
    FailureOrReference<const View> result_view_result =
        bound_expression->Evaluate(block.view());
    EXPECT_TRUE(result_view_result.is_success())
        << result_view_result.exception().message();
    // The point of this test is not checking the output, we have done this
    // before. So we just check the evaluation succeeded and ignore the results.
  }

  size_t memory_usage_after_three_evaluations = memory_limit->GetUsage();

  for (int i = 0; i < 7; ++i)  {
    FailureOrReference<const View> result_view_result =
        bound_expression->Evaluate(block.view());
    ASSERT_TRUE(result_view_result.is_success());
    ASSERT_EQ(memory_usage_after_three_evaluations, memory_limit->GetUsage())
        << "The expression " << expression.ToString(true) << " did not free "
        << "all memory after evaluation, "
        << static_cast<int>(memory_limit->GetUsage() -
          memory_usage_after_three_evaluations)
        << " bytes appeared at the second run.";
  }
}

// This testing function tests that the expressions created behave in the
// expected fashion with respect to changing blocks - that is, that when every
// row is given as input in a view of its own, the state does persist across the
// Evaluate calls. This is useful mostly for testing stateful expressions.
void TestEvaluationAcrossBlocks(
    const Block& block,
    const Expression& expr) {
  const TupleSchema& schema = block.view().schema();
  // The row capacity we give does not really matter, as we will pass single-row
  // views for evaluation anyway.
  FailureOrOwned<BoundExpressionTree> expression =
      expr.Bind(schema, HeapBufferAllocator::Get(), 2);
  ASSERT_FALSE(expression.is_failure())
      << expression.exception().message();

  // Prepare the expected output.
  int last_attribute_position = schema.attribute_count() - 1;
  const Attribute& last_attribute(schema.attribute(last_attribute_position));
  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectAttributeAt(last_attribute_position));
  FailureOrOwned<const BoundSingleSourceProjector> bound_projector =
      projector->Bind(schema);
  ASSERT_FALSE(bound_projector.is_failure())
      << bound_projector.exception().message();
  View expected_view(TupleSchema::Singleton("col0",
                                            last_attribute.type(),
                                            last_attribute.nullability()));
  bound_projector->Project(block.view(), &expected_view);
  expected_view.set_row_count(block.view().row_count());

  for (rowid_t row = 0; row < block.view().row_count(); ++row) {
    View restricted_input(block.view(),
                          /* offset = */ row,
                          /* row count = */ 1);
    FailureOrReference<const View> result_view =
        expression->Evaluate(restricted_input);
    ASSERT_FALSE(result_view.is_failure())
        << result_view.exception().message();

    View expected_output(expected_view,
                         /* offset = */ row,
                         /* row count = */ 1);
    ASSERT_VIEWS_EQUAL(expected_output, result_view.get());
  }
}

void TestEvaluationCommon(const Block* block,
                          bool stateful_expression,
                          const Expression* expr_ptr) {
  std::unique_ptr<const Expression> expr(expr_ptr);
  // Test for the original size and content.
  ASSERT_NO_FATAL_FAILURE(
      TestEvaluationForAlignedResizedBlock(*block, *expr,
                                           block->row_capacity()));
  // Test the original block split into 1-row views.
  ASSERT_NO_FATAL_FAILURE(TestEvaluationAcrossBlocks(*block, *expr));
  if (!stateful_expression) {
    // Test whether any memory gets left behind the expression.
    ASSERT_NO_FATAL_FAILURE(
        TestEvaluationMemoryManagement(*block, *expr));
    // To ensure that SIMD operations can be used at all and for all rows:
    // - use aligning BufferAllocator (TestEvaluationForAlignedResizedBlock),
    // - add 15 rows to fill the last relevant 16-byte memory block in each
    //   column (15 is chosen for BOOL, other types need less).
    ASSERT_NO_FATAL_FAILURE(
        TestEvaluationForAlignedResizedBlock(*block, *expr,
                                             block->row_capacity() + 15));
    // Test even bigger block - may be needed to test SIMD execution of
    // operations on bit_ptrs.
    ASSERT_NO_FATAL_FAILURE(
        TestEvaluationForAlignedResizedBlock(*block, *expr, 1024));
  }
}

template<typename ExpressionCreator>
void TestEvaluationTemplated(const Block* block, ExpressionCreator factory,
                             bool stateful) {
  std::unique_ptr<const Block> block_deleter(block);
  TestEvaluationCommon(
      block, stateful,
      CreateTestedExpression(factory));
  // Nullable expressions are expected to pass their nullability vector both
  // in the EvaluationResult, and via the mutable skip vector passed by the
  // parent. Invocation via the CompoundExpression tests the latter case.
  TestEvaluationCommon(
      block, stateful,
      (new CompoundExpression())->Add(CreateTestedExpression(factory)));
}

void TestEvaluation(const Block* block, ConstExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, UnaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, BinaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, TernaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, QuaternaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, QuinaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}
void TestEvaluation(const Block* block, SenaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, false);
}

void TestStatefulEvaluation(const Block* block,
                            ConstExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            UnaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            BinaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            TernaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            QuaternaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            QuinaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}
void TestStatefulEvaluation(const Block* block,
                            SenaryExpressionCreator factory) {
  TestEvaluationTemplated(block, factory, true);
}

// -------------------------- Evaluation failure tests --------------------

// Traverses the input view and evaluates the expression on each line
// separately, expecting a failure each time.
void ExpectFailureLineByLine(const View& input,
                             BoundExpressionTree* expression) {
  for (int line = 1; line < input.row_count(); ++line) {
    View one_line_view(input.schema());
    one_line_view.ResetFromSubRange(input, line, 1);
    EXPECT_TRUE(expression->Evaluate(one_line_view).is_failure());
  }
}

// Evaluation failure test for unary expressions.
void TestEvaluationFailureCommon(const Block* block_ptr,
                                 const Expression* expr) {
  std::unique_ptr<const Block> block(block_ptr);
  std::unique_ptr<BoundExpressionTree> bound_expr(
      DefaultBind(block->view().schema(), kMaxNumberOfRowsInTest, expr));
  ExpectFailureLineByLine(block->view(), bound_expr.get());
}

void TestEvaluationFailure(const Block* block, ConstExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block, UnaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block,
                           BinaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block,
                           TernaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block,
                           QuaternaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block,
                           QuinaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}
void TestEvaluationFailure(const Block* block,
                           SenaryExpressionCreator factory) {
  TestEvaluationFailureCommon(block, CreateTestedExpression(factory));
}

// ------------------------ Schema tests -------------------------------

// Helper, to provide some default content for ENUMs.
void AddAttribute(const string& name, DataType type, Nullability nullability,
                  TupleSchema* schema) {
  if (type == ENUM) {
    EnumDefinition enum_definition;
    enum_definition.AddEntry(1, "FIRST");
    enum_definition.AddEntry(5, "FIFTH");
    schema->add_attribute(Attribute(name, enum_definition, nullability));
  } else {
    schema->add_attribute(Attribute(name, type, nullability));
  }
}

// Encapsulates a schema containing all the possible DataTypes, and a method
// for accessing a column of the required type.
class SchemaHolder {
 public:
  SchemaHolder() {
    TupleSchema* schema = new TupleSchema();
    DataType types[kNumberOfTypes] = {INT32, UINT32, INT64, UINT64, FLOAT,
      DOUBLE, BOOL, DATE, DATETIME, STRING, BINARY, DATA_TYPE, ENUM};
    for (int i = 0; i < kNumberOfTypes; ++i) {
      AddAttribute(SimpleItoa(i), types[i], NOT_NULLABLE, schema);
    }
    for (int i = 0; i < kNumberOfTypes; ++i) {
      AddAttribute(SimpleItoa(i + 12), types[i], NULLABLE, schema);
    }
    schema_.reset(schema);
  }

  static const SchemaHolder* get() {
    static SchemaHolder schema_holder;
    return &schema_holder;
  }

  static int ColumnNumber(DataType type) {
    switch (type) {
      case INT32: return 0;
      case UINT32: return 1;
      case INT64: return 2;
      case UINT64: return 3;
      case FLOAT: return 4;
      case DOUBLE: return 5;
      case BOOL: return 6;
      case DATE: return 7;
      case DATETIME: return 8;
      case STRING: return 9;
      case BINARY: return 10;
      case DATA_TYPE: return 11;
      case ENUM: return 12;
      // No default statement to cause a compiler error if new DataTypes are
      // added.
    }
    // For compile error avoidance, will never get called.
    LOG(FATAL) << "Unknown type encountered in testing";
  }

  static int ColumnNumber(DataType type, bool nullable) {
    return nullable ? ColumnNumber(type) + kNumberOfTypes
        : ColumnNumber(type);
  }

  const TupleSchema* schema() const {
    return schema_.get();
  }

 private:
  // TODO(onufry): this constant should be defined in ../public/types.h and
  // used from there.
  static const int kNumberOfTypes = 13;
  std::unique_ptr<const TupleSchema> schema_;

  DISALLOW_COPY_AND_ASSIGN(SchemaHolder);
};

const int SchemaHolder::kNumberOfTypes;

::testing::AssertionResult CheckExpressionSuccessAndSchema(
    FailureOrOwned<BoundExpressionTree> expression,
    const string& expected_name,
    DataType return_type,
    bool nullable) {
  if (expression.is_failure())
    return ::testing::AssertionFailure() << expression.exception().message();

  return TupleSchemasStrictEqual(
      "expected", "result_schema",
      TupleSchema::Singleton(expected_name, return_type,
                             nullable ? NULLABLE : NOT_NULLABLE),
      expression->result_schema());
}

// Unary versions with and without nulls.

::testing::AssertionResult InternalTestUnaryBinding(
    UnaryExpressionCreator creator,
    int input_number,
    const char* expected_name_template,
    DataType return_type,
    bool nullable) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  string expected_name = strings::Substitute(expected_name_template,
                                             input_number);
  FailureOrOwned<BoundExpressionTree> expression =
      StandardBind(*(schema_holder->schema()),
                   1, (*creator)(AttributeAt(input_number)));
  return CheckExpressionSuccessAndSchema(
      expression, expected_name, return_type, nullable);
}

::testing::AssertionResult TestUnaryBindingFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    UnaryExpressionCreator creator,
    DataType input_type,
    const char* expected_name_template,
    DataType return_type,
    bool nullable) {
  int input_number = SchemaHolder::ColumnNumber(input_type);
  return InternalTestUnaryBinding(creator, input_number, expected_name_template,
                                  return_type, nullable);
}

void TestBindingWithNull(UnaryExpressionCreator creator, DataType input_type,
                         bool input_nullable,
                         const char* expected_name_template,
                         DataType return_type, bool nullable) {
  int input_number = SchemaHolder::ColumnNumber(input_type, input_nullable);
  InternalTestUnaryBinding(creator, input_number, expected_name_template,
                      return_type, nullable);
}

// Binary versions with and without nulls.

::testing::AssertionResult InternalTestBindingBinary(
    BinaryExpressionCreator creator,
    int left_number, int right_number, const char* expected_name_template,
    DataType return_type, bool nullable) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  string expected_name = strings::Substitute(expected_name_template,
                                             left_number, right_number);
  FailureOrOwned<BoundExpressionTree> expression =
      StandardBind(*(schema_holder->schema()),
                  1, (*creator)(AttributeAt(left_number),
                                AttributeAt(right_number)));
  return CheckExpressionSuccessAndSchema(
      expression, expected_name, return_type, nullable);
}

::testing::AssertionResult TestBinaryBindingFunction(
    BinaryExpressionCreator creator,
    DataType left_type, DataType right_type,
    const char* expected_name_template,
    DataType return_type,
    bool nullable) {
  int left_number = SchemaHolder::ColumnNumber(left_type);
  int right_number = SchemaHolder::ColumnNumber(right_type);
  return InternalTestBindingBinary(
      creator, left_number, right_number,
      expected_name_template, return_type, nullable);
}

::testing::AssertionResult TestBinaryBindingFunctionNullable(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    const char* s5,
    BinaryExpressionCreator creator,
    DataType left_type,
    DataType right_type,
    const char* expected_name_template,
    DataType return_type) {
  return TestBinaryBindingFunction(creator, left_type, right_type,
                                   expected_name_template, return_type, true);
}

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
    DataType return_type) {
  return TestBinaryBindingFunction(creator, left_type, right_type,
                                   expected_name_template, return_type, false);
}

void TestBindingWithNull(BinaryExpressionCreator creator, DataType left_type,
                         bool left_nullable, DataType right_type,
                         bool right_nullable,
                         const char* expected_name_template,
                         DataType return_type, bool nullable) {
  int left_number = SchemaHolder::ColumnNumber(left_type, left_nullable);
  int right_number = SchemaHolder::ColumnNumber(right_type, right_nullable);
  InternalTestBindingBinary(creator, left_number, right_number,
                      expected_name_template, return_type, nullable);
}

// Ternary versions with and without nulls.

::testing::AssertionResult InternalTestTernaryBinding(
    TernaryExpressionCreator creator,
    int left_number, int middle_number, int right_number,
    const char* expected_name_template,
    DataType return_type,
    bool nullable) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  string expected_name = strings::Substitute(expected_name_template,
                                             left_number, middle_number,
                                             right_number);
  FailureOrOwned<BoundExpressionTree> expression =
      StandardBind(*(schema_holder->schema()),
                  1, (*creator)(AttributeAt(left_number),
                                AttributeAt(middle_number),
                                AttributeAt(right_number)));
  return CheckExpressionSuccessAndSchema(
      expression, expected_name, return_type, nullable);
}

void TestTernaryBinding(
    TernaryExpressionCreator creator,
    DataType left_type, DataType middle_type, DataType right_type,
    const char* expected_name_template,
    DataType return_type,
    bool nullable) {
  int left_number = SchemaHolder::ColumnNumber(left_type);
  int middle_number = SchemaHolder::ColumnNumber(middle_type);
  int right_number = SchemaHolder::ColumnNumber(right_type);
  ASSERT_TRUE(InternalTestTernaryBinding(
      creator, left_number, middle_number, right_number,
      expected_name_template, return_type, nullable) == true);
}

void TestBindingWithNull(TernaryExpressionCreator creator, DataType left_type,
                         bool left_nullable, DataType middle_type,
                         bool middle_nullable, DataType right_type,
                         bool right_nullable,
                         const char* expected_name_template,
                         DataType return_type, bool nullable) {
  int left_number = SchemaHolder::ColumnNumber(left_type, left_nullable);
  int middle_number = SchemaHolder::ColumnNumber(middle_type, middle_nullable);
  int right_number = SchemaHolder::ColumnNumber(right_type, right_nullable);
  ASSERT_TRUE(InternalTestTernaryBinding(
      creator, left_number, middle_number, right_number,
      expected_name_template, return_type, nullable) == true);
}

// ------------------------- Binding failure testers --------------------
// Unary binding failure.

void TestUnaryBindingFailureInternal(UnaryExpressionCreator creator,
                                     int column_number) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  std::unique_ptr<const Expression> deleter(
      (*creator)(AttributeAt(column_number)));

  FailureOrOwned<BoundExpressionTree> bind_result =
      deleter->Bind(*(schema_holder->schema()), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(bind_result.is_failure())
      << bind_result->result_schema().GetHumanReadableSpecification();
}

void TestBindingFailure(UnaryExpressionCreator creator,
                        DataType input_type) {
  int column_number = SchemaHolder::ColumnNumber(input_type);
  TestUnaryBindingFailureInternal(creator, column_number);
}

void TestUnaryBindingFailureWithNulls(UnaryExpressionCreator creator,
                                      DataType input_type,
                                      bool input_is_nullable) {
  int column_number = SchemaHolder::ColumnNumber(input_type, input_is_nullable);
  TestUnaryBindingFailureInternal(creator, column_number);
}

// Binary binding failure.
void TestBinaryBindingFailureInternal(BinaryExpressionCreator creator,
                                      int left_column_number,
                                      int right_column_number) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  std::unique_ptr<const Expression> deleter((*creator)(
      AttributeAt(left_column_number), AttributeAt(right_column_number)));

  FailureOrOwned<BoundExpressionTree> bind_result =
      deleter->Bind(*(schema_holder->schema()), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(bind_result.is_failure())
      << bind_result->result_schema().GetHumanReadableSpecification();
}

void TestBindingFailure(BinaryExpressionCreator creator,
                        DataType left_type,
                        DataType right_type) {
  int left_column_number = SchemaHolder::ColumnNumber(left_type);
  int right_column_number = SchemaHolder::ColumnNumber(right_type);
  TestBinaryBindingFailureInternal(
      creator, left_column_number, right_column_number);
}

void TestBinaryBindingFailureWithNulls(BinaryExpressionCreator creator,
                                       DataType left_type,
                                       bool left_is_nullable,
                                       DataType right_type,
                                       bool right_is_nullable) {
  int left_column_number =
      SchemaHolder::ColumnNumber(left_type, left_is_nullable);
  int right_column_number =
      SchemaHolder::ColumnNumber(right_type, right_is_nullable);
  TestBinaryBindingFailureInternal(
      creator, left_column_number, right_column_number);
}

// Ternary binding failure.
void TestTernaryBindingFailureInternal(TernaryExpressionCreator creator,
                                       int left_column_number,
                                       int middle_column_number,
                                       int right_column_number) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  std::unique_ptr<const Expression> deleter((*creator)(
      AttributeAt(left_column_number), AttributeAt(middle_column_number),
      AttributeAt(right_column_number)));

  FailureOrOwned<BoundExpressionTree> bind_result =
      deleter->Bind(*(schema_holder->schema()), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(bind_result.is_failure())
      << bind_result->result_schema().GetHumanReadableSpecification();
}

void TestBindingFailure(TernaryExpressionCreator creator,
                        DataType left_type,
                        DataType middle_type,
                        DataType right_type) {
  int left_column_number = SchemaHolder::ColumnNumber(left_type);
  int middle_column_number = SchemaHolder::ColumnNumber(middle_type);
  int right_column_number = SchemaHolder::ColumnNumber(right_type);
  TestTernaryBindingFailureInternal(
      creator, left_column_number, middle_column_number, right_column_number);
}

void TestTernaryBindingFailureWithNulls(TernaryExpressionCreator creator,
                                        DataType left_type,
                                        bool left_is_nullable,
                                        DataType middle_type,
                                        bool middle_is_nullable,
                                        DataType right_type,
                                        bool right_is_nullable) {
  int left_column_number =
      SchemaHolder::ColumnNumber(left_type, left_is_nullable);
  int middle_column_number =
      SchemaHolder::ColumnNumber(middle_type, middle_is_nullable);
  int right_column_number =
      SchemaHolder::ColumnNumber(right_type, right_is_nullable);
  TestTernaryBindingFailureInternal(
      creator, left_column_number, middle_column_number, right_column_number);
}

// ------------------------ Bound factory testers ------------------

::testing::AssertionResult GetAssertionResultForFactory(
    FailureOrOwned<BoundExpression>* result,
    const char* expected_name) {
  if (result->is_failure()) {
    return ::testing::AssertionFailure()
        << "Got error:" << result->exception().PrintStackTrace();
  }
  if (expected_name != GetExpressionName(result->get())) {
    return ::testing::AssertionFailure()
        << "Expected : " << expected_name
        << "\nGot : " << GetExpressionName(result->get());
  }
  return ::testing::AssertionSuccess();
}

// A set of helper functions that takes SchemaHolder column numbers as inputs.
::testing::AssertionResult TestBoundUnaryFactoryInternal(
      BoundUnaryExpressionFactory factory,
      int input,
      const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  string expected_name = strings::Substitute(expected_name_template, input);
  std::unique_ptr<BoundExpression> bound_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(input)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_argument.release(), HeapBufferAllocator::Get(), 1);
  return GetAssertionResultForFactory(&result, expected_name.c_str());
}

// Helper function for binary factories.
::testing::AssertionResult TestBoundBinaryFactoryInternal(
    BoundBinaryExpressionFactory factory,
    int left,
    int right,
    const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  string expected_name = strings::Substitute(expected_name_template,
                                             left, right);
  std::unique_ptr<BoundExpression> bound_left_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(left)));
  std::unique_ptr<BoundExpression> bound_right_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(right)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_left_argument.release(), bound_right_argument.release(),
      HeapBufferAllocator::Get(), 1);
  return GetAssertionResultForFactory(&result, expected_name.c_str());
}

// Tester for constant expressions.
::testing::AssertionResult TestBoundFactoryFunction(
    const char* s1,
    const char* s2,
    BoundConstExpressionFactory factory,
    const char* expected_name_template) {
  string expected_name = string(expected_name_template);
  FailureOrOwned<BoundExpression> result = (*factory)(
      HeapBufferAllocator::Get(), 1);
  return GetAssertionResultForFactory(&result, expected_name.c_str());
}

// Tester for unary expressions.
::testing::AssertionResult TestBoundUnaryFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    BoundUnaryExpressionFactory factory,
    DataType input_type,
    const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int arg_number = schema_holder->ColumnNumber(input_type);
  return TestBoundUnaryFactoryInternal(
      factory, arg_number, expected_name_template);
}

// Tester for unary expressions with nulls.
void TestBoundFactoryWithNulls(BoundUnaryExpressionFactory factory,
                              DataType input_type, bool input_nullable,
                              const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int arg_number = schema_holder->ColumnNumber(input_type, input_nullable);
  TestBoundUnaryFactoryInternal(factory, arg_number, expected_name_template);
}

// Tester for binding failure.
void TestBoundFactoryFailure(BoundUnaryExpressionFactory factory,
                             DataType input_type) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int arg_number = schema_holder->ColumnNumber(input_type);
  std::unique_ptr<BoundExpression> bound_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(arg_number)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_argument.release(), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(result.is_failure()) << "Expected failure, but got: "
                                   << GetExpressionName(result.get());
}

// Tester for binary expressions.
::testing::AssertionResult TestBoundBinaryFunction(
    const char* s1,
    const char* s2,
    const char* s3,
    const char* s4,
    BoundBinaryExpressionFactory factory,
    DataType left_type,
    DataType right_type,
    const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type);
  int right_number = schema_holder->ColumnNumber(right_type);
  return TestBoundBinaryFactoryInternal(factory, left_number, right_number,
                                        expected_name_template);
}

// Tester for binary expressions with nulls.
void TestBoundFactoryWithNulls(BoundBinaryExpressionFactory factory,
                               DataType left_type, bool left_nullable,
                               DataType right_type, bool right_nullable,
                               const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type, left_nullable);
  int right_number = schema_holder->ColumnNumber(right_type, right_nullable);
  TestBoundBinaryFactoryInternal(factory, left_number, right_number,
                                 expected_name_template);
}

// Tester for binding failure.
void TestBoundFactoryFailure(BoundBinaryExpressionFactory factory,
                             DataType left_type, DataType right_type) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type);
  int right_number = schema_holder->ColumnNumber(right_type);

  std::unique_ptr<BoundExpression> bound_left_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(left_number)));
  std::unique_ptr<BoundExpression> bound_right_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(right_number)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_left_argument.release(), bound_right_argument.release(),
      HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(result.is_failure()) << "Expected failure, but got: "
                                   << GetExpressionName(result.get());
}

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
    const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type);
  int middle_number = schema_holder->ColumnNumber(middle_type);
  int right_number = schema_holder->ColumnNumber(right_type);

  string expected_name = strings::Substitute(expected_name_template,
                                             left_number, middle_number,
                                             right_number);
  std::unique_ptr<BoundExpression> bound_left_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(left_number)));
  std::unique_ptr<BoundExpression> bound_middle_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(middle_number)));
  std::unique_ptr<BoundExpression> bound_right_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(right_number)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_left_argument.release(), bound_middle_argument.release(),
      bound_right_argument.release(), HeapBufferAllocator::Get(), 1);
  return GetAssertionResultForFactory(&result, expected_name.c_str());
}

// Tester for binding failure.
void TestBoundFactoryFailure(BoundTernaryExpressionFactory factory,
                             DataType left_type, DataType middle_type,
                             DataType right_type) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type);
  int middle_number = schema_holder->ColumnNumber(middle_type);
  int right_number = schema_holder->ColumnNumber(right_type);

  std::unique_ptr<BoundExpression> bound_left_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(left_number)));
  std::unique_ptr<BoundExpression> bound_middle_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(middle_number)));
  std::unique_ptr<BoundExpression> bound_right_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(right_number)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_left_argument.release(), bound_middle_argument.release(),
      bound_right_argument.release(), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(result.is_failure()) << "Expected failure, but got: "
                                   << GetExpressionName(result.get());
}

void TestBoundQuaternary(BoundQuaternaryExpressionFactory factory,
                         DataType left_type,
                         DataType middle_left_type,
                         DataType middle_right_type,
                         DataType right_type,
                         const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  int left_number = schema_holder->ColumnNumber(left_type);
  int middle_left_number = schema_holder->ColumnNumber(middle_left_type);
  int middle_right_number = schema_holder->ColumnNumber(middle_right_type);
  int right_number = schema_holder->ColumnNumber(right_type);

  string expected_name = strings::Substitute(expected_name_template,
                                             left_number, middle_left_number,
                                             middle_right_number, right_number);
  std::unique_ptr<BoundExpression> bound_left_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(left_number)));
  std::unique_ptr<BoundExpression> bound_middle_left_argument(DefaultDoBind(
      *(schema_holder->schema()), 1, AttributeAt(middle_left_number)));
  std::unique_ptr<BoundExpression> bound_middle_right_argument(DefaultDoBind(
      *(schema_holder->schema()), 1, AttributeAt(middle_right_number)));
  std::unique_ptr<BoundExpression> bound_right_argument(
      DefaultDoBind(*(schema_holder->schema()), 1, AttributeAt(right_number)));
  FailureOrOwned<BoundExpression> result = (*factory)(
      bound_left_argument.release(), bound_middle_left_argument.release(),
      bound_middle_right_argument.release(),
      bound_right_argument.release(), HeapBufferAllocator::Get(), 1);
  ASSERT_TRUE(result.is_success()) << result.exception().PrintStackTrace();
  EXPECT_EQ(expected_name, GetExpressionName(result.get()));
}

void TestBoundExpressionList(BoundExpressionListExpressionFactory factory,
                             vector<DataType> data_types,
                             const char* expected_name_template) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  std::unique_ptr<BoundExpressionList> expression_list(
      new BoundExpressionList());
  string expected_name = expected_name_template;
  vector<int> column_numbers;
  for (int i = 0; i < data_types.size(); ++i) {
    int column_number = schema_holder->ColumnNumber(data_types[i]);
    expression_list->add(DefaultDoBind(*(schema_holder->schema()), 1,
        AttributeAt(column_number)));
    column_numbers.push_back(column_number);
  }
  // Going from the biggest to replace $11 before $1.
  for (int i = data_types.size() - 1; i >= 0; --i) {
    GlobalReplaceSubstring(StrCat("$", i),
                           SimpleItoa(column_numbers[i]), &expected_name);
  }
  FailureOrOwned<BoundExpression> result = (*factory)(
      expression_list.release(), HeapBufferAllocator::Get(), 1);
  ASSERT_TRUE(result.is_success()) << result.exception().PrintStackTrace();
  EXPECT_EQ(expected_name, GetExpressionName(result.get()));
}

void TestBoundFactoryFailure(BoundExpressionListExpressionFactory factory,
                             vector<DataType> data_types) {
  const SchemaHolder* schema_holder = SchemaHolder::get();
  std::unique_ptr<BoundExpressionList> expression_list(
      new BoundExpressionList());

  for (vector<DataType>::const_iterator it = data_types.begin();
      it != data_types.end(); ++it) {
    int column_number = schema_holder->ColumnNumber(*it);
    expression_list->add(DefaultDoBind(*(schema_holder->schema()), 1,
        AttributeAt(column_number)));
  }
  FailureOrOwned<BoundExpression> result = (*factory)(
      expression_list.release(), HeapBufferAllocator::Get(), 1);
  EXPECT_TRUE(result.is_failure()) << "Expected failure, but got: "
                                   << GetExpressionName(result.get());
}

}  // namespace supersonic
