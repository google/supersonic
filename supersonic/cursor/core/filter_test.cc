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

#include "supersonic/cursor/core/filter.h"

#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/comparison_expressions.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class BoundFakePredicate : public BoundExpression {
 public:
  BoundFakePredicate(const vector<bool>& results,
                     const vector<bool>& null_results)
      : BoundExpression(TupleSchema::Singleton(
            "result", BOOL, null_results.empty() ? NOT_NULLABLE : NULLABLE)),
        result_block_(TupleSchema::Singleton(
            "result", BOOL, null_results.empty() ? NOT_NULLABLE : NULLABLE),
            HeapBufferAllocator::Get()),
        to_advance_(0),
        index_(0) {
    result_block_.Reallocate(results.size());
    OwnedColumn& column = *result_block_.mutable_column(0);
    bool* data = column.mutable_typed_data<BOOL>();
    bool_ptr null_data = column.mutable_is_null();
    for (int i = 0; i < results.size(); ++i, ++data) {
      *data = results[i];
      if (null_results.size() != 0) {
        *null_data = null_results[i];
        ++null_data;
      }
    }
    my_view()->ResetFrom(result_block_.view());
  }
  virtual ~BoundFakePredicate() {}

  rowcount_t row_capacity() const {
    return result_block_.row_capacity() - index_;
  }

  bool is_constant() const {
    return false;
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    CHECK_LE(index_ + input.row_count(),
             result_block_.row_capacity());
    my_view()->Advance(to_advance_);
    to_advance_ = input.row_count();
    // This is not the most efficient way to do this, but we do not really care
    // about efficiency here.
    if (result_block_.schema().attribute(0).is_nullable()) {
      for (int i = 0; i < input.row_count(); ++i) {
        skip_vectors.column(0)[i] |= result_block_.is_null(0)[index_ + i];
      }
    }
    index_ += to_advance_;
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {}

 private:
  Block result_block_;
  rowcount_t to_advance_;
  rowid_t index_;
};

class FakePredicate : public Expression {
 public:
  FakePredicate(const vector<bool>& results, const vector<bool>& null_results)
      : results_(results),
        null_results_(null_results) {}
  virtual ~FakePredicate() {}

  virtual string ToString(bool verbose) const {
    return "FAKE_PREDICATE";
  }

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    return Success(new BoundFakePredicate(results_, null_results_));
  }

 private:
  const vector<bool> results_;
  const vector<bool> null_results_;
};

Operation* CreateFilter(Operation* input,
                        const vector<bool> predicate_results,
                        const vector<bool> predicate_null_results) {
  return Filter(
      new FakePredicate(predicate_results, predicate_null_results),
      ProjectAllAttributes(), input);
}

Operation* CreateFilter(Operation* input,
                        const vector<bool> predicate_results) {
  return CreateFilter(input, predicate_results, vector<bool>());
}

TEST(FilterCursorTest, AllPassing) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(1, "A")
                         .AddRow(3, "B")
                         .Build());
  test.Execute(
      CreateFilter(test.input(), vector<bool>(2, true)));
}

TEST(FilterCursorTest, NonePassing) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .Build());
  test.Execute(
      CreateFilter(test.input(), vector<bool>(2, false)));
}

TEST(FilterCursorTest, OnePassing) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(3, "B")
                         .Build());

  vector<bool> predicate_results;
  predicate_results.push_back(false);
  predicate_results.push_back(true);
  test.Execute(CreateFilter(test.input(), predicate_results));
}

TEST(FilterCursorTest, OutOfMemoryError) {
  Expression* predicate = new FakePredicate(vector<bool>(true),
                                            vector<bool>(true));
  MemoryLimit memory_limit(0);
  std::unique_ptr<Operation> filter(
      Filter(predicate, ProjectAllAttributes(),
             new Table(BlockBuilder<INT32, STRING>().Build())));
  filter->SetBufferAllocator(&memory_limit, true);
  FailureOrOwned<Cursor> result = filter->CreateCursor();
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_MEMORY_EXCEEDED, result.exception().return_code());
}

TEST(FilterCursorTest, ResultsAcrossBlocks) {
  TestDataBuilder<INT32, STRING> input_builder;
  TestDataBuilder<INT32, STRING> expected_builder;
  vector<bool> predicate_results;

  for (int i = 0; i < Cursor::kDefaultRowCount * 2; i++) {
    input_builder.AddRow(i, "A");
    if (i % 2) {
      predicate_results.push_back(true);
      expected_builder.AddRow(i, "A");
    } else {
      predicate_results.push_back(false);
    }
  }

  OperationTest test;
  test.SetInput(input_builder.Build());
  test.SetExpectedResult(expected_builder.Build());
  test.Execute(CreateFilter(test.input(), predicate_results));
}

TEST(FilterCursorTest, NoResultsFromFirstBlock) {
  TestDataBuilder<INT32, STRING> input_builder;
  TestDataBuilder<INT32, STRING> expected_builder;
  vector<bool> predicate_results;

  for (int i = 0; i < Cursor::kDefaultRowCount * 2; i++) {
    input_builder.AddRow(i, "A");
    if (i >= Cursor::kDefaultRowCount) {
      predicate_results.push_back(true);
      expected_builder.AddRow(i, "A");
    } else {
      predicate_results.push_back(false);
    }
  }

  OperationTest test;
  test.SetInput(input_builder.Build());
  test.SetExpectedResult(expected_builder.Build());
  test.Execute(CreateFilter(test.input(), predicate_results));
}

TEST(FilterCursorTest, NoResultsFromSecondBlock) {
  TestDataBuilder<INT32, STRING> input_builder;
  TestDataBuilder<INT32, STRING> expected_builder;
  vector<bool> predicate_results;

  for (int i = 0; i < Cursor::kDefaultRowCount * 2; i++) {
    input_builder.AddRow(i, "A");
    if (i < Cursor::kDefaultRowCount) {
      predicate_results.push_back(true);
      expected_builder.AddRow(i, "A");
    } else {
      predicate_results.push_back(false);
    }
  }

  OperationTest test;
  test.SetInput(input_builder.Build());
  test.SetExpectedResult(expected_builder.Build());
  test.Execute(CreateFilter(test.input(), predicate_results));
}

TEST(FilterCursorTest, NullableAllPassing) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(1, "A")
                         .AddRow(3, "B")
                         .Build());
  test.Execute(
      CreateFilter(test.input(), vector<bool>(2, true),
                                 vector<bool>(2, false)));
}

TEST(FilterCursorTest, NullableOnePassing) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(3, "B")
                         .Build());
  vector<bool> predicate_null_results;
  predicate_null_results.push_back(true);
  predicate_null_results.push_back(false);
  test.Execute(
      CreateFilter(test.input(), vector<bool>(2, true),
                                 predicate_null_results));
}

TEST(FilterCursorTest, FilterOnProjected) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(1)
                         .Build());
  test.Execute(
      Filter(Equal(NamedAttribute("col0"), ConstInt32(1)),
             ProjectNamedAttribute("col0"),
             test.input()));
}

TEST(FilterCursorTest, FilterOnDropped) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "A")
                .AddRow(3, "B")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(1)
                         .Build());
  test.Execute(
      Filter(Equal(NamedAttribute("col1"), ConstString("A")),
             ProjectNamedAttribute("col0"),
             test.input()));
}

TEST(FilterCursorTest, MaxRowCount) {
  std::unique_ptr<Table> table(
      new Table(BlockBuilder<INT32>().AddRow(-12).AddRow(12).Build()));
  std::unique_ptr<Cursor> cursor(SucceedOrDie(table->CreateCursor()));
  std::unique_ptr<BoundExpressionTree> bound(
      DefaultBind(table->schema(), 1, Greater(AttributeAt(0), ConstInt32(0))));

  std::unique_ptr<const SingleSourceProjector> projector(
      ProjectAllAttributes());
  std::unique_ptr<const BoundSingleSourceProjector> bound_projector(
      SucceedOrDie(projector->Bind(table->schema())));

  FailureOrOwned<Cursor> filter_result =
      BoundFilter(bound.release(), bound_projector.release(),
                  HeapBufferAllocator::Get(), cursor.release());
  ASSERT_TRUE(filter_result.is_success());
  std::unique_ptr<Cursor> filter_cursor(filter_result.release());
  filter_cursor->Next(1);
}

}  // namespace supersonic
