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

#include "supersonic/cursor/core/sort.h"

#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/core/specification_builder.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/proto/specification.pb.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class Operation;

class SortTest : public testing::TestWithParam<int> {
 public:
  SortTest() {}
  Operation* CreateSortOperation(const SortOrder* sort_order,
                                 const SingleSourceProjector* result_projector,
                                 Operation* input) {
    const size_t soft_quota = GetParam();
    return Sort(sort_order,
                result_projector,
                soft_quota,
                input);
  }

  FailureOrOwned<Cursor> CreateSortCursor(
      const SortOrder* sort_order,
      const SingleSourceProjector* result_projector,
      Cursor* input) {
    FailureOrOwned<const BoundSortOrder> bound_sort_order(
        sort_order->Bind(input->schema()));
    PROPAGATE_ON_FAILURE(bound_sort_order);

    FailureOrOwned<const BoundSingleSourceProjector> bound_projector(
        result_projector->Bind(input->schema()));
    PROPAGATE_ON_FAILURE(bound_projector);

    const size_t soft_quota = GetParam();

    return BoundSort(bound_sort_order.release(),
                     bound_projector.release(),
                     soft_quota,
                     "",
                     HeapBufferAllocator::Get(),
                     input);
  }

 protected:
  virtual void CreateSampleData() {
    sample_input_.AddRow(__, "a")
                 .AddRow(3,  "c")
                 .AddRow(__, "a")
                 .AddRow(7,  "g")
                 .AddRow(4,  "d")
                 .AddRow(6,  "f")
                 .AddRow(5,  "e");
    sample_output_.AddRow(__, "a")
                  .AddRow(__, "a")
                  .AddRow(3,  "c")
                  .AddRow(4,  "d")
                  .AddRow(5,  "e")
                  .AddRow(6,  "f")
                  .AddRow(7,  "g");
  }

  TestDataBuilder<INT32, STRING> sample_input_;
  TestDataBuilder<INT32, STRING> sample_output_;
};

class ExtendedSortTest : public testing::TestWithParam<int> {
 public:
  ExtendedSortTest() {}
  Operation* CreateExtendedSortOperation(
      const ExtendedSortSpecification* specification,
      Operation* input) {
    const size_t soft_quota = GetParam();
    return ExtendedSort(
        specification,
        /* result projector = */ static_cast<SingleSourceProjector*>(NULL),
        soft_quota,
        input);
  }
};

INSTANTIATE_TEST_CASE_P(soft_quota,
                        SortTest,
                        testing::Values(0, 1, 1<<12, 1<<16, 1<<20, 1<<24,
                                        std::numeric_limits<int>::max()));

INSTANTIATE_TEST_CASE_P(soft_quota,
                        ExtendedSortTest,
                        testing::Values(0, 1<<24,
                                        std::numeric_limits<int>::max()));

TEST_P(SortTest, OneIntegerColumnNoDuplicatesNoNulls) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(2, "b")
                .AddRow(3, "c")
                .AddRow(1, "a")
                .AddRow(7, "g")
                .AddRow(4, "d")
                .AddRow(6, "f")
                .AddRow(5, "e")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(1, "a")
                         .AddRow(2, "b")
                         .AddRow(3, "c")
                         .AddRow(4, "d")
                         .AddRow(5, "e")
                         .AddRow(6, "f")
                         .AddRow(7, "g")
                         .Build());
  test.Execute(CreateSortOperation(
      (new SortOrder())->add(ProjectNamedAttribute("col0"), ASCENDING),
      NULL,
      test.input()));
}

TEST_P(SortTest, OneIntegerColumnNoDuplicatesWithNulls) {
  OperationTest test;
  CreateSampleData();
  test.SetInput(sample_input_.Build());
  test.SetExpectedResult(sample_output_.Build());
  test.Execute(CreateSortOperation(
      (new SortOrder())->add(ProjectNamedAttribute("col0"), ASCENDING),
      NULL,
      test.input()));
}

TEST_P(SortTest, OneIntegerColumnNoDuplicatesWithNullsAndSpyTransform) {
  CreateSampleData();
  Cursor* input = sample_input_.BuildCursor();
  std::unique_ptr<Cursor> expected_result(sample_output_.BuildCursor());

  std::unique_ptr<SortOrder> order(new SortOrder);
  order->add(ProjectNamedAttribute("col0"), ASCENDING);

  std::unique_ptr<const SingleSourceProjector> project_all(
      ProjectAllAttributes());

  std::unique_ptr<Cursor> sort(
      SucceedOrDie(CreateSortCursor(order.get(), project_all.get(), input)));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  sort->ApplyToChildren(spy_transformer.get());
  sort.reset(spy_transformer->Transform(sort.release()));

  EXPECT_CURSORS_EQUAL(expected_result.release(), sort.release());
}

TEST_P(SortTest, OneEmptyStringColumn) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>().Build());
  test.SetExpectedResult(TestDataBuilder<STRING>().Build());
  test.Execute(CreateSortOperation(
      (new SortOrder())->add(ProjectNamedAttribute("col0"), ASCENDING),
      NULL,
      test.input()));
}

TEST_P(SortTest, OneStringColumnWithDuplicatesAndNulls) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("c")
                .AddRow("a")
                .AddRow(__)
                .AddRow("d")
                .AddRow(__)
                .AddRow("e")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow(__)
                         .AddRow(__)
                         .AddRow("a")
                         .AddRow("a")
                         .AddRow("c")
                         .AddRow("d")
                         .AddRow("e")
                         .Build());
  test.Execute(CreateSortOperation(
      (new SortOrder())->add(ProjectNamedAttribute("col0"), ASCENDING),
      NULL,
      test.input()));
}

TEST_P(SortTest, OneIntegerColumnMostlyNullsDescending) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(__, "a")
                .AddRow(__, "a")
                .AddRow(__, "a")
                .AddRow(7,  "g")
                .AddRow(__, "a")
                .AddRow(__, "a")
                .AddRow(__, "a")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(7,  "g")
                         .AddRow(__, "a")
                         .AddRow(__, "a")
                         .AddRow(__, "a")
                         .AddRow(__, "a")
                         .AddRow(__, "a")
                         .AddRow(__, "a")
                         .Build());
  test.Execute(CreateSortOperation(
      (new SortOrder())->add(ProjectNamedAttribute("col0"), DESCENDING),
      NULL,
      test.input()));
}

TEST_P(SortTest, TwoColumnsFirstUnique) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(2, "x")
                .AddRow(3, "v")
                .AddRow(1, "z")
                .AddRow(7, "x")
                .AddRow(4, "w")
                .AddRow(6, "x")
                .AddRow(5, "y")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(1, "z")
                         .AddRow(2, "x")
                         .AddRow(3, "v")
                         .AddRow(4, "w")
                         .AddRow(5, "y")
                         .AddRow(6, "x")
                         .AddRow(7, "x")
                         .Build());
  test.Execute(
      CreateSortOperation(
          (new SortOrder())
              ->add(ProjectNamedAttribute("col0"), ASCENDING)
              ->add(ProjectNamedAttribute("col1"), DESCENDING),
           NULL,
           test.input()));
}

TEST_P(SortTest, TwoColumnsFirstConst) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "x")
                .AddRow(1, "v")
                .AddRow(1, "z")
                .AddRow(1, "x")
                .AddRow(1, "w")
                .AddRow(1, "x")
                .AddRow(1, "y")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(1, "v")
                         .AddRow(1, "w")
                         .AddRow(1, "x")
                         .AddRow(1, "x")
                         .AddRow(1, "x")
                         .AddRow(1, "y")
                         .AddRow(1, "z")
                         .Build());
  test.Execute(
      CreateSortOperation(
          (new SortOrder())
              ->add(ProjectNamedAttribute("col0"), ASCENDING)
              ->add(ProjectNamedAttribute("col1"), ASCENDING),
           NULL,
           test.input()));
}

TEST_P(SortTest, TwoColumnsFirstMixed) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(3,  "z")
                .AddRow(__, "v")
                .AddRow(2,  "z")
                .AddRow(3,  __)
                .AddRow(__, "w")
                .AddRow(3,  "x")
                .AddRow(1,  "x")
                .AddRow(__, "y")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(__, "v")
                         .AddRow(__, "w")
                         .AddRow(__, "y")
                         .AddRow(1,  "x")
                         .AddRow(2,  "z")
                         .AddRow(3,  __)
                         .AddRow(3,  "x")
                         .AddRow(3,  "z")
                         .Build());
  test.Execute(
      CreateSortOperation(
          (new SortOrder())
              ->add(ProjectNamedAttribute("col0"), ASCENDING)
              ->add(ProjectNamedAttribute("col1"), ASCENDING),
           NULL,
           test.input()));
}

TEST_P(SortTest, TwoColumnsFirstMixedDescending) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(3,  "z")
                .AddRow(__, "v")
                .AddRow(2,  "z")
                .AddRow(3,  __)
                .AddRow(__, "w")
                .AddRow(3,  "x")
                .AddRow(1,  "x")
                .AddRow(__, "y")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32, STRING>()
                         .AddRow(3,  "z")
                         .AddRow(3,  "x")
                         .AddRow(3,  __)
                         .AddRow(2,  "z")
                         .AddRow(1,  "x")
                         .AddRow(__, "y")
                         .AddRow(__, "w")
                         .AddRow(__, "v")
                         .Build());
  test.Execute(
      CreateSortOperation(
          (new SortOrder())
              ->add(ProjectNamedAttribute("col0"), DESCENDING)
              ->add(ProjectNamedAttribute("col1"), DESCENDING),
           NULL,
           test.input()));
}

TEST_P(SortTest, Projections) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, INT32, INT32>()
                .AddRow(3, 105, 210)
                .AddRow(6, 111, 201)
                .AddRow(2, 102, 203)
                .AddRow(3, 104, 205)
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(105)
                         .AddRow(104)
                         .AddRow(102)
                         .AddRow(111)
                         .Build());
  test.Execute(
      CreateSortOperation(
          (new SortOrder())->add(ProjectAttributeAt(2), DESCENDING),
          ProjectAttributeAt(1),
          test.input()));
}

TEST_P(ExtendedSortTest, OneStringEmptyColumn) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>().Build());
  test.SetExpectedResult(TestDataBuilder<STRING>().Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneStringColumnWithNoDuplicatesAndCaseInsensitive) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("a")
                         .AddRow("B")
                         .AddRow("c")
                         .AddRow("D")
                         .AddRow("e")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, TwoStringColumnsCaseInsensitive) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING, STRING>()
                .AddRow("a", "a")
                .AddRow("B", "b")
                .AddRow("e", "a")
                .AddRow("D", "B")
                .AddRow("C", "A")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING, STRING>()
                         .AddRow("a", "a")
                         .AddRow("C", "A")
                         .AddRow("e", "a")
                         .AddRow("B", "b")
                         .AddRow("D", "B")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col1", ASCENDING, false)
                                        ->Add("col0", ASCENDING, false)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, TwoStringColumnsCaseSensitivityMix) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING, STRING>()
                .AddRow("a", "a")
                .AddRow("B", "b")
                .AddRow("e", "a")
                .AddRow("D", "B")
                .AddRow("C", "A")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING, STRING>()
                         .AddRow("C", "A")
                         .AddRow("a", "a")
                         .AddRow("e", "a")
                         .AddRow("B", "b")
                         .AddRow("D", "B")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col1", ASCENDING, false)
                                        ->Add("col0", ASCENDING, true)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneStringColumnDifferentSensitivityOneColumnTest) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .AddRow("A")
                .AddRow("C")
                .AddRow("a")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("A")
                         .AddRow("a")
                         .AddRow("a")
                         .AddRow("B")
                         .AddRow("C")
                         .AddRow("c")
                         .AddRow("D")
                         .AddRow("e")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->Add("col0", ASCENDING, true)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneStringColumnCaseInsensitiveWithLimit) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("a")
                         .AddRow("B")
                         .AddRow("c")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->SetLimit(3)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneStringColumnCaseInsensitiveWithLimitZero) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->SetLimit(0)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneStringColumnCaseSensitiveWithLimit) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("B")
                         .AddRow("D")
                         .AddRow("a")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, true)
                                        ->SetLimit(3)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneColumnTwiceAsKeyCaseSensitiveFailure) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedBindFailure(ERROR_INVALID_ARGUMENT_VALUE);
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, true)
                                        ->Add("col0", DESCENDING, true)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, OneColumnTwiceAsKeyCaseInsensitiveFailure) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedBindFailure(ERROR_INVALID_ARGUMENT_VALUE);
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col0", ASCENDING, false)
                                        ->Add("col0", DESCENDING, false)
                                        ->Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, NoKeyTest) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING>()
                .AddRow("a")
                .AddRow("B")
                .AddRow("e")
                .AddRow("D")
                .AddRow("c")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("a")
                         .AddRow("B")
                         .AddRow("e")
                         .AddRow("D")
                         .AddRow("c")
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Build(),
      test.input()));
}

TEST_P(ExtendedSortTest, MultitypeKeys) {
  OperationTest test;
  test.SetInput(TestDataBuilder<STRING, STRING, INT32, DOUBLE>()
                .AddRow("a", "A", 1, 0.5)
                .AddRow("a", "A", 1, -0.5)
                .AddRow("a", "A", 0, 0.5)
                .AddRow("a", "A", 0, -0.5)
                .AddRow("a", "B", 1, 0.5)
                .AddRow("a", "B", 1, -0.5)
                .AddRow("a", "B", 0, 0.5)
                .AddRow("a", "B", 0, -0.5)
                .AddRow("A", "a", 1, 0.5)
                .AddRow("A", "a", 1, -0.5)
                .AddRow("A", "a", 0, 0.5)
                .AddRow("A", "a", 0, -0.5)
                .AddRow("A", "b", 1, 0.5)
                .AddRow("A", "b", 1, -0.5)
                .AddRow("A", "b", 0, 0.5)
                .AddRow("A", "b", 0, -0.5)
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING, STRING, INT32, DOUBLE>()
                         .AddRow("A", "a", 1, -0.5)
                         .AddRow("a", "A", 1, -0.5)
                         .AddRow("A", "b", 1, -0.5)
                         .AddRow("a", "B", 1, -0.5)
                         .AddRow("A", "a", 0, -0.5)
                         .AddRow("a", "A", 0, -0.5)
                         .AddRow("A", "b", 0, -0.5)
                         .AddRow("a", "B", 0, -0.5)
                         .AddRow("A", "a", 1, 0.5)
                         .AddRow("a", "A", 1, 0.5)
                         .AddRow("A", "b", 1, 0.5)
                         .AddRow("a", "B", 1, 0.5)
                         .AddRow("A", "a", 0, 0.5)
                         .AddRow("a", "A", 0, 0.5)
                         .AddRow("A", "b", 0, 0.5)
                         .AddRow("a", "B", 0, 0.5)
                         .Build());
  test.Execute(CreateExtendedSortOperation(
      ExtendedSortSpecificationBuilder().Add("col3", ASCENDING, true)
                                        ->Add("col2", DESCENDING, false)
                                        ->Add("col1", ASCENDING, false)
                                        ->Add("col0", ASCENDING, true)
                                        ->Build(),
      test.input()));
}

TEST_P(SortTest, TransformTest) {
  // Empty input cursor.
  Cursor* input = sample_input_.BuildCursor();
  std::unique_ptr<SortOrder> order(new SortOrder);
  order->add(ProjectNamedAttribute("col0"), ASCENDING);

  std::unique_ptr<const SingleSourceProjector> project_all(
      ProjectAllAttributes());

  std::unique_ptr<Cursor> sort(
      SucceedOrDie(CreateSortCursor(order.get(), project_all.get(), input)));

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  sort->ApplyToChildren(spy_transformer.get());

  ASSERT_EQ(1, spy_transformer->GetHistoryLength());
  EXPECT_EQ(input, spy_transformer->GetEntryAt(0)->original());
}

}  // namespace supersonic
