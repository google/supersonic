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

#include "supersonic/cursor/core/compute.h"

#include "supersonic/expression/core/arithmetic_expressions.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class Operation;

class ComputeTest : public testing::Test {
 public:
  Operation* test_input() const {
    return TestDataBuilder<STRING, INT32, DOUBLE, INT64 >()
            .AddRow("1", 12, 5.0, 5)
            .AddRow("2", 13, 6.0, 6)
            .AddRow(__,  __, __, __)
            .Build();
  }
};

TEST_F(ComputeTest, EmptyShouldBeEmpty) {
  OperationTest test;
  test.SetInput(test_input());
  test.SetExpectedResult(TestDataBuilder<UNDEF>()
                         .AddRow()
                         .AddRow()
                         .AddRow()
                         .Build());
  test.Execute(Compute(new CompoundExpression(), test.input()));
}

TEST_F(ComputeTest, NamedAttribute) {
  OperationTest test;
  test.SetInput(test_input());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(12)
                         .AddRow(13)
                         .AddRow(__)
                         .Build());
  test.Execute(Compute(
      (new CompoundExpression())->AddAs("col0", NamedAttribute("col1")),
      test.input()));
}

TEST_F(ComputeTest, CompoundWithArithmetics) {
  OperationTest test;
  test.SetInput(test_input());
  test.SetExpectedResult(TestDataBuilder<INT32, INT64>()
                         .AddRow(12, 17)
                         .AddRow(13, 19)
                         .AddRow(__, __)
                         .Build());
  test.Execute(Compute(
      (new CompoundExpression())
          ->AddAs("col0", NamedAttribute("col1"))
          ->AddAs("col1", Plus(
              NamedAttribute("col1"),
              NamedAttribute("col3"))),
      test.input()));
}

}  // namespace supersonic
