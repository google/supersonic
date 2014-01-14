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

#include "supersonic/cursor/core/project.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/ownership_taker.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class ProjectCursorTest : public ::testing::Test {};

TEST_F(ProjectCursorTest, ProjectFirstColumnFromInput) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "foo")
                .AddRow(3, "bar")
                .Build());
  test.SetExpectedResult(TestDataBuilder<INT32>()
                         .AddRow(1)
                         .AddRow(3)
                         .Build());
  test.Execute(Project(ProjectNamedAttribute("col0"), test.input()));
}

TEST_F(ProjectCursorTest, ProjectSecondColumnFromInput) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .AddRow(1, "foo")
                .AddRow(3, "bar")
                .Build());
  test.SetExpectedResult(TestDataBuilder<STRING>()
                         .AddRow("foo")
                         .AddRow("bar")
                         .Build());
  test.Execute(Project(ProjectNamedAttribute("col1"), test.input()));
}

TEST_F(ProjectCursorTest, ProjectEmptyInput) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>().Build());
  test.SetExpectedResult(TestDataBuilder<STRING>().Build());
  test.Execute(Project(ProjectNamedAttribute("col1"), test.input()));
}

TEST_F(ProjectCursorTest, ExceptionFromInputPropagated) {
  OperationTest test;
  test.SetInput(TestDataBuilder<INT32, STRING>()
                .ReturnException(ERROR_GENERAL_IO_ERROR)
                .Build());
  test.SetExpectedResult(
      TestDataBuilder<STRING>()
      .ReturnException(ERROR_GENERAL_IO_ERROR)
      .Build());
  test.Execute(Project(ProjectNamedAttribute("col1"), test.input()));
}

TEST_F(ProjectCursorTest, InvalidProjectorSpecification) {
  std::unique_ptr<Operation> input(TestDataBuilder<INT32, STRING>()
                                       .AddRow(1, "foo")
                                       .AddRow(3, "bar")
                                       .Build());
  FailureOrOwned<Cursor> projector(TurnIntoCursor(
      Project(ProjectNamedAttribute("incorrect_name"), input.release())));
  EXPECT_TRUE(projector.is_failure());
}

}  // namespace supersonic
