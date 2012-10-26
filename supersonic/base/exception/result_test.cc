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

#include "supersonic/base/exception/result.h"

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/proto/supersonic.pb.h"
#include "gtest/gtest.h"

namespace supersonic {

class ResultTest : public testing::Test {};
class ResultDeathTest : public testing::Test {};

TEST_F(ResultTest, ResultWorksOnSuccess) {
  FailureOr<int> result = Success(5);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, result.get());
  EXPECT_EQ(5, SucceedOrDie(result));
}

TEST_F(ResultTest, FailureOrReferenceWorksOnSuccess) {
  int a = 5;
  FailureOrReference<int> result = Success(a);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, result.get());
  EXPECT_EQ(5, SucceedOrDie(result));
  a = 7;
  EXPECT_EQ(7, result.get());
}

TEST_F(ResultTest, FailureOrOwnedWorksOnSuccess) {
  int* result_content = new int(5);
  FailureOrOwned<int> result = Success(result_content);
  ASSERT_TRUE(result.is_success());
  EXPECT_FALSE(result.is_failure());
  EXPECT_EQ(5, *result.get());
  EXPECT_EQ(5, *result);
  EXPECT_EQ(result, result_content);
  scoped_ptr<int> not_result_content(new int(5));
  EXPECT_NE(result, not_result_content.get());
}

TEST_F(ResultTest, FailureOrOwnedReleasesResult) {
  scoped_ptr<int> value(SucceedOrDie(FailureOrOwned<int>(Success(new int(7)))));
  EXPECT_EQ(7, *value);
}

template<typename ResultType> void TestThatResultWorksOnFailure() {
  ResultType result = Failure(new Exception(ERROR_GENERAL_IO_ERROR, ""));
  ASSERT_FALSE(result.is_success());
  EXPECT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_GENERAL_IO_ERROR, result.exception().return_code());
}

TEST_F(ResultTest, ResultWorksOnFailure) {
  TestThatResultWorksOnFailure<FailureOr<int> >();
  TestThatResultWorksOnFailure<FailureOrOwned<int> >();
  TestThatResultWorksOnFailure<FailureOrReference<int> >();
  TestThatResultWorksOnFailure<FailureOrVoid>();
}

template<typename ResultType> void TestThatResultReleasesException() {
  ResultType result = Failure(new Exception(ERROR_GENERAL_IO_ERROR, ""));
  ASSERT_TRUE(result.is_failure());
  scoped_ptr<Exception> exception(result.release_exception());
  EXPECT_TRUE(result.is_failure());  // Release doesn't clear the exception.
  EXPECT_EQ(ERROR_GENERAL_IO_ERROR, exception->return_code());
}

TEST_F(ResultTest, ResultReleasesException) {
  TestThatResultReleasesException<FailureOr<int> >();
  TestThatResultReleasesException<FailureOrOwned<int> >();
  TestThatResultReleasesException<FailureOrReference<int> >();
  TestThatResultReleasesException<FailureOrVoid>();
}

}  // namespace supersonic
