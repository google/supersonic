// Copyright 2012 Google Inc.  All Rights Reserved
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
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"

#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace supersonic {

namespace {

TEST(BenchmarkListenerTest, BenchmarkValueInitTest) {
  std::unique_ptr<BenchmarkListener> listener(CreateBenchmarkListener());
  EXPECT_EQ(0, listener->TotalTimeUsec());
  EXPECT_EQ(0, listener->FirstNextTimeUsec());
  EXPECT_EQ(0, listener->NextCalls());
  EXPECT_EQ(0, listener->RowsProcessed());
}

}  // namespace

}  // namespace supersonic
