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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/core/regexp_bound_expressions.h"

#include "supersonic/expression/base/expression.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class BufferAllocator;

namespace {

using util::gtl::Container;

FailureOrOwned<BoundExpression> FullRegexpPatterned(BoundExpression* arg,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count) {
  return BoundRegexpFullMatch(arg, "X", allocator, max_row_count);
}

FailureOrOwned<BoundExpression> PartialRegexpPatterned(
    BoundExpression* arg,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundRegexpPartialMatch(arg, "_", allocator, max_row_count);
}

FailureOrOwned<BoundExpression> RegexpExtractPatterned(
    BoundExpression* arg,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundRegexpExtract(arg, "X", allocator, max_row_count);
}

FailureOrOwned<BoundExpression> RegexpReplacePatterned(
    BoundExpression* haystack,
    BoundExpression* substitute,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundRegexpReplace(haystack, "X", substitute, allocator,
                            max_row_count);
}

TEST(StringBoundExpressionsTest, BoundRegexpFull) {
  TestBoundUnary(&FullRegexpPatterned, STRING, "REGEXP_FULL_MATCH($0)");
}

TEST(StringBoundExpressionsTest, BoundRegexpPartial) {
  TestBoundUnary(&PartialRegexpPatterned, STRING, "REGEXP_PARTIAL_MATCH($0)");
}

TEST(StringBoundExpressionsTest, BoundRegexpExtract) {
  TestBoundUnary(&RegexpExtractPatterned, STRING, "REGEXP_EXTRACT($0)");
}

TEST(StringBoundExpressionsTest, BoundRegexpReplace) {
  TestBoundBinary(&RegexpReplacePatterned, STRING, STRING,
                  "REGEXP_REPLACE($0, $1)");
}

}  // namespace

}  // namespace supersonic
