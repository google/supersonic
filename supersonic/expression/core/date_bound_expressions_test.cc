// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: ptab@google.com (Piotr Tabor)

#include "supersonic/expression/core/date_bound_expressions.h"

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class BufferAllocator;

namespace {

using util::gtl::Container;

// TODO(ptab): Add core DateBoundExpressions tests.

TEST(DateBoundExpressionsTest, BoundMakeDatetimeCollectReferredAttributeNames) {
  TupleSchema schema;
  schema.add_attribute(Attribute("year", INT64, NULLABLE));
  schema.add_attribute(Attribute("month", INT64, NULLABLE));
  schema.add_attribute(Attribute("day", INT64, NULLABLE));
  schema.add_attribute(Attribute("hour", INT64, NULLABLE));
  schema.add_attribute(Attribute("minute", INT64, NULLABLE));
  schema.add_attribute(Attribute("second", INT64, NULLABLE));
  FailureOrOwned<BoundExpression> make_datetime = BoundMakeDatetime(
      SucceedOrDie(BoundNamedAttribute(schema, "year")),
      SucceedOrDie(BoundNamedAttribute(schema, "month")),
      SucceedOrDie(BoundNamedAttribute(schema, "day")),
      SucceedOrDie(BoundNamedAttribute(schema, "hour")),
      SucceedOrDie(BoundNamedAttribute(schema, "minute")),
      SucceedOrDie(BoundNamedAttribute(schema, "second")),
      HeapBufferAllocator::Get(), 20);
  ASSERT_TRUE(make_datetime.is_success())
      << make_datetime.exception().PrintStackTrace();
  EXPECT_EQ(Container("year", "month", "day", "hour", "minute", "second")
                .As<set<string> >(),
            make_datetime->referred_attribute_names());
}

}  // namespace

}  // namespace supersonic
