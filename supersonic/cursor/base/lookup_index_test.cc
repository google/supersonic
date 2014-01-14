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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/cursor/base/lookup_index.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/proto/supersonic.pb.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

class MockLookupIndex : public LookupIndex {
 public:
  // Takes ownership of projector.
  MockLookupIndex(const TupleSchema& schema,
                  BoundSingleSourceProjector* key_selector)
      : schema_(schema),
        key_selector_(key_selector) {}

  virtual FailureOrOwned<LookupIndexCursor> MultiLookup(
      const View* query) const {
    THROW(new Exception(ERROR_UNKNOWN_ERROR, "Mock error"));
  }

  virtual const TupleSchema& schema() const { return schema_; }

  virtual BoundSingleSourceProjector& key_selector() const {
    return *key_selector_.get();
  }

  virtual bool empty() const { return false; }

 private:
  TupleSchema schema_;
  std::unique_ptr<BoundSingleSourceProjector> key_selector_;
};

TEST(LookupIndexTest, ValueSelectorBasic) {
  TupleSchema schema;
  schema.add_attribute(Attribute("col1", INT32, NULLABLE));
  schema.add_attribute(Attribute("col2", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("col3", STRING, NULLABLE));
  schema.add_attribute(Attribute("col4", DATE, NOT_NULLABLE));

  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(schema));

  ASSERT_TRUE(projector->Add(1));
  ASSERT_TRUE(projector->Add(3));

  MockLookupIndex index(schema, projector.release());
  const BoundSingleSourceProjector& values = index.value_selector();

  EXPECT_EQ(0, values.source_attribute_position(0));
  EXPECT_FALSE(values.IsAttributeProjected(1));
  EXPECT_EQ(2, values.source_attribute_position(1));
  EXPECT_FALSE(values.IsAttributeProjected(3));
  EXPECT_EQ(2, values.result_schema().attribute_count());
}

TEST(LookupIndexTest, ValueSelectorNoKey) {
  TupleSchema schema;
  schema.add_attribute(Attribute("col1", INT32, NOT_NULLABLE));

  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(schema));

  MockLookupIndex index(schema, projector.release());
  const BoundSingleSourceProjector& values = index.value_selector();

  EXPECT_EQ(0, values.source_attribute_position(0));
  EXPECT_EQ(1, values.result_schema().attribute_count());
}

TEST(LookupIndexTest, ValueSelectorNoValues) {
  TupleSchema schema;
  schema.add_attribute(Attribute("col1", INT32, NOT_NULLABLE));

  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(schema));
  ASSERT_TRUE(projector->Add(0));

  MockLookupIndex index(schema, projector.release());
  const BoundSingleSourceProjector& values = index.value_selector();

  EXPECT_EQ(0, values.result_schema().attribute_count());
}

TEST(LookupIndexTest, ValueSelectorNoSchema) {
  TupleSchema schema;

  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(schema));

  MockLookupIndex index(schema, projector.release());
  const BoundSingleSourceProjector& values = index.value_selector();

  EXPECT_EQ(0, values.result_schema().attribute_count());
}

}  // namespace
}  // namespace supersonic
