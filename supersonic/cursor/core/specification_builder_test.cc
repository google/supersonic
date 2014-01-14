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

#include "supersonic/cursor/core/specification_builder.h"

#include <memory>

#include "supersonic/proto/specification.pb.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"
#include "supersonic/utils/random.h"

namespace supersonic {

namespace {

int keys[] = {0, 1, 2, 5};
int keys_count = 4;
bool contains_limit[] = {false, true};

// This test works by generating several messages:
// a) Iterate over keys to get the number of keys in the message. The parameters
// of the key are completely randomized.
// b) Iterate over {true, false} which determines whether limit is set or not.
// c) Build the message using builder and also using normal message. Then,
// assert they're equal.
TEST(SpecificationBuilderTest, ExtendedSortSpecificationTest) {
  MTRandom random_generator(290890);
  for (int i = 0; i < keys_count; ++i) {
    for (int j = 0; j < 2; ++j) {
      int key = keys[i];
      ExtendedSortSpecificationBuilder builder;
      ExtendedSortSpecification expected;
      if (contains_limit[j]) {
        int limit = abs(random_generator.Next());
        builder.SetLimit(limit);
        expected.set_limit(limit);
      }
      for (int k = 0; k < key; ++k) {
        ColumnOrder order = ASCENDING;
        if (random_generator.Next() % 2 == 1) {
          order = DESCENDING;
        }
        bool case_sensitive = false;
        if (random_generator.Next() % 2 == 1) {
          case_sensitive = true;
        }
        string path = "irvan";
        if (k % 2 == 1) {
          path = "awesome";
        }
        builder.Add(path, order, case_sensitive);
        ExtendedSortSpecification::Key* new_key = expected.add_keys();
        new_key->set_case_sensitive(case_sensitive);
        new_key->set_column_order(order);
        new_key->set_attribute_name(path);
      }
      std::unique_ptr<ExtendedSortSpecification> actual(builder.Build());
      ASSERT_PROTO_EQ(expected, *actual);
    }
  }
}

}  // namespace

}  // namespace supersonic

