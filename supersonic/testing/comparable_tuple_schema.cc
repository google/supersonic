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

#include "supersonic/testing/comparable_tuple_schema.h"

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

void ComparableTupleSchema::AppendToStream(std::ostream *s) const {
  view_printer_.AppendSchemaToStream(schema_, s);
}

testing::AssertionResult ComparableTupleSchema::Compare(
    const ComparableTupleSchema& other) const {
  // Compare counts.
  if (schema_.attribute_count() != other.schema_.attribute_count()) {
    return testing::AssertionFailure()
        << "attribute count mismatch: "
        << schema_.attribute_count() << " vs "
        << other.schema_.attribute_count();
  }

  // Compare types.
  for (size_t i = 0; i < schema_.attribute_count(); i++) {
    if (schema_.attribute(i).type() != other.schema_.attribute(i).type()) {
      return testing::AssertionFailure()
          << "attribute #" << i << " type mismatch: "
          << GetTypeInfo(schema_.attribute(i).type()).name() << " vs "
          << GetTypeInfo(other.schema_.attribute(i).type()).name();
    }
    if (schema_.attribute(i).type() == ENUM) {
      // compare enum values.
      FailureOrVoid enum_equals = EnumDefinition::VerifyEquals(
          other.schema_.attribute(i).enum_definition(),
          schema_.attribute(i).enum_definition());
      if (enum_equals.is_failure()) {
        return testing::AssertionFailure()
            << enum_equals.exception().ToString();
      }
    }
  }

  return testing::AssertionSuccess();
}


testing::AssertionResult ComparableTupleSchema::CompareStrict(
    const ComparableTupleSchema& other) const {
  testing::AssertionResult result = Compare(other);
  if (!result) {
    return result;
  }

  // Additionally compare names and nullability.
  for (size_t i = 0; i < schema_.attribute_count(); i++) {
    if (schema_.attribute(i).name() != other.schema_.attribute(i).name()) {
      return testing::AssertionFailure()
          << "attribute #" << i << " name mismatch: "
          << schema_.attribute(i).name() << " vs "
          << other.schema_.attribute(i).name();
    }
    if (schema_.attribute(i).is_nullable() !=
            other.schema_.attribute(i).is_nullable()) {
      return testing::AssertionFailure()
          << "attribute #" << i << " nullability mismatch: "
          << schema_.attribute(i).is_nullable() << " vs "
          << other.schema_.attribute(i).is_nullable();
    }
  }
  return testing::AssertionSuccess();
}


testing::AssertionResult ComparableTupleSchema::operator==(
    const ComparableTupleSchema& other) const {
  return CompareStrict(other);
}

testing::AssertionResult TupleSchemasEqual(
    const char* a_str, const char* b_str,
    const TupleSchema& a, const TupleSchema& b) {
  ComparableTupleSchema a_comparable(a);
  ComparableTupleSchema b_comparable(b);
  testing::AssertionResult result = a_comparable.Compare(b_comparable);
  if (!result) {
    result << std::endl
           << "in TupleSchemasEqual(" << a_str << ", " << b_str << ")"
        << std::endl << a_str << " = " << a_comparable
        << std::endl << b_str << " = " << b_comparable;
  }
  return result;
}

testing::AssertionResult TupleSchemasStrictEqual(
    const char* a_str, const char* b_str,
    const TupleSchema& a, const TupleSchema& b) {
  ComparableTupleSchema a_comparable(a);
  ComparableTupleSchema b_comparable(b);
  testing::AssertionResult result = a_comparable.CompareStrict(b_comparable);
  if (!result) {
    result << std::endl
        << "in TupleSchemasStrictEqual(" << a_str << ", " << b_str << ")"
        << std::endl << a_str << " = " << a_comparable
        << std::endl << b_str << " = " << b_comparable;
  }
  return result;
}


}  // namespace supersonic
