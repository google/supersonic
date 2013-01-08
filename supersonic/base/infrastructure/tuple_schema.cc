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

#include "supersonic/base/infrastructure/tuple_schema.h"

namespace supersonic {

bool TupleSchema::CanMerge(const TupleSchema& a, const TupleSchema& b) {
  for (int i = 0; i < a.attribute_count(); ++i) {
    if (b.LookupAttributePosition(a.attribute(i).name()) >= 0) {
      return false;
    }
  }
  for (int i = 0; i < b.attribute_count(); i++) {
    if (a.LookupAttributePosition(b.attribute(i).name()) >= 0) {
      return false;
    }
  }
  return true;
}

TupleSchema TupleSchema::Merge(const TupleSchema& a, const TupleSchema& b) {
  FailureOr<TupleSchema> result = TryMerge(a, b);
  CHECK(result.is_success())
      << "TupleSchema::Merge failed, "
      << result.exception().PrintStackTrace();
  return result.get();
}

FailureOr<TupleSchema> TupleSchema::TryMerge(const TupleSchema& a,
                                             const TupleSchema& b) {
  TupleSchema result(a);
  for (int i = 0; i < b.attribute_count(); ++i) {
    if (!result.add_attribute(b.attribute(i))) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_EXISTS,
          StrCat("Can't merge schemas, ambiguous attribute name: ",
                 b.attribute(i).name())));
    }
  }
  return Success(result);
}

bool TupleSchema::EqualByType(const TupleSchema& other) const {
  if (rep_.get() == other.rep_.get()) return true;
  if (attribute_count() != other.attribute_count())
    return false;
  for (int i = 0; i < attribute_count(); ++i) {
    if (attribute(i).type() != other.attribute(i).type())
      return false;
  }
  return true;
}

string TupleSchema::Rep::GetHumanReadableSpecification() const {
  string result;
  for (int i = 0; i < attribute_count(); ++i) {
    if (i > 0) result += ", ";
    result += attribute(i).name();
    result += ": ";
    result += GetTypeInfo(attribute(i).type()).name();
    if (!attribute(i).is_nullable()) result += " NOT NULL";
  }
  return result;
}

}  // namespace supersonic
