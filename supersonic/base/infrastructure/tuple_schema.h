// Copyright 2008 Google Inc.  All Rights Reserved
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


#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_TUPLE_SCHEMA_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_TUPLE_SCHEMA_H_

#include <map>
using std::map;
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/proto/supersonic.pb.h"
#include <memory>

namespace supersonic {

class EnumDefinition {
 public:
  EnumDefinition();
  FailureOrVoid AddEntry(const int32 number, StringPiece name);
  FailureOr<StringPiece> NumberToName(int32 number) const;
  FailureOr<int32> NameToNumber(StringPiece name) const;
  size_t entry_count() const;
  static FailureOrVoid VerifyEquals(const EnumDefinition& a,
                                    const EnumDefinition& b);

 private:
  class Rep {
   public:
    explicit Rep(BufferAllocator* buffer_allocator);
    void CopyFrom(const Rep& other);
    FailureOrVoid Add(const int32 number, StringPiece name);
    FailureOr<StringPiece> NumberToName(int32 number) const;
    FailureOr<int32> NameToNumber(StringPiece name) const;
    size_t entry_count() const { return name_to_number_.size(); }
    BufferAllocator* buffer_allocator() const { return buffer_allocator_; }

    static FailureOrVoid VerifyEquals(const Rep& a, const Rep& b);

   private:
    BufferAllocator* const buffer_allocator_;
    Arena arena_;
    map<StringPiece, int32> name_to_number_;
    map<int32, StringPiece> number_to_name_;
    DISALLOW_COPY_AND_ASSIGN(Rep);
  };
  shared_ptr<Rep> rep_;
  // Copyable. Use value semantics.
};

// Describes a single attribute in a tuple (its name and type).
class Attribute {
 public:
  // Creates an attribute with specified options. For ENUM attributes, you
  // probably want to use the other constructor instead, as this one would
  // create empty ENUM.
  Attribute(const string& name,
            const DataType type,
            const Nullability nullability)
      : name_(name),
        type_(type),
        nullability_(nullability),
        enum_definition_() {}

  // Creates an ENUM attribute with the specified definition.
  Attribute(const string& name,
            const EnumDefinition enum_definition,
            const Nullability nullability)
      : name_(name),
        type_(ENUM),
        nullability_(nullability),
        enum_definition_(enum_definition) {}

  ~Attribute();

  const string& name() const { return name_; }
  const DataType type() const { return type_; }
  const Nullability nullability() const { return nullability_; }
  // Tells whether this attribute can can have null values.
  const bool is_nullable() const { return nullability_ == NULLABLE; }
  EnumDefinition enum_definition() const {
    DCHECK_EQ(ENUM, type_);
    return enum_definition_;
  }

 private:
  string name_;
  DataType type_;
  Nullability nullability_;
  // Empty, lightweight (just an empty shared_ptr) for non-ENUM types.
  EnumDefinition enum_definition_;
};

// Represents the schema for a tuple, which is an ordered list of named and
// typed values (e.g. a single row in a table/cursor). The tuple schema
// lists types and names of tuple elements, called 'attributes'.
//
// The schema is often used in objects passed by value, e.g. comparators used
// for table sorting; therefore, it needs to support copy construction and
// assignment.
class TupleSchema {
 private:
  class Rep {
   public:
    // Creates a new, empty schema (with no attributes).
    Rep() {}

    // A copy constructor.
    Rep(const Rep& other)
        : attributes_(other.attributes_),
          attribute_names_(other.attribute_names_) {}

    int attribute_count() const { return attributes_.size(); }

    const Attribute& attribute(const int position) const {
      DCHECK_GE(position, 0);
      DCHECK_LT(position, attribute_count());
      return attributes_[position];
    }

    bool add_attribute(const Attribute& attribute) {
      const bool inserted = attribute_names_.insert(
          make_pair(attribute.name(), attributes_.size())).second;
      if (inserted) {
        attributes_.push_back(attribute);
        return true;
      } else {
        return false;
      }
    };

    const Attribute& LookupAttribute(const string& name) const {
      int position = LookupAttributePosition(name);
      CHECK_GE(position, 0) << "Cannot LookupAttribute '" << name
                            << "' in schema "
                            << GetHumanReadableSpecification();
      CHECK_LT(position, attribute_count());
      return attributes_[position];
    }

    int LookupAttributePosition(const string& attribute_name) const {
      map<string, int>::const_iterator i = attribute_names_.find(
          attribute_name);
      return (i == attribute_names_.end()) ? -1 : i->second;
    }

    string GetHumanReadableSpecification() const;

   private:
    vector<Attribute> attributes_;
    map<string, int> attribute_names_;
  };

 public:
  // Creates a new, empty schema (with no attributes).
  TupleSchema() : rep_(new Rep()) {}

  // A copy constructor.
  TupleSchema(const TupleSchema& other) : rep_(other.rep_) {}

  // Returns the number of attributes.
  int attribute_count() const { return rep_->attribute_count(); }

  // Returns the attribute at the specified position.
  const Attribute& attribute(const int position) const {
    return rep_->attribute(position);
  }

  // Adds an attribute to the schema if it isn't already defined. The added
  // attribute occupies the last position (positions of all existing attributes
  // do not change). Returns true iff name has been successfully added.
  bool add_attribute(const Attribute& attribute) {
    if (!rep_.unique()) {
      rep_.reset(new Rep(*rep_));
    }
    return rep_->add_attribute(attribute);
  };

  // Looks up an attribute with the specified name. O(log2(attribute count)).
  // The attribute must exist in the schema.
  const Attribute& LookupAttribute(const string& name) const {
    return rep_->LookupAttribute(name);
  }

  // Checks whether two schemas are equal, i.e. they have attributes of the
  // same types and (if check_names is set to true) names on the same positions.
  static bool AreEqual(const TupleSchema& a,
                       const TupleSchema& b,
                       bool check_names) {
    if (a.rep_.get() == b.rep_.get()) return true;
    if (a.attribute_count() != b.attribute_count()) {
      return false;
    }
    for (int i = 0; i < a.attribute_count(); i++) {
      const Attribute& a_attr = a.attribute(i);
      const Attribute& b_attr = b.attribute(i);
      if (a_attr.type() != b_attr.type()) {
        return false;
      }
      if (a_attr.type() == ENUM) {
        FailureOrVoid enums_equal = EnumDefinition::VerifyEquals(
            a_attr.enum_definition(), b_attr.enum_definition());
        if (enums_equal.is_failure()) {
          return false;
        }
      }
      if (a_attr.is_nullable() != b_attr.is_nullable() ||
          (check_names && (a_attr.name() != b_attr.name()))) {
        return false;
      }
    }
    return true;
  }

  // Returns true if two schemas can be merged (they do not contain attributes
  // with the same name).
  static bool CanMerge(const TupleSchema& a, const TupleSchema& b);

  // TODO(user): Remove this method and replace its uses with TryMerge.
  // Merges two TupleSchema into a new TupleSchema object. It is safe to call
  // this method only if CanMerge for the two schemas returns true.
  static TupleSchema Merge(const TupleSchema& a, const TupleSchema& b);

  // Like Merge, but when the schemas can't be merged, returns a failure instead
  // of failing a CHECK.
  static FailureOr<TupleSchema> TryMerge(const TupleSchema& a,
                                         const TupleSchema& b);

  // Looks up and returns the position of an attribute with the specified
  // name, or -1 if not found in the schema. O(log2(attribute count)).
  int LookupAttributePosition(const string& attribute_name) const {
    return rep_->LookupAttributePosition(attribute_name);
  }

  // Creates and returns a schema with a single attribute.
  static TupleSchema Singleton(const string& name,
                               const DataType type,
                               Nullability nullability) {
    TupleSchema result;
    Attribute attribute(name, type, nullability);
    CHECK(result.add_attribute(attribute));
    return result;
  }

  // Equality tester for CHECKs and tests.
  // TODO(user): clarify relationship to AreEqual (above).
  bool EqualByType(const TupleSchema& other) const;

  // Convenience function that returns a schema specification formatted
  // in the human-readable representation:
  // <name>: <type> [, <name>: <type>]*
  // For example: "a: STRING, b: INT32"
  // Note: for logging only. Prone to ambiguities in corner cases, e.g.
  // attribute names with non-ASCII characters, or ':'.
  string GetHumanReadableSpecification() const {
    return rep_->GetHumanReadableSpecification();
  }

 private:
  shared_ptr<Rep> rep_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_TUPLE_SCHEMA_H_
