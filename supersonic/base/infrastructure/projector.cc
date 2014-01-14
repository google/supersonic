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

#include "supersonic/base/infrastructure/projector.h"

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

bool BoundMultiSourceProjector::AddAs(int source_index,
                                      int attribute_position,
                                      const StringPiece& alias) {
  CHECK_LT(source_index, source_count());
  CHECK_LT(attribute_position, source_schema(source_index).attribute_count());
  const Attribute& source_attribute =
      source_schema(source_index).attribute(attribute_position);
  string attribute_name(alias.data(), alias.size());
  if (attribute_name.empty()) {
    attribute_name = source_attribute.name();
  }
  Attribute attribute(attribute_name,
                      source_attribute.type(),
                      source_attribute.nullability());
  if (!result_schema_.add_attribute(attribute)) {
    return false;
  }
  const SourceAttribute projected_attribute =
      SourceAttribute(source_index, attribute_position);
  reverse_projection_map_.insert(
      make_pair(projected_attribute, projection_map_.size()));
  projection_map_.push_back(projected_attribute);
  return true;
}

pair<PositionIterator, PositionIterator>
BoundMultiSourceProjector::ProjectedAttributePositions(
    int source_index, int attribute_position) const {
  const pair<ReverseProjectionMap::const_iterator,
             ReverseProjectionMap::const_iterator> it =
      reverse_projection_map_.equal_range(
          SourceAttribute(source_index, attribute_position));
  return make_pair(it.first, it.second);
}

bool BoundMultiSourceProjector::IsAttributeProjected(
    int source_index,
    int attribute_position) const {
  const ReverseProjectionMap::const_iterator it =
      reverse_projection_map_.find(
          SourceAttribute(source_index, attribute_position));
  return it != reverse_projection_map_.end();
}

int BoundMultiSourceProjector::NumberOfProjectionsForAttribute(
    int source_index,
    int attribute_position) const {
  return reverse_projection_map_.count(SourceAttribute(source_index,
                                                       attribute_position));
}

BoundSingleSourceProjector BoundMultiSourceProjector::GetSingleSourceProjector(
    int source_index) const {
  BoundSingleSourceProjector result(source_schema(source_index));
  for (int i = 0; i < result_schema_.attribute_count(); ++i) {
    if (this->source_index(i) == source_index) {
      result.AddAs(
          source_attribute_position(i), result_schema().attribute(i).name());
    }
  }
  return result;
}

NamedAttributeProjector::NamedAttributeProjector(const StringPiece& name)
    : name_(name.data(), name.size()) {}

FailureOrOwned<const BoundSingleSourceProjector> NamedAttributeProjector::Bind(
    const TupleSchema& source_schema) const {
  int source_position = source_schema.LookupAttributePosition(name_);
  if (source_position < 0) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_MISSING,
        StringPrintf("No attribute '%s' in the schema:\n '%s'", name_.c_str(),
            source_schema.GetHumanReadableSpecification().c_str())));
  } else {
    scoped_ptr<BoundSingleSourceProjector> projector(
        new BoundSingleSourceProjector(source_schema));
    CHECK(projector->Add(source_position));
    return Success(projector.release());
  }
}

namespace {

class RenamingProjector : public SingleSourceProjector {
 public:
  explicit RenamingProjector(const vector<string>& aliases,
                             const SingleSourceProjector* source)
      : aliases_(aliases),
        source_(source) {
    set<string> unique(aliases.begin(), aliases.end());
    CHECK_EQ(aliases.size(), unique.size())
        << "The provided list of aliases isn't unique: " << strings::Join(
                                                                aliases, ", ");
  }

  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& input_schema) const {
    FailureOrOwned<const BoundSingleSourceProjector> bound =
        source_->Bind(input_schema);
    PROPAGATE_ON_FAILURE(bound);
    const TupleSchema& intermediate_schema = bound->result_schema();
    if (aliases_.size() != intermediate_schema.attribute_count()) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_COUNT_MISMATCH,
          StringPrintf(
              "Number of aliases (%zu) does not match "
              "the attribute count in source schema (%d): %s",
              aliases_.size(),
              intermediate_schema.attribute_count(),
              intermediate_schema.GetHumanReadableSpecification().c_str())));
    }
    // Create a new projector, and copy all attributes, replacing the names.
    scoped_ptr<BoundSingleSourceProjector> result_projector(
        new BoundSingleSourceProjector(input_schema));
    for (int i = 0; i < intermediate_schema.attribute_count(); ++i) {
      CHECK(result_projector->AddAs(bound->source_attribute_position(i),
                                    aliases_[i]));
    }
    return Success(result_projector.release());
  }

  virtual RenamingProjector* Clone() const {
    return new RenamingProjector(aliases_, source_->Clone());
  }

  // (result_projection) RENAME AS (name1, name2, name3)
  virtual string ToString(bool verbose) const;

 private:
  const vector<string> aliases_;
  const scoped_ptr<const SingleSourceProjector> source_;
};

string RenamingProjector::ToString(bool verbose) const {
  string result_description = StringPrintf("(%s) RENAME AS (",
                                           source_->ToString(verbose).c_str());
  for (vector<string>::const_iterator it = aliases_.begin();
      it < aliases_.end(); ++it) {
    if (it != aliases_.begin()) result_description.append(", ");
    result_description.append(*it);
  }
  result_description.append(")");
  return result_description;
}

class PositionedAttributeProjector : public SingleSourceProjector {
 public:
  explicit PositionedAttributeProjector(const int source_position)
      : source_position_(source_position) {}

  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& source_schema) const {
    if (source_position_ >= source_schema.attribute_count()) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_COUNT_MISMATCH,
          StringPrintf("source schema has too few attributes (%d vs %d)",
                       source_schema.attribute_count(),
                       source_position_)));
    }
    scoped_ptr<BoundSingleSourceProjector> projector(
        new BoundSingleSourceProjector(source_schema));
    CHECK(projector->Add(source_position_));
    return Success(projector.release());
  }

  virtual PositionedAttributeProjector* Clone() const {
    return new PositionedAttributeProjector(source_position_);
  }

  virtual string ToString(bool verbose) const {
    return StringPrintf("AttributeAt(%d)", source_position_);
  }

 private:
  int source_position_;
  DISALLOW_COPY_AND_ASSIGN(PositionedAttributeProjector);
};

class AllAttributesProjector : public SingleSourceProjector {
 public:
  AllAttributesProjector() {}
  explicit AllAttributesProjector(const StringPiece& prefix) :
      prefix_(prefix.ToString()) {}

  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& source_schema) const {
    BoundSingleSourceProjector* result =
        new BoundSingleSourceProjector(source_schema);
    for (int i = 0; i < source_schema.attribute_count(); ++i) {
      if (prefix_.empty()) {
        result->Add(i);
      } else {
        const string prefixed_name =
            prefix_ + source_schema.attribute(i).name();
        result->AddAs(i, prefixed_name);
      }
    }
    return Success(result);
  }

  virtual AllAttributesProjector* Clone() const {
    return new AllAttributesProjector(prefix_);
  }

  virtual string ToString(bool verbose) const {
    return StrCat(prefix_, "*");
  }

 private:
  string prefix_;
};

}  // namespace

FailureOrOwned<const BoundSingleSourceProjector>
CompoundSingleSourceProjector::Bind(
    const TupleSchema& source_schema) const {
  scoped_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(source_schema));
  for (int i = 0; i < projectors_.size(); ++i) {
    FailureOrOwned<const BoundSingleSourceProjector> component =
        projectors_[i]->Bind(source_schema);
    PROPAGATE_ON_FAILURE(component);
    for (int j = 0; j < component->result_schema().attribute_count();
         ++j) {
      if (!projector->AddAs(
              component->source_attribute_position(j),
              component->result_schema().attribute(j).name())) {
        THROW(new Exception(
            ERROR_ATTRIBUTE_EXISTS,
            StringPrintf(
                "Duplicate attribute name \"%s\" in result schema: %s",
                component->result_schema().attribute(j).name().c_str(),
                component->result_schema().
                    GetHumanReadableSpecification().c_str())));
      }
    }
  }
  return Success(projector.release());
}

CompoundSingleSourceProjector* CompoundSingleSourceProjector::Clone() const {
  CompoundSingleSourceProjector* clone = new CompoundSingleSourceProjector();
  for (vector<linked_ptr<const SingleSourceProjector> >::const_iterator i =
       projectors_.begin(); i != projectors_.end(); ++i) {
    clone->add((*i)->Clone());
  }
  return clone;
}

string CompoundSingleSourceProjector::ToString(bool verbose) const {
  string result_description = "(";
  for (vector<linked_ptr<const SingleSourceProjector> >::const_iterator it
          = projectors_.begin(); it < projectors_.end(); ++it) {
    if (it != projectors_.begin()) result_description.append(", ");
    result_description.append((*it)->ToString(verbose));
  }
  result_description.append(")");
  return result_description;
}

const SingleSourceProjector* ProjectRename(
    const vector<string>& aliases,
    const SingleSourceProjector* source) {
  return new RenamingProjector(aliases, source);
}

const SingleSourceProjector* ProjectNamedAttribute(const StringPiece& name) {
  return new NamedAttributeProjector(name);
}

const SingleSourceProjector* ProjectAttributeAt(const int position) {
  return new PositionedAttributeProjector(position);
}

const SingleSourceProjector* ProjectAttributesAt(const vector<int>& positions) {
  scoped_ptr<CompoundSingleSourceProjector> projector(
      new CompoundSingleSourceProjector);
  for (int i = 0; i < positions.size(); ++i) {
    projector->add(ProjectAttributeAt(positions[i]));
  }
  return projector.release();
}

const SingleSourceProjector* ProjectNamedAttributes(
    const vector<string>& names) {
  CompoundSingleSourceProjector* projector = new CompoundSingleSourceProjector;
  for (int i = 0; i < names.size(); ++i) {
    projector->add(ProjectNamedAttribute(names[i]));
  }
  return projector;
}

const SingleSourceProjector* ProjectAllAttributes() {
  return new AllAttributesProjector();
}

const SingleSourceProjector* ProjectAllAttributes(const StringPiece& prefix) {
  return new AllAttributesProjector(prefix);
}

FailureOrOwned<const BoundMultiSourceProjector>
CompoundMultiSourceProjector::Bind(
    const vector<const TupleSchema*>& source_schemas) const {
  scoped_ptr<BoundMultiSourceProjector> projector(
      new BoundMultiSourceProjector(source_schemas));
  for (int i = 0; i < projectors_.size(); ++i) {
    FailureOrOwned<const BoundSingleSourceProjector> component =
        projectors_[i].second->Bind(*source_schemas[projectors_[i].first]);
    PROPAGATE_ON_FAILURE(component);

    for (int j = 0; j < component->result_schema().attribute_count();
         ++j) {
      if (!projector->AddAs(
              projectors_[i].first,
              component->source_attribute_position(j),
              component->result_schema().attribute(j).name())) {
        THROW(new Exception(
            ERROR_ATTRIBUTE_EXISTS,
            StringPrintf(
                "Duplicate attribute name \"%s\" in result schema: %s",
                component->result_schema().attribute(j).name().c_str(),
                component->result_schema().
                    GetHumanReadableSpecification().c_str())));
      }
    }
  }

  return Success(projector.release());
}

string CompoundMultiSourceProjector::ToString(bool verbose) const {
  vector<string> projectors_str(projectors_.size());
  for (int i = 0; i < projectors_.size(); i++) {
    projectors_str[i] = StrCat(
        projectors_[i].first, ": ", projectors_[i].second->ToString(verbose));
  }
  return strings::Join(projectors_str, ", ");
}

pair<BoundMultiSourceProjector*, BoundSingleSourceProjector*> DecomposeNth(
    int source_index,
    const BoundMultiSourceProjector& projector) {
  scoped_ptr<BoundSingleSourceProjector> new_nth(
      new BoundSingleSourceProjector(projector.source_schema(source_index)));
  vector<const TupleSchema*> schemas;
  for (int i = 0; i < projector.source_count(); ++i) {
    schemas.push_back(&projector.source_schema(i));
  }
  scoped_ptr<BoundMultiSourceProjector> new_projector(
      new BoundMultiSourceProjector(schemas));

  std::map<int, int> uniqualizer;
  for (int i = 0; i < projector.result_schema().attribute_count(); ++i) {
    int source_attribute_position = projector.source_attribute_position(i);
    const string& alias = projector.result_schema().attribute(i).name();
    if (projector.source_index(i) != source_index) {  // Leave unchanged.
      new_projector->AddAs(projector.source_index(i),
                           source_attribute_position, alias);
    } else {
      // Decompose.
      int position;
      if (uniqualizer.find(source_attribute_position) != uniqualizer.end()) {
        // Found one already projected; reuse.
        position = uniqualizer[source_attribute_position];
      } else {
        position = new_nth->result_schema().attribute_count();
        uniqualizer[source_attribute_position] = position;
        new_nth->Add(source_attribute_position);
      }
      new_projector->AddAs(source_index, position, alias);
    }
  }
  return std::make_pair(new_projector.release(), new_nth.release());
}

}  // namespace supersonic
