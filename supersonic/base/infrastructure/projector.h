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

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_PROJECTOR_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_PROJECTOR_H_

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
#include "supersonic/utils/macros.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/iterator_adaptors.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

class BoundSingleSourceProjector;

struct SourceAttribute {
  SourceAttribute(int source_index, int attribute_position)
      : source_index(source_index),
        attribute_position(attribute_position) {}

  // Needed by multimap<SourceAttribute, ...>.
  bool operator<(const SourceAttribute& other) const {
    return source_index < other.source_index ||
        (source_index == other.source_index &&
         attribute_position < other.attribute_position);
  }

  int source_index;
  int attribute_position;
};

// Mapping of attributes in source(s) onto their position in projection's result
// schema. Should probably be called ProjectionMap.
typedef multimap<SourceAttribute, int> ReverseProjectionMap;

// Iterator over positions in projection's result schema (over
// ReverseProjectionMap values).
typedef iterator_second<ReverseProjectionMap::const_iterator> PositionIterator;

// Project multiple views / schemas into one. It is simply a crossbar that
// expects certain number of input views with specific schemas, and picks
// attributes from these views, producing a single output view with specific
// (resolved) result schema.
class BoundMultiSourceProjector {
 public:
  // A single element in the crossbar; picks an attribute at a given position
  // from a view at a given index.

  // Creates an empty projector for a specific number of sources with
  // specified schemas.
  explicit BoundMultiSourceProjector(
      const vector<const TupleSchema*>& sources) {
    for (int i = 0; i < sources.size(); ++i) {
      source_schemas_.push_back(*sources[i]);
    }
  }

  // Adds a single attribute to the projector. Specifies the index of its
  // input view and the attribute's position within that input view.
  // Returns true on success; false in case of a name clash with an existing
  // attribute.
  bool Add(int source_index, int source_attribute_position) {
    return AddAs(source_index, source_attribute_position, "");
  }

  // Adds a single attribute to the projector. Specifies the index of its
  // input view, the attribute's position within that input view,
  // and an alias to be used as a name in the result schema.
  // If alias is empty, the original attribute's name will be used.
  bool AddAs(int source_index,
             int source_attribute_position,
             const StringPiece& alias);

  // Return the count of sources this projector expects.
  int source_count() const { return  source_schemas_.size(); }

  // Returns the schema of the specified source this projector expects.
  const TupleSchema& source_schema(int source_index) const {
    CHECK_LT(source_index, source_count());
    return source_schemas_[source_index];
  }

  // Returns the result schema.
  const TupleSchema& result_schema() const { return result_schema_; }

  // Methods for querying the source -> target mapping and the reverse
  // target -> source mapping.

  // Maps an attribute in projection's result back to the index of the source
  // view it comes from.
  int source_index(int projected_attribute_position) const {
    return projection_map_[projected_attribute_position].source_index;
  }

  // Maps an attribute in projection's result back to the position in the source
  // view it comes from.
  int source_attribute_position(int projected_attribute_position) const {
    return projection_map_[projected_attribute_position].attribute_position;
  }

  // Maps an attribute in projection's result back to the original in the source
  // schema.
  const Attribute& source_attribute(int projected_attribute_position) const {
    return source_schemas_[source_index(projected_attribute_position)].
        attribute(source_attribute_position(projected_attribute_position));
  }

  // Maps a source attribute to its position(s) in projection's result.
  pair<PositionIterator, PositionIterator> ProjectedAttributePositions(
      int source_index, int attribute_position) const;

  // Returns true if a source attribute is a part of projection.
  bool IsAttributeProjected(int source_index,
                            int attribute_position) const;

  // Returns the number of output columns to which the input column is
  // projected.
  int NumberOfProjectionsForAttribute(int source_index,
                                      int attribute_position) const;

  // Extracts a sub-projector specific to a particular source.
  BoundSingleSourceProjector GetSingleSourceProjector(int source_index) const;

  // Applies the projection from the source views into the result view.
  // ViewIterator must have random-access iterator semantics, and yield
  // const View* when dereferenced.
  // Note: does NOT set the row_count on the result.
  template<typename ViewIterator>
  void Project(const ViewIterator sources_begin,
               const ViewIterator sources_end,
               View* result) const;

  string GetDebugString() const {
    string result = "\n";
    for (int i = 0; i < source_count(); ++i) {
      StringAppendF(&result, "source %d: ", i);
      result += source_schema(i).GetHumanReadableSpecification();
      result += "\n";
    }
    for (int i = 0; i < result_schema().attribute_count(); ++i) {
      if (i > 0) result += ", ";
      StringAppendF(&result, "(%d, %d)",
                    source_index(i),
                    source_attribute_position(i));
    }
    return result;
  }

 private:
  vector<TupleSchema> source_schemas_;
  TupleSchema result_schema_;
  vector<SourceAttribute> projection_map_;
  ReverseProjectionMap reverse_projection_map_;
  // Copyable.
};

// Projector from a single source view. Picks attributes from a source view
// with some expected schema. Usually created by calling
// SingleSourceProjector::Bind(), binding the abstract logic of some
// SingleSourceProjector to the specific source schema.
class BoundSingleSourceProjector {
 public:
  explicit BoundSingleSourceProjector(const TupleSchema& source_schema)
      : projector_(vector<const TupleSchema*>(1, &source_schema)) {}

  bool Add(int source_attribute_position) {
    return projector_.Add(0, source_attribute_position);
  }

  bool AddAs(int source_attribute_position, const StringPiece& alias) {
    return projector_.AddAs(0, source_attribute_position, alias);
  }

  const TupleSchema& source_schema() const {
    return projector_.source_schema(0);
  }

  const TupleSchema& result_schema() const {
    return projector_.result_schema();
  }

  int source_attribute_position(int projected_attribute_position) const {
    return projector_.source_attribute_position(projected_attribute_position);
  }

  pair<PositionIterator, PositionIterator> ProjectedAttributePositions(
      int source_attribute_pos) const {
    return projector_.ProjectedAttributePositions(0, source_attribute_pos);
  }

  bool IsAttributeProjected(int source_attribute_position) const {
    return projector_.IsAttributeProjected(0, source_attribute_position);
  }

  int NumberOfProjectionsForAttribute(int source_attribute_position) const {
    return projector_.NumberOfProjectionsForAttribute(
        0, source_attribute_position);
  }

  // Note: does NOT set the row_count on the result.
  void Project(const View& source, View* result) const {
    const View* sources[] = { &source };
    projector_.Project(&sources[0], &sources[1], result);
  }

  string GetDebugString() const {
    string result = "\nsource: ";
    result += source_schema().GetHumanReadableSpecification();
    result += "\n";
    for (int i = 0; i < result_schema().attribute_count(); ++i) {
      if (i > 0) result += ", ";
      StringAppendF(&result, "%d", source_attribute_position(i));
    }
    return result;
  }

 private:
  BoundMultiSourceProjector projector_;
  // Copyable.
};

// Utility function.
// Given a multi-source projector, factors out its specified source to a
// 'canonical form' that has no duplicates. This way, if the source data must
// be pre-processed somehow, we can make sure that each column that we
// eventually need is pre-processed once.
//
// The function returns two projectors:
// (1) a SingleSourceProjector that retains all needed columns from the nth
// source but without duplicates,
// (2) modified MultiSourceProjector that assumes that the nth source is now
// the result of that 'canonical' projection (i.e. it always uses all columns
// of that 'canonical' projection, possibly with duplicates).
//
// The ownership of the new projectors is passed to the caller.
pair<BoundMultiSourceProjector*, BoundSingleSourceProjector*> DecomposeNth(
    int source_index,
    const BoundMultiSourceProjector& projector);

// Abstract projector from a single source view. Source schema (and thus result
// schema) is not resolved.
//
// Concrete implementations of projectors may include:
// * project an attribute named 'foo'
// * project the fifth attribute
// * project all attributes with names like 'foo%'
// * project all attributes except 'foo'
//
class SingleSourceProjector {
 public:
  virtual ~SingleSourceProjector() {}
  // Returns new BoundSingleSourceProjector, binding the logic of this
  // projector to the specified source schema, or an Exception if binding fails.
  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& source_schema) const = 0;

  // Clones the projector recursively all the way to the bottom. Useful when the
  // same projector needs to be given to many exclusive owners.
  virtual SingleSourceProjector* Clone() const = 0;

  virtual string ToString(bool verbose) const = 0;
 protected:
  SingleSourceProjector() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(SingleSourceProjector);
};

// Concatenates a list of SingleSourceProjectors into a single projector.
// For example:
// new CompoundSingleSourceProjector()
//     ->add(ProjectNamedAttribute("foo"))
//     ->add(ProjectNamedAttribute("bar"))
//     ->add(ProjectAttributeAt(12));
class CompoundSingleSourceProjector : public SingleSourceProjector {
 public:
  CompoundSingleSourceProjector() {}
  virtual ~CompoundSingleSourceProjector() {}
  CompoundSingleSourceProjector* add(const SingleSourceProjector* projector) {
    projectors_.push_back(make_linked_ptr(projector));
    return this;
  }
  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& source_schema) const;

  virtual CompoundSingleSourceProjector* Clone() const;

  virtual string ToString(bool verbose) const;

 private:
  vector<linked_ptr<const SingleSourceProjector> > projectors_;
  DISALLOW_COPY_AND_ASSIGN(CompoundSingleSourceProjector);
};

// TODO(user): move to .cc anonymous namespace, or some internal namespace.
// Apply consistently for all files from projector.cc.
class NamedAttributeProjector : public SingleSourceProjector {
 public:
  explicit NamedAttributeProjector(const StringPiece& name);

  virtual FailureOrOwned<const BoundSingleSourceProjector> Bind(
      const TupleSchema& source_schema) const;

  virtual NamedAttributeProjector* Clone() const {
    return new NamedAttributeProjector(name_);
  }

  virtual string ToString(bool verbose) const { return name_; }

 private:
  string name_;
  DISALLOW_COPY_AND_ASSIGN(NamedAttributeProjector);
};

// Returns a new projector that decorates its source projector and renames
// all its resulting attributes, using to the specified aliases.
// Aliases in the the vector of aliases must be unique. If, later during
// binding, the count of attributes in the schema does not match the number of
// aliases, an exception object is returned.
// Ownership of the projector is transferred to the caller.
const SingleSourceProjector* ProjectRename(const vector<string>& aliases,
                                           const SingleSourceProjector* source);

// Returns a new projector that decorates its source projector, assumes that
// it produces a single-attribute result, and renames the resulting attribute
// using the specified alias. If, later during binding, the count of attributes
// is different than 1, an exception is returned.
// Ownership of the projector is transferred to the caller.
inline const SingleSourceProjector* ProjectRenameSingle(
    const StringPiece& alias,
    const SingleSourceProjector* source) {
  return ProjectRename(vector<string>(1, alias.ToString()), source);
}

// Returns a new projector that projects a single attribute with a given name.
// Ownership of the projector is transferred to the caller.
const SingleSourceProjector* ProjectNamedAttribute(const StringPiece& name);

// Returns a new projector that projects a single attribute with a given name,
// and renames it to the specified alias.
// Ownership of the projector is transferred to the caller.
inline const SingleSourceProjector* ProjectNamedAttributeAs(
    const StringPiece& name,
    const StringPiece& alias) {
  return ProjectRenameSingle(alias, ProjectNamedAttribute(name));
}

// Returns a new projector that projects a single attribute at a given position.
// Ownership of the projector is transferred to the caller.
const SingleSourceProjector* ProjectAttributeAt(int position);

// Returns a new projector that projects a single attribute at a given position,
// and renames it to the specified alias.
// Ownership of the projector is transferred to the caller.
inline const SingleSourceProjector* ProjectAttributeAtAs(
    const int position,
    const StringPiece& alias) {
  return ProjectRenameSingle(alias, ProjectAttributeAt(position));
}

// A utility shortcut for building a compound projector for those columns, whose
// positions are specified.
const SingleSourceProjector* ProjectAttributesAt(const vector<int>& positions);

// A convenience shourtcut to build a compound projector for the list of
// specific named attributes. Useful for specifying join keys, sort keys, etc.
// Ownership of the projector is transferred to the caller.
const SingleSourceProjector* ProjectNamedAttributes(
    const vector<string>& names);

// Returns a new projector that projects all attributes from a given source.
const SingleSourceProjector* ProjectAllAttributes();

// Returns a new projector that projects all attributes from a given source
// and prepends the prefix to their original names.
const SingleSourceProjector* ProjectAllAttributes(const StringPiece& prefix);

// A mirror copy of SingleSourceProjector.
class MultiSourceProjector {
 public:
  virtual ~MultiSourceProjector() {}
  // Returns new BoundMultiSourceProjector, binding the logic of this
  // projector to the specified source schemas, or an Exception if binding
  // fails.
  virtual FailureOrOwned<const BoundMultiSourceProjector> Bind(
      const vector<const TupleSchema*>& source_schemas) const = 0;

  virtual string ToString(bool verbose) const = 0;
 protected:
  MultiSourceProjector() {}
 private:
  DISALLOW_COPY_AND_ASSIGN(MultiSourceProjector);
};

// Modeled after CompoundSingleSourceProjector.
class CompoundMultiSourceProjector : public MultiSourceProjector {
 public:
  CompoundMultiSourceProjector() {}
  virtual ~CompoundMultiSourceProjector() {}
  CompoundMultiSourceProjector* add(int source_index,
                                    const SingleSourceProjector* projector) {
    projectors_.push_back(make_pair(source_index, make_linked_ptr(projector)));
    return this;
  }
  virtual FailureOrOwned<const BoundMultiSourceProjector> Bind(
      const vector<const TupleSchema*>& source_schemas) const;

  virtual string ToString(bool verbose) const;

 private:
  // Pair maps a number of input to the projector for this input.
  vector<pair<int, linked_ptr<const SingleSourceProjector> > > projectors_;

  DISALLOW_COPY_AND_ASSIGN(CompoundMultiSourceProjector);
};


// Inline and template methods.

template<typename ViewIterator>
void BoundMultiSourceProjector::Project(const ViewIterator sources_begin,
                                        const ViewIterator sources_end,
                                        View* result) const {
  DCHECK_EQ(projection_map_.size(), result->schema().attribute_count());
  int sources_count = sources_end - sources_begin;
  for (int i = 0; i < projection_map_.size(); ++i) {
    const SourceAttribute& source_attribute = projection_map_[i];
    DCHECK_LT(source_attribute.source_index, sources_count);
    const View& view = *sources_begin[source_attribute.source_index];
    DCHECK_LT(source_attribute.attribute_position,
              view.schema().attribute_count());
    const Column& column = view.column(source_attribute.attribute_position);
    result->mutable_column(i)->ResetFrom(column);
  }
}

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_PROJECTOR_H_
