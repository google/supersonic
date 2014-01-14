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
//
// Classes and functions for handling ordered data.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ORDERING_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ORDERING_H_

#include <stddef.h>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

// Represents a sort order (list of attributes with specified direction,
// ASCENDING or DESCENDING) for a given schema. Combines a
// BoundSingleSourceProjector (which specifies key columns) with a vector of
// ColumnOrders (specifying the direction).
// TODO(user): rename to BoundSortSpecification for consistency.
class BoundSortOrder {
 public:
  BoundSortOrder(const BoundSingleSourceProjector* projector,
                 const vector<ColumnOrder>& column_order)
      : projector_(projector),
        column_order_(column_order) {
    CHECK_EQ(projector_->result_schema().attribute_count(),
             column_order_.size());
  }

  // A short-hand for all ASCENDING.
  explicit BoundSortOrder(const BoundSingleSourceProjector* projector)
      : projector_(projector),
        column_order_(
            vector<ColumnOrder>(projector->result_schema().attribute_count(),
                                ASCENDING)) {
    CHECK_EQ(projector_->result_schema().attribute_count(),
             column_order_.size());
  }

  // Returns the sort key schema.
  const TupleSchema& schema() const {
    return projector().result_schema();
  }

  // Return the ordering of the ith attribute in the sort key schema.
  ColumnOrder column_order(int i) const { return column_order_[i]; }

  // Returns the projector, used to extract the sort key schema from the source
  // schema.
  const BoundSingleSourceProjector& projector() const { return *projector_; }

  // Return the schema of the data source, which the sort key schema is a
  // sub-schema of. Equivalent to projector().source_schema().
  const TupleSchema& source_schema() const {
    return projector().source_schema();
  }

  // Given the ith attribute in the sort key schema, returns its position in the
  // source schema. Equivalent to projector().source_attribute_position(i).
  size_t source_attribute_position(size_t i) const {
    return projector().source_attribute_position(i);
  }

 private:
  std::unique_ptr<const BoundSingleSourceProjector> projector_;
  vector<ColumnOrder> column_order_;
  DISALLOW_COPY_AND_ASSIGN(BoundSortOrder);
};

// Defines a sort order, not yet bound to a particular schema. Consists of
// a list of SingleSourceProjectors, representing (groups of) key columns,
// and a ColumnOrder vector, specifying the sort direction for each column
// in the group.
// TODO(user): rename to SortSpecification for consistency.
class SortOrder {
 public:
  SortOrder() {}
  // Adds a new (group of) columns to the sort order. The column(s) will be
  // sorted in a direction specified by the column_order parameter.
  SortOrder* add(const SingleSourceProjector* projector,
                 const ColumnOrder column_order) {
    projectors_.push_back(make_linked_ptr(projector));
    column_order_.push_back(column_order);
    return this;
  }

  // Shorthand methods combining add(...) and particular projection.
  SortOrder* OrderByAttributeAt(const size_t position,
                                const ColumnOrder column_order) {
    return add(ProjectAttributeAt(position), column_order);
  }
  SortOrder* OrderByNamedAttribute(StringPiece name,
                                   const ColumnOrder column_order) {
    return add(ProjectNamedAttribute(name), column_order);
  }

  // Returns a sort order bound to the specified schema (replacing 'symbolic'
  // column references to column indexes).
  FailureOrOwned<const BoundSortOrder> Bind(
      const TupleSchema& source_schema) const;

 private:
  vector<linked_ptr<const SingleSourceProjector> > projectors_;
  vector<ColumnOrder> column_order_;
  DISALLOW_COPY_AND_ASSIGN(SortOrder);
};

// Class for generating selection vectors in which each value occurs exactly
// once. Exposes mutators that preserve this invariant (sort, reverse,
// partition, etc.)
class Permutation {
 public:
  // Creates an 'identity' permutation of the specified size. The underlying
  // selection vector consists of consecutive integers in the range
  // [0, size - 1].
  explicit Permutation(rowcount_t size) {
    permutation_.resize(size);
    for (rowcount_t i = 0; i < size; ++i) permutation_[i] = i;
  }

  // Sorts the specified range of the permutation using the specified
  // comparator.
  template<typename LessThanComparator>
  void Sort(rowcount_t from, rowcount_t to, LessThanComparator comparator) {
    DCHECK_GE(from, 0);
    DCHECK_LE(from, to);
    DCHECK_LE(to, size());
    const vector<rowid_t>::iterator begin = permutation_.begin() + from;
    const vector<rowid_t>::iterator end = permutation_.begin() + to;
    std::sort(begin, end, comparator);
  }

  // Partitions the specified range of the permutation using the specified
  // predicate. All elements for which the predicate evaluated to false will
  // precede all these for which it evaluated to true.
  template<typename Predicate>
  rowcount_t Partition(rowcount_t from, rowcount_t to, Predicate predicate) {
    DCHECK_GE(from, 0);
    DCHECK_LE(from, to);
    DCHECK_LE(to, size());
    const vector<rowid_t>::iterator begin = permutation_.begin() + from;
    const vector<rowid_t>::iterator end = permutation_.begin() + to;
    return std::partition(begin, end, predicate) - begin;
  }

  // Reverses the specified range of the permutation.
  void Reverse(rowcount_t from, rowcount_t to) {
    DCHECK_GE(from, 0);
    DCHECK_LE(from, to);
    DCHECK_LE(to, size());
    const vector<rowid_t>::iterator begin = permutation_.begin() + from;
    const vector<rowid_t>::iterator end = permutation_.begin() + to;
    std::reverse(begin, end);
  }

  // Rotates the specified range of the permutation, about the given element.
  // Equivalent to left-shifting by [middle - from] modulo size.
  void Rotate(rowcount_t from, rowcount_t middle, rowcount_t to) {
    DCHECK_GE(from, 0);
    DCHECK_LE(to, size());
    DCHECK_LE(from, middle);
    DCHECK_LE(middle, to);
    const vector<rowid_t>::iterator begin = permutation_.begin() + from;
    const vector<rowid_t>::iterator mid = permutation_.begin() + middle;
    const vector<rowid_t>::iterator end = permutation_.begin() + to;
    std::rotate(begin, mid, end);
  }

  // Randomly shuffles the specified range of the partition, using the specified
  // random number generator.
  template<typename Generator>
  void RandomShuffle(rowcount_t from, rowcount_t to, Generator generator) {
    DCHECK_GE(from, 0);
    DCHECK_LE(from, to);
    DCHECK_LE(to, size());
    const vector<rowid_t>::iterator begin = permutation_.begin() + from;
    const vector<rowid_t>::iterator end = permutation_.begin() + to;
    std::random_shuffle(begin, end, generator);
  }

  // Returns the number of elements in the partition.
  rowcount_t size() const { return permutation_.size(); }

  // Returns the partition element the specified position.
  rowid_t at(rowcount_t position) const { return permutation_[position]; }

  // Returns the entire permutation as a selection vector.
  const rowid_t* permutation() const {
    return (!permutation_.empty()) ? &permutation_.front() : NULL;
  }

 private:
  vector<rowid_t> permutation_;
  DISALLOW_COPY_AND_ASSIGN(Permutation);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ORDERING_H_
