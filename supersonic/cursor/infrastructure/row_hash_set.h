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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_

#include <stddef.h>

#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

class BoundSingleSourceProjector;
class BufferAllocator;
class TupleSchema;
class View;

namespace row_hash_set {

static const rowid_t kInvalidRowId = -1;

class FindMultiResult;
class FindResult;
class RowHashSetImpl;

// A row container that stores rows in an internal storage block (defined by
// a schema passed in constructor) and groups rows with identical keys together.
// Allows efficient key-based row lookups and insertions with respectively Find
// and Insert operations that work on blocks (technically Views) of rows at
// a time.
//
// A logical atomic piece of data in the set is a row. Rows must have at least
// one key column and can have any number of value columns. Row equality is
// established using only key columns. The storage block and insert query views
// should have the same schema. Find queries should only include key columns.
//
// RowHashSet can hold at most one row with a given key. Attempts to insert
// subsequent rows with the same key will result in row id of the existing row
// being returned.
//
// RowHashMultiSet can hold any number of rows with a given key. New rows are
// always inserted. For each row passed to Insert the returned row id is
// different.
//
// At most Cursor::kDefaultRowCount rows can be passed in a single call to
// Find / Insert.
class RowHashSet {
 public:
  typedef FindResult ResultType;

  ~RowHashSet();

  explicit RowHashSet(const TupleSchema& block_schema,
                      BufferAllocator* const allocator);

  RowHashSet(const TupleSchema& block_schema,
             BufferAllocator* const allocator,
             const BoundSingleSourceProjector* key_selector);

  // All columns are key.
  RowHashSet(const TupleSchema& block_schema,
             BufferAllocator* const allocator,
             int64 max_unique_keys_in_result);

  // Takes ownership of a SingleSourceProjector that indicates the subset of
  // internal block's columns that are key columns. It must be bound to block's
  // schema.
  RowHashSet(const TupleSchema& block_schema,
             BufferAllocator* const allocator,
             const BoundSingleSourceProjector* key_selector,
             int64 max_unique_keys_in_result);

  // Ensures that the hash set has capacity for at least the specified
  // number of rows. Returns false on OOM. Using this method may reduce
  // the number of reallocations (normally, the row capacity starts at 16
  // and grows by a factor of two every time the hash table needs to grow).
  bool ReserveRowCapacity(rowcount_t row_capacity);

  // Writes into result a vector such that the n-th element is the result of
  // a lookup of the n-th query row. Each result is an id of a row in the block
  // or kInvalid if key not found.
  void Find(const View& query, FindResult* result) const;

  // A variant of Find that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  void Find(const View& query,
            const bool_const_ptr selection_vector,
            FindResult* result) const;

  // Writes into result a vector such that the n-th element is the result of
  // an insertion of the n-th query row. For each query row the insertion
  // happens only if the row's key is not NULL and is not already present in
  // the block. Returns the number of rows successfully inserted. Can be
  // lower than query.row_count() if data being inserted is of variable-
  // length type and the storage block's arena can't accomodate a copy of
  // a variable-length data buffer.
  size_t Insert(const View& query, FindResult* result);

  // A variant of Insert that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  size_t Insert(const View& query, const bool_const_ptr selection_vector,
                FindResult* result);

  // A variant of Insert that doesn't return the result vector (insertions still
  // take place).
  size_t Insert(const View& query);

  // Same as above, with selection vector.
  size_t Insert(const View& query, const bool_const_ptr selection_vector);

  // Clears set to contain 0 rows.
  void Clear();

  // Compacts internal datastructures to minimize memory usage.
  void Compact();

  // The read-only content.
  const View& indexed_view() const;

  // Number of rows successfully inserted so far.
  rowcount_t size() const;

 private:
  RowHashSetImpl* impl_;
};

class RowHashMultiSet {
 public:
  typedef FindMultiResult ResultType;

  ~RowHashMultiSet();

  // All columns are key.
  explicit RowHashMultiSet(const TupleSchema& block_schema,
                           BufferAllocator* const allocator);

  // Takes ownership of a SingleSourceProjector that indicates the subset of
  // internal block's columns that are key columns. It must be bound to block's
  // schema.
  RowHashMultiSet(const TupleSchema& block_schema,
                  BufferAllocator* const allocator,
                  const BoundSingleSourceProjector* key_selector);

  // Ensures that the hash set has capacity for at least the specified
  // number of rows. Returns false on OOM. Using this method may reduce
  // the number of reallocations (normally, the row capacity starts at 16
  // and grows by a factor of two every time the hash table needs to grow).
  bool ReserveRowCapacity(rowcount_t capacity);

  // Writes into result a vector such that the n-th element is the result of
  // a lookup of the n-th query row. Each result is a set of ids of rows in the
  // block (empty if key not found).
  void Find(const View& query, FindMultiResult* result) const;

  // A variant of Find that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  void Find(const View& query, const bool_const_ptr selection_vector,
            FindMultiResult* result) const;

  // Writes into result a vector such that the n-th element is the result of
  // an insertion of the n-th query row. The insertion happens for each query
  // row with non-NULL key. Returns the number of rows successfully inserted.
  // Can be lower than query.row_count() if data being inserted is of
  // variable-length type and the storage block's arena can't accomodate a copy
  // of a variable-length data buffer.
  size_t Insert(const View& query, FindMultiResult* result);

  // A variant of Insert that takes an additional selection vector; it must have
  // query.row_count elements. If selection_vector[i] == false then query row
  // at index i is not processed at all. Its corresponding result entry is set
  // to kInvalidRowId.
  size_t Insert(const View& query, const bool_const_ptr selection_vector,
                FindMultiResult* result);

  // A variant of Insert that doesn't return the result vector (insertions still
  // take place).
  size_t Insert(const View& query);

  // Same as above, with selection vector.
  size_t Insert(const View& query, const bool_const_ptr selection_vector);

  // Clears multiset to contain 0 rows.
  void Clear();

  // Compacts internal datastructures to minimize memory usage.
  void Compact();

  // The read-only content.
  const View& indexed_view() const;

  // Number of rows in the storage block (successfully inserted so far).
  rowcount_t size() const;

  // Total number of rows this hash set can hold.
  rowcount_t capacity() const;

 private:
  RowHashSetImpl* impl_;
};


// Result type of RowHashSet::Find and RowHashSet::Insert. A vector of row ids,
// one for each row in the original query, of the resulting inserted row or of a
// row with identical key already present in the storage block. In case of Find,
// the special RowId value kInvalid denotes absence of a matching row.
class FindResult {
 public:
  // FindResult should have as big capacity as is the size of the corresponding
  // query.
  explicit FindResult(rowcount_t query_row_count) :
      row_ids_(new rowid_t[query_row_count]),
      row_ids_size_(query_row_count) { }

  rowid_t Result(rowid_t query_id) const {
    DCHECK_GT(row_ids_size_, 0);
    return row_ids_[query_id];
  }

  // Exposes the entire array of Results.
  const rowid_t* row_ids() { return row_ids_.get(); }

 private:
  rowid_t* mutable_row_ids() { return row_ids_.get(); }

  std::unique_ptr<rowid_t[]> row_ids_;
  rowcount_t row_ids_size_;

  friend class RowHashSetImpl;
};


struct EqualRowIdsLink;

// An iterator over a set of ids of equivalent rows (with identical keys). For
// use with RowHashMultiSet where the return value for a single query row is
// a set of row ids.
class RowIdSetIterator {
 public:
  // Public constructor; the iterator starts AtEnd().
  RowIdSetIterator() : equal_row_ids_(NULL), current_(kInvalidRowId) { }

  bool AtEnd() const { return current_ == kInvalidRowId; }
  rowid_t Get() const {
    DCHECK(!AtEnd());
    return current_;
  }
  void Next();

 private:
  RowIdSetIterator(const EqualRowIdsLink* equal_row_ids, rowid_t first)
      : equal_row_ids_(equal_row_ids),
        current_(first) {}

  const EqualRowIdsLink* equal_row_ids_;
  rowid_t current_;

  friend class FindMultiResult;
};


// Result type of RowHashMultiSet::Find and RowHashMultiSet::Insert. A vector
// of sets of row ids, one for each row in the original query.
class FindMultiResult {
 public:
  // FindMultiResult should have as big capacity as is the size of
  // the corresponding query.
  explicit FindMultiResult(rowcount_t query_row_count)
      : query_row_count_(query_row_count),
        row_ids_(new rowid_t[query_row_count]),
        equal_row_ids_(NULL) {}

  RowIdSetIterator Result(rowid_t query_id) const {
    return RowIdSetIterator(equal_row_ids_, row_ids_[query_id]);
  }

 private:
  rowid_t* mutable_row_ids() { return row_ids_.get(); }

  void set_equal_row_ids(const EqualRowIdsLink* equal_row_ids) {
    equal_row_ids_ = equal_row_ids;
  }

  rowcount_t query_row_count_;
  std::unique_ptr<rowid_t[]> row_ids_;
  const EqualRowIdsLink* equal_row_ids_;

  friend class RowHashSetImpl;
};

}  // namespace row_hash_set

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_HASH_SET_H_
