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
//
// BlockBuilder that allows to create blocks that hold predefined data.
// Used also as a building block for CursorBuilder class.
//
// Example usage:
//    scoped_ptr<Block> block(
//      BlockBuilder<STRING, INT32>()
//      .AddRow("first", 12)
//      .AddRow("second", __)
//      .Build());
// Creates block with two columns STRING and INT32, STRING column is not
// nullable (because all string values passed to AddRow are not null). INT32
// column is nullable, (because value in second row is NULL, represented by
// global constant __).

#ifndef SUPERSONIC_TESTING_BLOCK_BUILDER_H_
#define SUPERSONIC_TESTING_BLOCK_BUILDER_H_

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/cursor/infrastructure/value_ref.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// Allows to distinguish how many columns should row writers and builders
// be accepting in AddRow() calls.
class Block;
class View;

namespace internal {

// Makes a deep copy of the specified view, and returns it in a new block.
Block* CloneView(const View& view);

// Makes a copy of the specified view, and returns it in a new block.
// The ownership is transferred to the caller. All nullable columns for which
// no nulls have been found are transformed into non-nullable columns.
// Columns can be forced to be nullable independent whether they have nulls
// or not, using the is_column_forced_nullable vector.
Block* CloneViewAndOptimizeNullability(
    const View& view, const vector<bool>& is_column_forced_nullable);

// Helper function to set value or null from a value reference.
template<int type>
inline void SetTypedValueRef(const ValueRef<type> ref, TableRowWriter* writer) {
  if (ref.is_null()) {
    writer->Null();
  } else {
    writer->Set<static_cast<DataType>(type)>(ref.value());
  }
}

template<>
inline void SetTypedValueRef<UNDEF>(const ValueRef<UNDEF> ref,
                                    TableRowWriter* writer) {
  // Does nothing.
}

// Convenience function to create a schema, given the list of types.
// All attributes will be nullable. Attribute names will be col0, col1, etc.
inline TupleSchema CreateReferenceNullableSchema(
    int A = UNDEF, int B = UNDEF, int C = UNDEF, int D = UNDEF,
    int E = UNDEF, int F = UNDEF, int G = UNDEF, int H = UNDEF,
    int I = UNDEF, int J = UNDEF, int K = UNDEF, int L = UNDEF,
    int M = UNDEF, int N = UNDEF, int O = UNDEF, int P = UNDEF,
    int Q = UNDEF, int R = UNDEF, int S = UNDEF, int T = UNDEF) {
  int types[] = { A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T};
  TupleSchema schema;
  for (int i = 0; i < arraysize(types); ++i) {
    if (types[i] == UNDEF) return schema;
    schema.add_attribute(
        Attribute(StringPrintf("col%d", i),
                  static_cast<DataType>(types[i]), NULLABLE));
  }
  return schema;
}

}  // namespace internal

// Builder that can create blocks with up to 20 columns.
template <int A = UNDEF, int B = UNDEF, int C = UNDEF, int D = UNDEF,
          int E = UNDEF, int F = UNDEF, int G = UNDEF, int H = UNDEF,
          int I = UNDEF, int J = UNDEF, int K = UNDEF, int L = UNDEF,
          int M = UNDEF, int N = UNDEF, int O = UNDEF, int P = UNDEF,
          int Q = UNDEF, int R = UNDEF, int S = UNDEF, int T = UNDEF>
class BlockBuilder {
  typedef BlockBuilder<A, B, C, D, E, F, G, H, I, J,
                       K, L, M, N, O, P, Q, R, S, T> This;

 public:
  BlockBuilder()
      : table_(internal::CreateReferenceNullableSchema(A, B, C, D, E, F,
                                                       G, H, I, J, K, L,
                                                       M, N, O, P, Q, R,
                                                       S, T),
               HeapBufferAllocator::Get()),
        writer_(&table_),
        is_column_forced_nullable_(20, false) {}

  BlockBuilder(const TupleSchema& schema)
      : table_(schema, HeapBufferAllocator::Get()),
        writer_(&table_),
        is_column_forced_nullable_() {
    TupleSchema reference_schema =
        internal::CreateReferenceNullableSchema(A, B, C, D, E, F,
                                                G, H, I, J, K, L,
                                                M, N, O, P, Q, R,
                                                S, T);
    CHECK(schema.EqualByType(reference_schema));
  }

  // Adds a row. Caller can use built-in types instead of ValueRef objects.
  // Number of arguments passed to AddRow must equal number of parameters passed
  // to BlockBuilder template. Global constant __ can be used for each value
  // that is null.
  This& AddRow(ValueRef<A> a = ValueRef<UNDEF>(),
               ValueRef<B> b = ValueRef<UNDEF>(),
               ValueRef<C> c = ValueRef<UNDEF>(),
               ValueRef<D> d = ValueRef<UNDEF>(),
               ValueRef<E> e = ValueRef<UNDEF>(),
               ValueRef<F> f = ValueRef<UNDEF>(),
               ValueRef<G> g = ValueRef<UNDEF>(),
               ValueRef<H> h = ValueRef<UNDEF>(),
               ValueRef<I> i = ValueRef<UNDEF>(),
               ValueRef<J> j = ValueRef<UNDEF>(),
               ValueRef<K> k = ValueRef<UNDEF>(),
               ValueRef<L> l = ValueRef<UNDEF>(),
               ValueRef<M> m = ValueRef<UNDEF>(),
               ValueRef<N> n = ValueRef<UNDEF>(),
               ValueRef<O> o = ValueRef<UNDEF>(),
               ValueRef<P> p = ValueRef<UNDEF>(),
               ValueRef<Q> q = ValueRef<UNDEF>(),
               ValueRef<R> r = ValueRef<UNDEF>(),
               ValueRef<S> s = ValueRef<UNDEF>(),
               ValueRef<T> t = ValueRef<UNDEF>()) {
    writer_.AddRow();
    internal::SetTypedValueRef(a, &writer_);
    internal::SetTypedValueRef(b, &writer_);
    internal::SetTypedValueRef(c, &writer_);
    internal::SetTypedValueRef(d, &writer_);
    internal::SetTypedValueRef(e, &writer_);
    internal::SetTypedValueRef(f, &writer_);
    internal::SetTypedValueRef(g, &writer_);
    internal::SetTypedValueRef(h, &writer_);
    internal::SetTypedValueRef(i, &writer_);
    internal::SetTypedValueRef(j, &writer_);
    internal::SetTypedValueRef(k, &writer_);
    internal::SetTypedValueRef(l, &writer_);
    internal::SetTypedValueRef(m, &writer_);
    internal::SetTypedValueRef(n, &writer_);
    internal::SetTypedValueRef(o, &writer_);
    internal::SetTypedValueRef(p, &writer_);
    internal::SetTypedValueRef(q, &writer_);
    internal::SetTypedValueRef(r, &writer_);
    internal::SetTypedValueRef(s, &writer_);
    internal::SetTypedValueRef(t, &writer_);
    return *this;
  }

  // Returns a copy of the prebuilt block. Ownership is passed to the caller.
  Block* Build() const {
    writer_.CheckSuccess();
    if (has_explicit_schema()) {
      return internal::CloneView(table_.view());
    } else {
      return internal::CloneViewAndOptimizeNullability(
        table_.view(),
        is_column_forced_nullable_);
    }
  }

  // Forces a column to be nullable, even though no NULL is present in the
  // column. The block builder must not have explicitly specified schema.
  This& ForceNullable(int column_number) {
    CHECK(!has_explicit_schema())
        << "This builder has an explicitly provided schema; can't force-"
        << "nullable individual columns";
    CHECK_GE(column_number, 0);
    CHECK_LT(column_number, 20);
    is_column_forced_nullable_[column_number] = true;
    return *this;
  }

 private:
  // Specifies whether the schema of this block has been explicitly specified
  // in the constructor. (Otherwise, a default schema will be provided, based
  // on the templated types, and the actual data nullability).
  bool has_explicit_schema() const {
    return is_column_forced_nullable_.empty();
  }

  // Internal table where rows are incrementally added.
  Table table_;
  // Helper writer.
  TableRowWriter writer_;
  // Keeps track of whether a column's nullability has been enforced to be
  // null.
  vector<bool> is_column_forced_nullable_;
  DISALLOW_COPY_AND_ASSIGN(BlockBuilder);
};

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_BLOCK_BUILDER_H_
