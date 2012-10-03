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
// Class for repetitive copying of fixed-schema row(s) in a single call.
// Optionally, the user can specify a projector to select input columns to copy.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_COPIER_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_COPIER_H_

#include <stddef.h>

#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

template <typename ConstRow>
class RowCopier {
 public:
  typedef bool (*CopyFn)(const ConstRow& row,
                         size_t column_position,
                         size_t output_offset,
                         OwnedColumn* output);

  // Creates a copier that copies all columns from the given schema.
  RowCopier(const TupleSchema& schema, bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  bool Copy(const ConstRow& input,
            const size_t output_offset,
            Block* output) const;

 private:
  vector<CopyFn> copiers_;
};

template <typename ConstRow>
class RowCopierWithProjector {
 public:
  // Variant that only copies projected columns; input and output schemas
  // are given by the bound projector, which must outlive this instance
  // of the RowCopier.
  RowCopierWithProjector(const BoundSingleSourceProjector* projector,
                         bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  bool Copy(const ConstRow& input,
            const size_t output_offset,
            Block* output) const;

 private:
  const BoundSingleSourceProjector* projector_;
  vector<typename RowCopier<ConstRow>::CopyFn> copiers_;
};

// Copies multiple source rows into a destination block. A projector must
// be supplied to indicate the mapping from many sources to a single
// destination.
template<typename ConstRow>
class MultiRowCopier {
 public:
  // Input and output schemas are given by the bound projector, which must
  // outlive this instance of ViewMultiCopier
  MultiRowCopier(const BoundMultiSourceProjector* projector, bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  size_t Copy(const vector<const ConstRow*>& input_rows,
              const size_t output_offset,
              Block* output) const;

 private:
  const BoundMultiSourceProjector* projector_;
  vector<typename RowCopier<ConstRow>::CopyFn> copiers_;
};


// Implementation of template methods.

namespace internal {

template<typename ConstRow,
         DataType type,
         bool nullable,
         bool deep_copy>
bool CopyItem(const ConstRow& row,
              size_t column_position,
              size_t output_offset,
              OwnedColumn* output) {
  if (nullable && row.is_null(column_position)) {
    output->mutable_is_null()[output_offset] = true;
    return true;
  }
  if (nullable) output->mutable_is_null()[output_offset] = false;
  DatumCopy<type, deep_copy> copier;
  return copier(row.template typed_notnull_data<type>(column_position),
                output->mutable_typed_data<type>() + output_offset,
                output->arena());
}

template<typename ConstRow>
struct ItemExtractorResolver {
  ItemExtractorResolver(bool nullable, bool deep_copy)
      : nullable(nullable),
        deep_copy(deep_copy) {}

  template<DataType type>
  typename RowCopier<ConstRow>::CopyFn operator()() const {
    return nullable
        ? Resolve1<type, true>()
        : Resolve1<type, false>();
  }

  template<DataType type, bool nullable_p>
  typename RowCopier<ConstRow>::CopyFn Resolve1() const {
    return deep_copy
        ? Resolve2<type, nullable_p, true>()
        : Resolve2<type, nullable_p, false>();
  }

  template<DataType type, bool nullable_p, bool deep_copy_p>
  typename RowCopier<ConstRow>::CopyFn Resolve2() const {
    return &CopyItem<ConstRow, type, nullable_p, deep_copy_p>;
  }

  bool nullable;
  bool deep_copy;
};

template<typename ConstRow>
void CreateDataCopiers(
    const TupleSchema& schema,
    bool deep_copy,
    vector<typename RowCopier<ConstRow>::CopyFn>* copiers) {
  for (size_t i = 0; i < schema.attribute_count(); i++) {
    const Attribute& attribute = schema.attribute(i);
    ItemExtractorResolver<ConstRow> resolver(attribute.is_nullable(),
                                             deep_copy);
    copiers->push_back(
        TypeSpecialization<typename RowCopier<ConstRow>::CopyFn,
                           ItemExtractorResolver<ConstRow> >(
            attribute.type(), resolver));
  }
}

}  // namespace internal

template<typename ConstRow>
RowCopier<ConstRow>::RowCopier(const TupleSchema& schema, bool deep_copy) {
  internal::CreateDataCopiers<ConstRow>(schema, deep_copy, &copiers_);
}

template<typename ConstRow>
bool RowCopier<ConstRow>::Copy(const ConstRow& input,
                               const size_t output_offset,
                               Block* output) const {
  DCHECK(output != NULL) << "Missing output for view copy";
  DCHECK(input.schema().EqualByType(output->schema()));
  bool copied = true;
  for (size_t i = 0; i < copiers_.size(); i++) {
    copied &= copiers_[i](input, i, output_offset, output->mutable_column(i));
  }
  return copied;
}

template<typename ConstRow>
RowCopierWithProjector<ConstRow>::RowCopierWithProjector(
    const BoundSingleSourceProjector* projector,
    bool deep_copy)
    : projector_(projector) {
  internal::CreateDataCopiers<ConstRow>(projector->result_schema(),
                                        deep_copy, &copiers_);
}

template<typename ConstRow>
bool RowCopierWithProjector<ConstRow>::Copy(const ConstRow& input,
                                            const size_t output_offset,
                                            Block* output) const {
  DCHECK(output != NULL) << "Missing output for view copy";
  const TupleSchema& output_schema = projector_->result_schema();
  DCHECK(output_schema.EqualByType(output->schema()));
  bool copied = true;
  for (size_t i = 0; i < copiers_.size(); i++) {
    copied &= copiers_[i](
        input,
        projector_->source_attribute_position(i),
        output_offset,
        output->mutable_column(i));
  }
  return copied;
}

template<typename ConstRow>
MultiRowCopier<ConstRow>::MultiRowCopier(
    const BoundMultiSourceProjector* projector,
    bool deep_copy)
    : projector_(projector) {
  for (size_t i = 0; i < projector_->result_schema().attribute_count(); i++) {
    const Attribute& attribute = projector_->result_schema().attribute(i);
    internal::ItemExtractorResolver<ConstRow> resolver(
        attribute.is_nullable(),
        deep_copy);
    copiers_.push_back(
        TypeSpecialization<typename RowCopier<ConstRow>::CopyFn,
                           internal::ItemExtractorResolver<ConstRow> >(
            attribute.type(), resolver));
  }
}

template<typename ConstRow>
size_t MultiRowCopier<ConstRow>::Copy(
    const vector<const ConstRow*>& input_rows,
    const size_t output_offset,
    Block* output) const {
  DCHECK(projector_->result_schema().EqualByType(output->schema()));
  bool copied = true;
  for (size_t i = 0; i < projector_->result_schema().attribute_count(); i++) {
    copied &= copiers_[i](*input_rows[projector_->source_index(i)],
                          projector_->source_attribute_position(i),
                          output_offset,
                          output->mutable_column(i));
  }
  return copied;
}

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_COPIER_H_
