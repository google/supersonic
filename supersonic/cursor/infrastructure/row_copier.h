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

template <typename RowReader, typename RowWriter>
class RowCopier {
 public:
  typedef bool (*CopyFn)(const RowReader& reader,
                         const typename RowReader::ValueType& input,
                         size_t input_column_index,
                         const RowWriter& writer,
                         typename RowWriter::ValueType* output,
                         size_t output_column_index);

  // Creates a copier that copies all columns from the given schema.
  RowCopier(const TupleSchema& schema, bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  bool Copy(const RowReader& reader, const typename RowReader::ValueType& input,
            const RowWriter& writer, typename RowWriter::ValueType* output)
      const;

 private:
  vector<CopyFn> copiers_;
};

template <typename RowReader, typename RowWriter>
class RowCopierWithProjector {
 public:
  // Variant that only copies projected columns; input and output schemas
  // are given by the bound projector, which must outlive this instance
  // of the RowCopier.
  RowCopierWithProjector(const BoundSingleSourceProjector* projector,
                         bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  bool Copy(const RowReader& reader, const typename RowReader::ValueType& input,
            const RowWriter& writer, typename RowWriter::ValueType* output)
      const;

 private:
  const BoundSingleSourceProjector* projector_;
  vector<typename RowCopier<RowReader, RowWriter>::CopyFn> copiers_;
};

// Copies multiple source rows into a destination block. A projector must
// be supplied to indicate the mapping from many sources to a single
// destination.
template<typename RowReader, typename RowWriter>
class MultiRowCopier {
 public:
  // Input and output schemas are given by the bound projector, which must
  // outlive this instance of ViewMultiCopier
  MultiRowCopier(const BoundMultiSourceProjector* projector, bool deep_copy);

  // Returns true if successfully copied; false otherwise.
  size_t Copy(const vector<const RowReader*>& readers,
              const vector<const typename RowReader::ValueType*>& inputs,
              const RowWriter& writer,
              typename RowWriter::ValueType* output) const;

 private:
  const BoundMultiSourceProjector* projector_;
  vector<typename RowCopier<RowReader, RowWriter>::CopyFn> copiers_;
};


// Implementation of template methods.

namespace internal {

template<typename RowReader,
         typename RowWriter,
         DataType type,
         bool nullable,
         bool deep_copy>
struct ItemCopy {
  bool operator()(const RowReader& reader,
                  const typename RowReader::ValueType& input,
                  size_t input_column_index,
                  const RowWriter& writer,
                  typename RowWriter::ValueType* output,
                  size_t output_column_index) {
    COMPILE_ASSERT(!TypeTraits<type>::is_variable_length,
                   Simple_ItemCopy_called_for_variable_length_argument);
    if (nullable && reader.is_null(input, input_column_index)) {
      writer.set_null(output_column_index, output);
    } else {
      writer.template set_value<type, nullable>(
          output_column_index,
          reader.template fetch<type>(input, input_column_index),
          output);
    }
    return true;
  }
};

template<typename RowReader,
         typename RowWriter,
         DataType type,
         bool nullable,
         bool deep_copy>
bool CopyVariableLengthItem(const RowReader& reader,
                            const typename RowReader::ValueType& input,
                            size_t input_column_index,
                            const RowWriter& writer,
                            typename RowWriter::ValueType* output,
                            size_t output_column_index) {
  COMPILE_ASSERT(TypeTraits<type>::is_variable_length,
                 Variable_length_ItemCopy_called_for_simple_argument);
  if (nullable && reader.is_null(input, input_column_index)) {
    writer.set_null(output_column_index, output);
    return true;
  } else {
    StringPiece datum;
    if (!reader.template fetch_variable_length_using_allocator<
        type, deep_copy, typename RowWriter::Allocator>(
        input, input_column_index, &datum,
        writer.allocator(output_column_index, output))) {
      return false;
    }
    writer.template set_value<type, nullable>(output_column_index, datum,
                                              output);
    return true;
  }
}

// A specialization for STRING.
template<typename RowReader,
         typename RowWriter,
         bool nullable,
         bool deep_copy>
struct ItemCopy<RowReader, RowWriter, STRING, nullable, deep_copy> {
  bool operator()(
      const RowReader& reader,
      const typename RowReader::ValueType& input,
      size_t input_column_index,
      const RowWriter& writer,
      typename RowWriter::ValueType* output,
      size_t output_column_index) const {
    return CopyVariableLengthItem<RowReader, RowWriter, STRING, nullable,
                                  deep_copy>(
        reader, input, input_column_index, writer, output, output_column_index);
  }
};

// A specialization for BINARY.
template<typename RowReader,
         typename RowWriter,
         bool nullable,
         bool deep_copy>
struct ItemCopy<RowReader, RowWriter, BINARY, nullable, deep_copy> {
  bool operator()(
      const RowReader& reader,
      const typename RowReader::ValueType& input,
      size_t input_column_index,
      const RowWriter& writer,
      typename RowWriter::ValueType* output,
      size_t output_column_index) const {
    return CopyVariableLengthItem<RowReader, RowWriter, BINARY, nullable,
                                  deep_copy>(
        reader, input, input_column_index, writer, output, output_column_index);
  }
};

// Definition of the functions that we will be calling via pointers.
template<typename RowReader,
         typename RowWriter,
         DataType type,
         bool nullable,
         bool deep_copy>
bool CopyItem(const RowReader& reader,
              const typename RowReader::ValueType& input,
              size_t input_column_index,
              const RowWriter& writer,
              typename RowWriter::ValueType* output,
              size_t output_column_index) {
  ItemCopy<RowReader, RowWriter, type, nullable, deep_copy> copy;
  return copy(reader, input, input_column_index,
              writer, output, output_column_index);
}


template<typename RowReader, typename RowWriter>
struct ItemExtractorResolver {
  ItemExtractorResolver(bool nullable, bool deep_copy)
      : nullable(nullable),
        deep_copy(deep_copy) {}

  template<DataType type>
  typename RowCopier<RowReader, RowWriter>::CopyFn operator()() const {
    return nullable
        ? Resolve1<type, true>()
        : Resolve1<type, false>();
  }

  template<DataType type, bool nullable_p>
  typename RowCopier<RowReader, RowWriter>::CopyFn Resolve1() const {
    return deep_copy
        ? Resolve2<type, nullable_p, true>()
        : Resolve2<type, nullable_p, false>();
  }

  template<DataType type, bool nullable_p, bool deep_copy_p>
  typename RowCopier<RowReader, RowWriter>::CopyFn Resolve2() const {
    return &CopyItem<RowReader, RowWriter, type, nullable_p, deep_copy_p>;
  }

  bool nullable;
  bool deep_copy;
};

template<typename RowReader, typename RowWriter>
void CreateDataCopiers(
    const TupleSchema& schema,
    bool deep_copy,
    vector<typename RowCopier<RowReader, RowWriter>::CopyFn>* copiers) {
  for (size_t i = 0; i < schema.attribute_count(); i++) {
    const Attribute& attribute = schema.attribute(i);
    ItemExtractorResolver<RowReader, RowWriter> resolver(
        attribute.is_nullable(), deep_copy);
    copiers->push_back(
        TypeSpecialization<typename RowCopier<RowReader, RowWriter>::CopyFn,
                           ItemExtractorResolver<RowReader, RowWriter> >(
            attribute.type(), resolver));
  }
}

}  // namespace internal

template<typename RowReader, typename RowWriter>
RowCopier<RowReader, RowWriter>::RowCopier(const TupleSchema& schema,
                                         bool deep_copy) {
  internal::CreateDataCopiers<RowReader, RowWriter>(schema, deep_copy,
                                                    &copiers_);
}

template<typename RowReader, typename RowWriter>
bool RowCopier<RowReader, RowWriter>::Copy(
    const RowReader& reader,
    const typename RowReader::ValueType& input,
    const RowWriter& writer,
    typename RowWriter::ValueType* output) const {
  DCHECK(output != NULL) << "Missing output for view copy";
  DCHECK(reader.schema(input).EqualByType(writer.schema(output)));
  bool copied = true;
  for (size_t i = 0; i < copiers_.size(); i++) {
    copied &= copiers_[i](reader, input, i, writer, output, i);
  }
  return copied;
}

template<typename RowReader, typename RowWriter>
RowCopierWithProjector<RowReader, RowWriter>::RowCopierWithProjector(
    const BoundSingleSourceProjector* projector,
    bool deep_copy)
    : projector_(projector) {
  internal::CreateDataCopiers<RowReader, RowWriter>(projector->result_schema(),
                                                    deep_copy, &copiers_);
}

template<typename RowReader, typename RowWriter>
bool RowCopierWithProjector<RowReader, RowWriter>::Copy(
    const RowReader& reader,
    const typename RowReader::ValueType& input,
    const RowWriter& writer,
    typename RowWriter::ValueType* output) const {
  DCHECK(output != NULL) << "Missing output for view copy";
  const TupleSchema& output_schema = projector_->result_schema();
  DCHECK(output_schema.EqualByType(writer.schema(output)));
  bool copied = true;
  for (size_t i = 0; i < copiers_.size(); i++) {
    copied &= copiers_[i](reader, input,
                          projector_->source_attribute_position(i),
                          writer, output, i);
  }
  return copied;
}

template<typename RowReader, typename RowWriter>
MultiRowCopier<RowReader, RowWriter>::MultiRowCopier(
    const BoundMultiSourceProjector* projector,
    bool deep_copy)
    : projector_(projector) {
  for (size_t i = 0; i < projector_->result_schema().attribute_count(); i++) {
    const Attribute& attribute = projector_->result_schema().attribute(i);
    internal::ItemExtractorResolver<RowReader, RowWriter> resolver(
        attribute.is_nullable(),
        deep_copy);
    copiers_.push_back(
        TypeSpecialization<
            typename RowCopier<RowReader, RowWriter>::CopyFn,
            internal::ItemExtractorResolver<RowReader, RowWriter> >(
            attribute.type(), resolver));
  }
}

template<typename RowReader, typename RowWriter>
size_t MultiRowCopier<RowReader, RowWriter>::Copy(
    const vector<const RowReader*>& readers,
    const vector<const typename RowReader::ValueType*>& inputs,
    const RowWriter& writer,
    typename RowWriter::ValueType* output) const {
  DCHECK(projector_->result_schema().EqualByType(output->schema()));
  bool copied = true;
  for (size_t i = 0; i < projector_->result_schema().attribute_count(); i++) {
    copied &= copiers_[i](*readers[projector_->source_index(i)],
                          *inputs[projector_->source_index(i)],
                          projector_->source_attribute_position(i),
                          writer, output, i);
  }
  return copied;
}

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_COPIER_H_
