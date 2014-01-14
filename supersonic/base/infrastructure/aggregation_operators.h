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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// Defines the AssignmentOperator and the AggregationOperator.
// The assignment operator is responsible for writing a new value onto the
// result, and (if necessary) to deep-copy the underlying data to a dedicated
// buffer. The aggregation operator is responsible for aggregating the
// appropriate value to the result. Both assume the values passed in are
// correct, the problem of resolving NULLs should be handled by the caller.
//
// The general usage pattern for any aggregation is that the first value is
// inserted via a AssignmentOperator, and an AggregationOperator is used only
// for the subsequent values (see expression/core/stateful_bound_expressions.h
// and cursor/infrastructure/column_aggregators.h).

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_AGGREGATION_OPERATORS_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_AGGREGATION_OPERATORS_H_

#include <stddef.h>
#include <string.h>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {
namespace aggregations {

// Assigns value to result. If result is variable length and a deep copy is
// asked for (which is the default) allocates buffer to hold result's data and
// stores this buffer in the scoped pointer passed at invocation time. If
// result is constant length, the buffer argument and the allocator passed at
// construction are never actually used. Returns false if the allocation failed
// due to OOM, true otherwise.
template<DataType InputType, DataType OutputType, bool deep_copy = true>
struct AssignmentOperator {
  // Allocator is used only if OutputType is of variable length. This
  // parameter needs to be passed to constructors of all assigment operators to
  // allow to have a common code that creates these operators.
  explicit AssignmentOperator(BufferAllocator* allocator) {}

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    *result = val;
    return true;
  }
};

template<>
struct AssignmentOperator<STRING, STRING, true> {
  explicit AssignmentOperator(BufferAllocator* allocator)
      : allocator_(allocator) {}

  inline bool operator()(const StringPiece& val,
                         StringPiece* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    Buffer* buffer = buffer_ptr->get();
    if (!buffer) {
      buffer = allocator_->Allocate(val.length());
      if (!buffer) {
        return false;
      }
      buffer_ptr->reset(buffer);
    } else if (val.length() > buffer->size()) {
      // Try to grow the buffer x2 to avoid many reallocations, but accept
      // smaller (but still exponential) growth under memory constraints.
      size_t requested = std::max(2 * buffer->size(),
                                  static_cast<size_t>(val.length()));
      size_t minimal = std::max(buffer->size() + buffer->size() / 8,
                                static_cast<size_t>(val.length()));

      if (!allocator_->BestEffortReallocate(requested, minimal, buffer)) {
        return false;
      }
    }
    memcpy(buffer->data(), val.data(), val.length());
    *result = StringPiece(static_cast<char*>(buffer->data()), val.length());
    return true;
  }

 private:
  BufferAllocator* allocator_;
};

template<>
struct AssignmentOperator<BINARY, BINARY, true> {
  explicit AssignmentOperator(BufferAllocator* allocator)
      : string_assignment_operator_(allocator) {}

  inline bool operator()(const StringPiece& val,
                         StringPiece* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    // The BINARY and STRING logic is identical, so we delegate.
    return string_assignment_operator_(val, result, buffer_ptr);
  }

 private:
  AssignmentOperator<STRING, STRING> string_assignment_operator_;
};

template<DataType InputType, bool deep_copy>
struct AssignmentOperator<InputType, STRING, deep_copy> {
  explicit AssignmentOperator(BufferAllocator* allocator)
      : string_assigment_operator_(allocator) {
    // We need a place to allocate for the results of the copy.
    COMPILE_ASSERT(deep_copy,
                   cannot_make_a_shallow_copy_when_converting_to_string);
  }

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         StringPiece* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    string val_string;
    PrintTyped<InputType>(val, &val_string);
    return string_assigment_operator_(val_string, result, buffer_ptr);
  }

 private:
  AssignmentOperator<STRING, STRING> string_assigment_operator_;
};

// A more specialized template, to override the above for shallow string to
// string copies.
template<>
struct AssignmentOperator<STRING, STRING, false> {
  explicit AssignmentOperator(BufferAllocator* allocator) {}

  inline bool operator()(const StringPiece& val,
                         StringPiece* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    *result = val;
    return true;
  }
};

// Functions to compute two arguments aggregations. Return false when there is
// not enough memory to update the result of an aggregation (This can happen
// only when result is variable length and a deep copy is requested).
template<Aggregation, DataType InputType, DataType OutputType,
         bool deep_copy = true>
struct AggregationOperator {
  // If the result is variable length, the buffer (in buffer_ptr) is used to
  // store the variable length data.
  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr);
};

template<DataType InputType, DataType OutputType, bool deep_copy>
struct AggregationOperator<SUM, InputType, OutputType, deep_copy> {
  // All aggregation operators have an allocator parameter for the constructor.
  // It is not used by operators for which OutputType is not variable length,
  // but it is needed to allow to have a common code that creates these
  // operators.
  explicit AggregationOperator(BufferAllocator* allocator) {}

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    *result += val;
    return true;
  }
};

template<DataType InputType, DataType OutputType, bool deep_copy>
struct AggregationOperator<MAX, InputType, OutputType, deep_copy> {
  explicit AggregationOperator(BufferAllocator* allocator) :
      assigment_operator_(allocator) {}

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    if (ThreeWayCompare<OutputType, InputType, false>(*result, val)
        == RESULT_LESS) {
      return assigment_operator_(val, result, buffer_ptr);
    }
    // TODO(onufry): Add a specialization here to deal with NaN values for
    // FLOAT/DOUBLE types.
    return true;
  }

 private:
  AssignmentOperator<InputType, OutputType, deep_copy> assigment_operator_;
};

template<DataType InputType, DataType OutputType, bool deep_copy>
struct AggregationOperator<MIN, InputType, OutputType, deep_copy> {
  explicit AggregationOperator(BufferAllocator* allocator)
      : assigment_operator_(allocator) {}

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    if (ThreeWayCompare<InputType, OutputType, false>(val, *result)
        == RESULT_LESS) {
      return assigment_operator_(val, result, buffer_ptr);
    }
    // TODO(onufry): Add a specialization here to deal with NaN values for
    // FLOAT/DOUBLE types.
    return true;
  }

 private:
  AssignmentOperator<InputType, OutputType, deep_copy> assigment_operator_;
};

// Note - the Concat implementation assumes (to avoid doing unnecessary string
// copying at every point) that the current state of the concatenation is
// actually stored in the buffer. A NULL input buffer is interpreted as an empty
// string.
template<DataType InputType, bool deep_copy>
struct AggregationOperator<CONCAT, InputType, STRING, deep_copy> {
  explicit AggregationOperator(BufferAllocator* allocator)
      : allocator_(allocator) {
    // As we create a new string as the result of the concatenation, we need
    // deep_copy to be set to true, as we will use memory.
    COMPILE_ASSERT(deep_copy,
                   concat_needs_deep_copy_set_to_true);
  }

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         StringPiece* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    Buffer* buffer = buffer_ptr->get();
    // If the buffer is NULL, we expect the string passed in to be empty.
    DCHECK(buffer != NULL || result->length() == 0);
    // If the result is not NULL, we expect the string passed in to be stored in
    // the buffer.
    DCHECK(buffer == NULL || buffer->data() == result->data());
    size_t current_string_length = result->length();
    AsString<InputType> as_string;
    StringPiece val_string = as_string(val);
    // Make sure the buffer is large enough to hold old result, char ',' and the
    // val converted to string.
    size_t concat_result_size = current_string_length + val_string.length() + 1;
    if (buffer == NULL) {
      buffer = allocator_->BestEffortAllocate(2 * concat_result_size,
                                              concat_result_size);
      if (buffer == NULL) return false;
      buffer_ptr->reset(buffer);
    }
    if (concat_result_size > buffer->size()) {
      // Try to grow buffer to be two times bigger then needed to avoid many
      // reallocations.
      if (!allocator_->BestEffortReallocate(2 * concat_result_size,
                                            concat_result_size,
                                            buffer)) {
        return false;
      }
    }
    DCHECK(buffer != NULL);
    static_cast<char*>(buffer->data())[current_string_length] = ',';
    if (val_string.length()) {
      memcpy(static_cast<char*>(buffer->data()) + current_string_length + 1,
             val_string.data(), val_string.length());
    }
    *result = StringPiece(static_cast<char*>(buffer->data()),
                          concat_result_size);
    return true;
  }

 private:
  BufferAllocator* allocator_;
};

template<DataType InputType, DataType OutputType, bool deep_copy>
struct AggregationOperator<FIRST, InputType, OutputType, deep_copy> {
  explicit AggregationOperator(BufferAllocator* allocator) {}

  // The first entry of the group is aggregated via the assignment operator, and
  // not via the AggregationOperator (see comment at beginning of file). For
  // the next entries we need not do anything.
  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    return true;
  }
};

template<DataType InputType, DataType OutputType, bool deep_copy>
struct AggregationOperator<LAST, InputType, OutputType, deep_copy> {
  explicit AggregationOperator(BufferAllocator* allocator)
      : assigment_operator_(allocator) {}

  inline bool operator()(const typename TypeTraits<InputType>::cpp_type& val,
                         typename TypeTraits<OutputType>::cpp_type* result,
                         std::unique_ptr<Buffer>* buffer_ptr) {
    return assigment_operator_(val, result, buffer_ptr);
  }

 private:
  AssignmentOperator<InputType, OutputType, deep_copy> assigment_operator_;
};

}  // namespace aggregations
}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_AGGREGATION_OPERATORS_H_
