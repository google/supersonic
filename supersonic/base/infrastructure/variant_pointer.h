// Copyright 2011 Google Inc.  All Rights Reserved
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
// Helpers for data whose type is not known at compile time.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_POINTER_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_POINTER_H_

#include <stddef.h>

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

class StringPiece;

namespace supersonic {

// TODO(user): consider adding debug-mode type safety. (It needs NULL_TYPE
// first, to represent uninitialized pointers).

// Wrapper around const void* that adds a bit of type safety.
// NOTE(user): if/when we want to support BIT, we can add new fields
// here as needed, and specialize methods and constructors.
class VariantPointer {
 public:
  // Creates an uninitialized pointer. (Don't rely on it being initialized).
  VariantPointer() : pointer_(NULL) {}

  // Initialize the pointer given the start address of its memory block.
  VariantPointer(void* pointer) : pointer_(pointer) {}

  // Creates a variant pointer from a typed pointer.
  // Only accepts pointers to types representable in the Supersonic type system.
  template<DataType type>
  VariantPointer(typename TypeTraits<type>::cpp_type* pointer)
      : pointer_(pointer) {}

  // Returns a typed pointer.
  template<DataType type>
  typename TypeTraits<type>::cpp_type* as() const {
    return static_cast<typename TypeTraits<type>::cpp_type*>(pointer_);
  }

  // Returns a typed pointer to variable-length data.
  StringPiece* as_variable_length() const {
    return static_cast<StringPiece*>(pointer_);
  }

  // Returns true if this pointer is NULL.
  bool is_null() const { return pointer_ == NULL; }

  // Returns a pointer equal to this pointer + offset, where the offset
  // specifies the count of typed items, as indicated by type_info.
  VariantPointer offset(int64 offset, const TypeInfo& type_info) const {
    return VariantPointer(
        static_cast<char*>(pointer_) + (offset << type_info.log2_size()));
  }

  // As above, for when the offset is known at compile time.
  template<int64 typed_offset>
  VariantPointer static_offset(const TypeInfo& type_info) const {
    return VariantPointer(static_cast<void*>(
        static_cast<char*>(pointer_) + typed_offset * type_info.size()));
  }

 private:
  friend bool operator==(VariantPointer a, VariantPointer b);
  friend class VariantConstPointer;  // For its copy constructor.
  void* pointer_;
  // Copyable.
};

// Like the above, but for const pointers.
class VariantConstPointer {
 public:
  // Creates an uninitialized pointer. (Don't rely on it being initialized).
  VariantConstPointer() : pointer_(NULL) {}

  // Initialize the pointer given the start address of its memory block.
  VariantConstPointer(const void* pointer) : pointer_(pointer) {}

  // Creates a variant pointer from a typed pointer.
  template<DataType type>
  VariantConstPointer(const typename TypeTraits<type>::cpp_type* pointer)
      : pointer_(pointer) {}

  // Allows using the VariantPointer when the const one is expected.
  VariantConstPointer(const VariantPointer& pointer)                   // NOLINT
      : pointer_(pointer.pointer_) {}

  // Returns a typed pointer.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type* as() const {
    return static_cast<const typename TypeTraits<type>::cpp_type*>(pointer_);
  }

  // Returns a typed pointer to variable-length data.
  const StringPiece* as_variable_length() const {
    return static_cast<const StringPiece*>(pointer_);
  }

  // file_io and copy_column currently use this.
  const void* raw() const { return pointer_; }

  // Returns true if this pointer is NULL.
  bool is_null() const { return pointer_ == NULL; }

  // Returns a pointer equal to this pointer + offset, where the offset
  // specifies the count of typed items, as indicated by type_info.
  VariantConstPointer offset(int64 offset, const TypeInfo& type_info) const {
    return VariantConstPointer(
        static_cast<const char*>(pointer_) + (offset << type_info.log2_size()));
  }

  // As above, for when the offset is known at compile time.
  template<int64 typed_offset>
  VariantConstPointer static_offset(const TypeInfo& type_info) const {
    return VariantConstPointer(static_cast<const void*>(
        static_cast<const char*>(pointer_) + typed_offset * type_info.size()));
  }

 private:
  friend bool operator==(VariantConstPointer a, VariantConstPointer b);
  const void* pointer_;
  // Copyable.
};

inline bool operator==(VariantPointer a, VariantPointer b) {
  return a.pointer_ == b.pointer_;
}

inline bool operator==(VariantConstPointer a, VariantConstPointer b) {
  return a.pointer_ == b.pointer_;
}

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_POINTER_H_
