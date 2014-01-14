// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// Contains definitions for bit pointers - pointers to particular bits. We use
// them to iterate over and access bit-oriented data (which is simply boolean
// data stored in a more compact fashion). A bit_ptr points to a particular bit.
// The incrementation and decrementation of a bit_ptr occurs bit-wise, thus
// to move the pointer one byte forward one has to increment it by eight.
// The pointer can be dereferenced to a bit_reference, which supports implicit
// casts to and assignments from boolean, thus you can write things like:
//   bit_ptr p(...);
//   for (int i = 0; i < N; ++i) {
//     *p |= (i == 2);
//     if (*p) { ... }
//     ++p;
//   }
//
// The basic intention here is that if you replace your bool arrays by arrays
// 8 times smaller, you can most of the time replace all your bool* pointers
// by bit_ptr pointers, const bool* by bit_const_ptr, and everything should
// work.
//
// Lacking: bit_ptrs do not have the postincrementation and postdecrementation
// operators and do not have the -= and - operator. If you need them, add them.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_BIT_POINTERS_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_BIT_POINTERS_H_

#include <stddef.h>

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/paranoid.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"

// This is the single switch you have to toggle to change the is_null
// representation. If the flag is set to true, we will use the BIT
// representation for the is_null columns, if it is set to false, we will use
// standard C++ booleans.
// At the moment of writing this comment using simple booleans gets better
// performance results, so the flag is set to zero, but the point is to be
// able to switch easily.
#define USE_BITS_FOR_IS_NULL_REPRESENTATION false

// This code is little-endian only. Now, this is (I guess) Google standard, so
// I'm not overly worried about this.
#ifndef IS_LITTLE_ENDIAN
#error "The supersonic bit_pointer code supports little endian only"
#endif

namespace supersonic {

namespace bit_pointer {
// The point of this function is to normalize a boolean to a proper value of
// zero or one, even if it was unnormalized (e.g. taken from uninitialized
// memory).
inline bool normalize(bool value) {
  return !IsSaneBool(value) || value;
}

// A bit_reference is a reference to a single bit (interpreted as a boolean
// true/false value). The reference is stored as a pointer to uint32, and a
// shift (between 0 and 31) describing which bit of the uint32 we are pointing
// to.
class bit_reference {
 public:
  // Accessors.
  inline uint32* data() const { return data_; }
  inline int shift() const { return shift_; }
  inline operator bool() const { return ((*data_) >> shift_) & 1; }

  // Assignment from bool. This assumes that the bool is correctly formed (ie.,
  // it has a numerical value of zero or one).
  inline bit_reference operator=(bool value) {
    bool normalized = bit_pointer::normalize(value);
    // Set to one if value is one.
    *data_ |= (normalized << shift_);
    // Set to zero if value is zero.
    *data_ &= ~((!normalized) << shift_);
    return *this;
  }

  // Quicker (than implementing through the assignment operator) versions of
  // |= and &= operators.
  inline bit_reference operator|=(bool value) {
    *data_ |= (bit_pointer::normalize(value) << shift_);
    return *this;
  }
  bit_reference operator&=(bool value) {
    *data_ &= ~((!bit_pointer::normalize(value)) << shift_);
    return *this;
  }

 private:
  // The bit_ptr needs access to the private constructor of the reference.
  friend class bit_ptr;

  uint32* data_;
  int shift_;

  bit_reference(uint32* data, int shift)
      : data_(CHECK_NOTNULL(data)), shift_(shift) {}

  // Copyable.
};

class bit_const_ptr;

// A bit_ptr is a pointer to a single bit. It increments and decrements
// bit-by-bit.
class bit_ptr {
 public:
  // Constructors. None of them take ownership of data.
  explicit bit_ptr(uint32* data) : data_(data), shift_(0) {}
  bit_ptr(uint32* data, int shift) : data_(data), shift_(shift) {}
  bit_ptr() : data_(NULL), shift_(0) {}

  // Accessors.
  uint32* data() const { return data_; }
  int shift() const { return shift_; }
  bool is_aligned() const { return shift_ == 0; }
  bool is_null() const { return data_ == NULL; }

  // Pointer arithmetics.
  // Incrementation (by a single bit). We increase the shift, and if it wrapped
  // around 32, we reset it to zero and increment data. The bit magic is to
  // avoid branching.
  inline bit_ptr& operator++() {
    DCHECK(data_ != NULL);
    shift_++;
    data_ += shift_ >> 5;
    shift_ &= 31;
    return *this;
  }

  // Decrementation. Similarly as before, but we have to add 32 first, so that
  // we don't wrap around 0 if shift was 0 to begin with.
  inline bit_ptr& operator--() {
    DCHECK(data_ != NULL);
    shift_ |= 32;
    shift_--;
    data_ -= (shift_ >> 5) ^ 1;
    shift_ &= 31;
    return *this;
  }

  inline bit_ptr& operator+=(int offset) {
    DCHECK(data_ != NULL);
    shift_ += offset;
    data_ += (shift_ >> 5);
    shift_ &= 31;
    return *this;
  }

  inline bit_ptr operator+(int offset) const {
    return bit_ptr(*this) += offset;
  }

  // Dereferencing operator.
  inline bit_reference operator*() const {
    return bit_reference(data_, shift_);
  }

  // Array-like dereference. Included for convenience reasons.
  //
  // Note that
  // for (int i = 0; i < row_count; ++i) {
  //   do_something(ptr[i]);
  // }
  // is less efficient than
  // bit_ptr ptr_copy = ptr;
  // for (int i = 0; i < row_count; ++i) {
  //   do_something(*ptr_copy);
  //   ++ptr_copy;
  // }
  // (or even better:
  // for (bit_ptr ptr_copy = ptr; ptr_copy < ptr + row_count(); ++ptr_copy) {
  //   do_something(*ptr_copy);
  // }
  // TODO(onufry): consider going over the code and removing all instances of
  // iteration using array dereferencing, replacing them with iterator loops.
  inline bit_reference operator[](int offset) {
    return *(*this + offset);
  }

  inline bit_ptr& operator=(const bit_ptr& source) {
    data_ = source.data();
    shift_ = source.shift();
    return *this;
  }

  // The comparisons with void* are mainly for the purposes of == NULL
  // comparisons.
  bool operator== (const void* other) const;
  bool operator== (const bit_ptr& other) const;
  bool operator== (const bit_const_ptr& other) const;

  bool operator!= (const void* other) const { return !(*this == other); }
  bool operator!= (const bit_ptr& other) const { return !(*this == other); }
  bool operator!= (const bit_const_ptr& other) const {
    return !(*this == other);
  }

 private:
  uint32* data_;
  int shift_;

  // Copyable, assigneable.
};

// A bit reference is a reference to a single bit. This one is const.
class bit_const_reference {
 public:
  // Constructors.
  bit_const_reference(const bit_reference& source)                     // NOLINT
      : data_(source.data()), shift_(source.shift()) {}

  // Accessors.
  inline const uint32* data() const { return data_; }
  inline int shift() const { return shift_; }
  inline operator bool() const { return (((*data_) >> shift_) & 1) == 1; }

 private:
  const uint32* data_;
  int shift_;

  friend class bit_const_ptr;
  bit_const_reference(const uint32* data, int shift)
      : data_(CHECK_NOTNULL(data)), shift_(shift) {}
  explicit bit_const_reference(const uint32* data)
      : data_(CHECK_NOTNULL(data)) {}
};

// A const bit pointer.
// It serves to replace const bool* in all places, where bit_ptr replaces bool*.
// Note that const bit_ptr is something different (it is akin to bool const*).
class bit_const_ptr {
 public:
  // Constructors.
  bit_const_ptr(const bit_const_ptr& source)
      : data_(source.data()), shift_(source.shift()) {}
  bit_const_ptr(const bit_ptr& source)                                 // NOLINT
      : data_(source.data()), shift_(source.shift()) {}
  explicit bit_const_ptr(const uint32* data) : data_(data), shift_(0) {}
  bit_const_ptr(const uint32* data, int shift) : data_(data), shift_(shift) {}
  bit_const_ptr() : data_(NULL), shift_(0) {}

  // Accessors.
  inline const uint32* data() const { return data_; }
  inline int shift() const { return shift_; }
  inline bool is_aligned() const { return shift_ == 0; }
  inline bool is_null() const { return data_ == NULL; }

  // Pointer arithmetics.
  inline bit_const_ptr& operator++() {
    DCHECK(data_ != NULL);
    shift_++;
    data_ += shift_ >> 5;
    shift_ &= 31;
    return *this;
  }

  inline bit_const_ptr& operator--() {
    DCHECK(data_ != NULL);
    shift_ |= 32;
    shift_--;
    --data_ += (shift_ >> 5);
    shift_ &= 31;
    return *this;
  }

  inline bit_const_ptr& operator+=(int offset) {
    DCHECK(data_ != NULL);
    shift_ += offset;
    data_ += (shift_ >> 5);
    shift_ &= 31;
    return *this;
  }

  inline bit_const_ptr operator+(int offset) const {
    return bit_const_ptr(*this) += offset;
  }

  inline bit_const_reference operator*() const {
    return bit_const_reference(data_, shift_);
  }

  inline bit_const_reference operator[](int offset) const {
    return *(*this + offset);
  }

  inline bit_const_ptr& operator=(const bit_const_ptr& source) {
    data_ = source.data();
    shift_ = source.shift();
    return *this;
  }

  // The comparisons with void* are mainly for the purpose of a == NULL
  // comparison.
  bool operator== (const void* other) const;
  bool operator== (const bit_const_ptr& other) const;
  bool operator== (const bit_ptr& other) const;

  bool operator!= (const void* other) const { return !(*this == other); }
  bool operator!= (const bit_const_ptr& other) const {
    return !(*this == other);
  }
  bool operator!= (const bit_ptr& other) const { return !(*this == other); }

 private:
  const uint32* data_;
  int shift_;

  // Copyable, assigneable.
};

// An array owning an area of memory that will be interpreted as bit arrays
// (that is arrays of boolean values stored bit-by-bit).
class bit_array {
 public:
  bit_array() {}

  // Reallocates to a given size. Allocates a number of _bits_ no smaller than
  // row_capacity, and guarantees that the number of _bits_ allocated is
  // divisible by 128. If allocator guarantees that a call to allocate gives
  // aligned memory, this function will allocate aligned memory. Cannot fail
  // for row_capacity == 0 if allocator cannot fail on a request for zero bytes.
  bool Reallocate(size_t bit_capacity, BufferAllocator* allocator);
  // A wrapper around Reallocate. Throws a memory exceeded exception on failure.
  FailureOrVoid TryReallocate(size_t bit_capacity, BufferAllocator* allocator);

  bit_ptr mutable_data() const {
    return (data_buffer_ == NULL) ? bit_ptr() :
        bit_ptr(reinterpret_cast<uint32*>(data_buffer_->data()));
  }

  bit_const_ptr const_data() const {
    return (data_buffer_ == NULL) ? bit_const_ptr() :
        bit_const_ptr(reinterpret_cast<uint32*>(data_buffer_->data()));
  }

 private:
  scoped_ptr<Buffer> data_buffer_;

  DISALLOW_COPY_AND_ASSIGN(bit_array);
};

// Same as above, only the data is represented by booleans, and not by bits.
class boolean_array {
 public:
  boolean_array() {}

  // Again, same as above.
  bool Reallocate(size_t bytes_capacity, BufferAllocator* allocator);
  FailureOrVoid TryReallocate(size_t bit_capacity, BufferAllocator* allocator);

  bool* mutable_data() const {
    return (data_buffer_ == NULL) ? NULL :
        reinterpret_cast<bool*>(data_buffer_->data());
  }

  const bool* const_data() const {
    return (data_buffer_ == NULL) ? NULL :
        reinterpret_cast<const bool*>(data_buffer_->data());
  }

 private:
  scoped_ptr<Buffer> data_buffer_;

  DISALLOW_COPY_AND_ASSIGN(boolean_array);
};

// A static (that is not reallocatable) bit_array. Owns the area of memory
// sufficient to contain size bit (that's _bits_, not bytes). Guarantees
// that the data() pointers are 16-bits aligned.
//
// We want to avoid code bloat, so we will expilicitly specify which
// arrays are available. Please add to the list carefully.
template <size_t size>
class static_bit_array {
 public:
  // The only accepted sizes are 10 and 1024.
  static_bit_array() {
    COMPILE_ASSERT(size == 10 || size == 1024,
                   size_of_static_array_not_declared_legal);
  }

  bit_ptr mutable_data() { return bit_ptr(aligned_buffer()); }
  bit_const_ptr const_data() { return bit_const_ptr(aligned_buffer()); }
  static const size_t capacity() { return size; }

 private:
  // We allocate more memory than necessary, to be able to assure that w
  // return a pointer to 16-byte-aligned data.
  uint32 buffer_[(size / 32) + 4];                                      //NOLINT

  // Returns an incrementation of buffer_ to the next 16-byte aligned value.
  uint32* aligned_buffer() {
    int64 buf_address = reinterpret_cast<int64>(buffer_);
    // The number of bytes we have to shift to get 16-byte aligned data.
    int64 shift = (~buf_address + 1LL) & 15LL;
    // The purpose of converting to char and back again is to be able to move
    // forward in bytes (instead of in quads of bytes, as would happen when
    // incrementing a uint32* pointer).
    return
      reinterpret_cast<uint32*>(reinterpret_cast<char*>(buffer_) + shift);
  }

  DISALLOW_COPY_AND_ASSIGN(static_bit_array);
};

template <size_t size>
class static_boolean_array {
 public:
  // The only accepted sizes are 10 and 1024.
  static_boolean_array() {
    COMPILE_ASSERT(size == 10 || size == 1024,
                   size_of_static_array_not_declared_legal);
  }

  bool* mutable_data() { return buffer_; }
  const bool* const_data() { return buffer_; }
  static const size_t capacity() { return size; }

 private:
  // Lint thinks this is a variable size array and protests, though the size is
  // known at compile-time.
  bool buffer_[size];                                                   //NOLINT

  DISALLOW_COPY_AND_ASSIGN(static_boolean_array);
};

// Various functions concerned with manipulating bit pointers.
// Number of bytes needed to store bit_count bits.
inline size_t byte_count(size_t bit_count) { return (bit_count + 7) / 8; }

// Number of int32 units needed to store bit_count bits.
inline size_t int_count(size_t bit_count) { return (bit_count + 31) / 32; }

// Fills bit_count bits, beginning with *ptr, with true values.
void FillWithTrue(bit_ptr dest, size_t bit_count);
void FillWithTrue(bool* dest, size_t byte_count);

// Same, but fills with false.
void FillWithFalse(bit_ptr dest, size_t bit_count);
void FillWithFalse(bool* dest, size_t byte_count);

// Copies bit_count bits from source to dest. Will be much slower if the
// pointers are not byte-aligned (that is, their offset with respect to the
// beginning of a byte is different).
void FillFrom(bit_ptr dest, bit_const_ptr source, size_t bit_count);
void FillFrom(bool* dest, const bool* source, size_t byte_count);

// The same as above, but will work only for byte-equialigned pointers (that is,
// source.shift() & 7 == dest.shift() & 7), CHECKs for this. This is the
// efficient way of copying bit data.
void FillFromAligned(bit_ptr dest, bit_const_ptr source, size_t bit_count);

// Copies bit_count values from a boolean array to dest (compressing 8 times).
void FillFrom(bit_ptr dest, const bool* source, size_t bit_count);

// Safe copying from bool values - works even if the source contains "illegal"
// values, differing from 0 and 1. The output values are normalized.
void SafeFillFrom(bool* dest, const bool* source, size_t bit_count);
void SafeFillFrom(bit_ptr dest, const bool* source, size_t bit_count);

// Copies bit_count values from a bit_ptr to a boolean array dest (decompressing
// 8 times).
void FillFrom(bool* dest, bit_const_ptr source, size_t bit_count);

// Counts the number of true values in the first bit_count bits of source.
// Can be used only on aligned pointers.
size_t PopCount(bit_const_ptr source, size_t bit_count);
size_t PopCount(const bool* source, size_t byte_count);

// Implementation details - equality operators.
inline bool bit_ptr::operator==(const bit_const_ptr& other) const {
  return (data_ == other.data() && shift_ == other.shift());
}

inline bool bit_const_ptr::operator==(const bit_ptr& other) const {
  return (data_ == other.data() && shift_ == other.shift());
}

inline bool bit_ptr::operator==(const bit_ptr& other) const {
  return (data_ == other.data() && shift_ == other.shift());
}

inline bool bit_const_ptr::operator==(const bit_const_ptr& other) const {
  return (data_ == other.data() && shift_ == other.shift());
}

inline bool bit_ptr::operator==(const void* other) const {
  return (data_ == other && shift_ == 0);
}

inline bool bit_const_ptr::operator==(const void* other) const {
  return (data_ == other && shift_ == 0);
}
}  // namespace bit_pointer

#if USE_BITS_FOR_IS_NULL_REPRESENTATION == true
typedef bit_pointer::bit_ptr bool_ptr;
typedef bit_pointer::bit_const_ptr bool_const_ptr;
typedef bit_pointer::bit_array bool_array;
typedef bit_pointer::static_bit_array<10> small_bool_array;
typedef bit_pointer::static_bit_array<1024> large_bool_array;
#endif
#if USE_BITS_FOR_IS_NULL_REPRESENTATION == false
typedef bool* bool_ptr;
typedef const bool* bool_const_ptr;
typedef bit_pointer::boolean_array bool_array;
typedef bit_pointer::static_boolean_array<10> small_bool_array;
typedef bit_pointer::static_boolean_array<1024> large_bool_array;
#endif
#undef USE_BITS_FOR_IS_NULL_REPRESENTATION

// This is a class encapsulating a number of bool_ptrs. This can either come
// from a block, or be constructed by hand (this is an equivalent of a
// projection, but at least for the moment introducing projections into this
// stuff seems like a bit of an overkill.
class BoolView {
 public:
  // An empty view with the specified number of columns.
  explicit BoolView(size_t column_count)
      : columns_(new bool_ptr[column_count]),
        column_count_(column_count),
        row_count_(0) {
    for (int i = 0; i < column_count; ++i) columns_[i] = bool_ptr(NULL);
  }

  // A one-column view encapsulating the bit_pointer - a convenience
  // constructor.
  explicit BoolView(bool_ptr data)
      : columns_(new bool_ptr[1]),
        column_count_(1),
        row_count_(0) {
    ResetColumn(0, data);
  }

  const int column_count() const { return column_count_; }
  const rowcount_t row_count() const { return row_count_; }

  bool_ptr column(int column_index) const {
    DCHECK(column_index < column_count_);
    return columns_[column_index];
  }

  // Resets a column to point to another piece of data.
  void ResetColumn(int column_index, bool_ptr data) {
    DCHECK(column_index < column_count_);
    columns_[column_index] = data;
  }

  void set_row_count(rowcount_t row_count) {
    row_count_ = row_count;
  }

 private:
  scoped_ptr<bool_ptr[]> columns_;
  int column_count_;
  rowcount_t row_count_;
};

// This is a class encapsulating a number of unnamed bool_arrays, for easy
// allocation and reallocation. Is similar to the general block (see block.h).
class BoolBlock {
 public:
  // Creates a block with the given number of columns. It is not ready to use
  // before the first reallocate, this contract is different from the block
  // contract.
  BoolBlock(int column_count, BufferAllocator* allocator)
      : allocator_(allocator),
        column_count_(column_count),
        columns_(new bool_array[column_count]),
        view_(column_count) {}

  // Reallocates all the arrays with capacity for row_capacity rows. Can be
  // called multiple times.
  // If failed, some of the buffers may have been reallocated. The block's
  // capacity is not increased, it might be decreased if the new capacity is
  // lower than the demanded capacity. Accessing the new capacity number of rows
  // is still safe.
  // Throws a memory exceeded exception on failure.
  FailureOrVoid TryReallocate(rowcount_t new_row_capacity);

  // Exposes a (mutable) view over the block's columns. Note that there is no
  // need to expose the arrays themselves - we do not want the user to call
  // Reallocate on the arrays, just to be able to modify the data stored, thus
  // we give access to the data through the view (which exposes pointers, and
  // not arrays).
  const BoolView& view() const { return view_; }

  BufferAllocator* allocator() { return allocator_; }

  const int column_count() const { return column_count_; }

  const rowcount_t row_capacity() const { return view().row_count(); }

 private:
  BufferAllocator* allocator_;
  int column_count_;
  scoped_ptr<bool_array[]> columns_;
  BoolView view_;

  DISALLOW_COPY_AND_ASSIGN(BoolBlock);
};

}  // namespace supersonic
#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_BIT_POINTERS_H_
