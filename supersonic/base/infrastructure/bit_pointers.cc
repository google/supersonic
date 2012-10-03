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

#include "supersonic/base/infrastructure/bit_pointers.h"

#include <string.h>

#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

namespace bit_pointer {

bool bit_array::Reallocate(size_t bit_capacity, BufferAllocator* allocator) {
  size_t bytes = ((bit_capacity + 127) / 128) * 16;
  if (data_buffer_.get() == NULL) {
    data_buffer_.reset(allocator->Allocate(bytes));
    return data_buffer_.get() != NULL;
  } else {
    return allocator->Reallocate(bytes, data_buffer_.get()) != NULL;
  }
}

FailureOrVoid bit_array::TryReallocate(size_t bit_capacity,
                                       BufferAllocator* allocator) {
  if (!Reallocate(bit_capacity, allocator)) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        StrCat("Failed to reallocate ",
                               bit_capacity,
                               " rows for a bit vector.")));
  }
  return Success();
}

bool boolean_array::Reallocate(size_t bytes_capacity,
                               BufferAllocator* allocator) {
  if (data_buffer_.get() == NULL) {
    data_buffer_.reset(allocator->Allocate(bytes_capacity));
    return data_buffer_.get() != NULL;
  } else {
    return allocator->Reallocate(bytes_capacity, data_buffer_.get()) != NULL;
  }
}

FailureOrVoid bool_array::TryReallocate(size_t bit_capacity,
                                        BufferAllocator* allocator) {
  if (!Reallocate(bit_capacity, allocator)) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        StrCat("Failed to reallocate ",
                               bit_capacity,
                               " rows for a boolean vector.")));
  }
  return Success();
}

void FillWithTrue(bit_ptr dest, size_t bit_count) {
  // We're going to operate on bytes, not on 32-bit blocks.
  int char_shift = dest.shift() & 7;
  // This is the ugly case - all the bits we insert fit into a single byte.
  // We don't do this case very effectively, but then again it's not really
  // worth optimizing overly.
  if (char_shift + bit_count < 8) {
    while (bit_count--) {
      *dest = true;
      ++dest;
    }
  } else {
    char* dest_ptr = reinterpret_cast<char*>(dest.data());
    // We omit the bytes that were before dest due to dest.shift() > 7.
    dest_ptr += dest.shift() >> 3;
    *dest_ptr |= ((~'\0') << char_shift);
    bit_count -= (8 - char_shift);
    ++dest_ptr;
    // Set the bulk of the bytes.
    memset(dest_ptr, ~0, bit_count / 8);
    // Set the remainder. We want to avoid touching the last byte if we don't
    // write into it, as it may be outside our allocated memory.
    // TODO(onufry): it would be better always to allocate a single byte more
    // and avoid the performance cost of this check. This would however require
    // having this one spare byte kept somewhere in storage for the case of
    // bit_count == 0, because Reallocate(0) has to be guaranteed to succeed
    // (it is called in the block constructor). Reconsider this design when
    // tweaking the performance of bitpointers.
    if ((bit_count & 7) != 0) {
      dest_ptr[bit_count / 8] |= static_cast<char>((1 << (bit_count & 7)) - 1);
    }
  }
}

void FillWithTrue(bool* dest, size_t byte_count) {
  memset(dest, '\1', byte_count * sizeof(*dest));
}

void FillWithFalse(bit_ptr dest, size_t bit_count) {
  // This code is almost identical to the FillWithTrue code, so I'll drop the
  // comments here.
  int char_shift = dest.shift() & 7;
  if (char_shift + bit_count < 8) {
    while (bit_count--) {
      *dest = false;
      ++dest;
    }
  } else {
    char* dest_ptr = reinterpret_cast<char*>(dest.data());
    dest_ptr += dest.shift() >> 3;
    *dest_ptr &= ~((~'\0') << char_shift);
    bit_count -= (8 - dest.shift());
    ++dest_ptr;
    memset(dest_ptr, 0, bit_count / 8);
    // See comment in FillWithTrue.
    if ((bit_count & 7) != 0) {
      dest_ptr[bit_count / 8] &= ~static_cast<char>((1 << (bit_count & 7)) - 1);
    }
  }
}

void FillWithFalse(bool* dest, size_t byte_count) {
  memset(dest, '\0', byte_count * sizeof(*dest));
}

void FillFromAligned(bit_ptr dest, bit_const_ptr source,
                     size_t bit_count) {
  CHECK((dest.shift() & 7) == (source.shift() & 7))
      << "Pointers with different shifts passed to FillFromAligned. Destination"
      << " has shift: " << dest.shift() << ", while source: " << source.shift();
  // We are again operating on bytes, not 4-byte blocks.
  int char_shift = dest.shift() & 7;
  // This is the "short case", where the whole copy fits into a single byte.
  // It is not optimal, but also probably not worth optimizing.
  if (char_shift + bit_count < 8) {
    while (bit_count-- != 0) {
      *dest = *source;
      ++dest;
      ++source;
    }
  } else {
    char* dest_ptr = reinterpret_cast<char*>(dest.data());
    const char* source_ptr = reinterpret_cast<const char*>(source.data());
    // We omit the bytes that were before dest/source due to shift > 7.
    dest_ptr += dest.shift() >> 3;
    source_ptr += source.shift() >> 3;
    // We zero the end (the high-values) of dest, and copy the appropriate part
    // of source onto it.
    // The mask has ones on the last 8-char_shift bits .
    const char mask = ((~'\0') << char_shift);
    *dest_ptr &= ~mask;
    *dest_ptr |= (*source_ptr & mask);
    bit_count -= (8 - char_shift);
    ++dest_ptr;
    ++source_ptr;
    // Fill the bulk of the bytes.
    memcpy(dest_ptr, source_ptr, bit_count / 8);
    // Fill the ending - zero first, then paste source on. The mask has ones
    // on the first bit_count & 7 bits.
    // See comment in FillWithTrue.
    if ((bit_count & 7) != 0) {
      const char mask2 = static_cast<char>((1 << (bit_count & 7)) - 1);
      dest_ptr[bit_count / 8] &= ~mask2;
      dest_ptr[bit_count / 8] |= (source_ptr[bit_count / 8] & mask2);
    }
  }
}

void FillFrom(bit_ptr dest, bit_const_ptr source, size_t bit_count) {
  DCHECK(dest != NULL);
  DCHECK(source != NULL);
  if ((dest.shift() & 7) == (source.shift() & 7)) {
    FillFromAligned(dest, source, bit_count);
    return;
  }
  // We go to a 4-byte border on the source pointer. We use 32-bit blocks to
  // optimize for the case of bit_count around 1024 (the typical case for
  // supersonic), in which case we get - on average - 32 operations of copying
  // a 4-byte-block and 32 "unaligned" bits.
  while ((source.shift() != 0
          || (reinterpret_cast<int64>(source.data()) & 3) != 0)
         && bit_count != 0) {
    *dest = *source;
    ++dest;
    ++source;
    --bit_count;
  }
  if (bit_count == 0) return;
  // This cast is guaranteed to be exact.
  const uint32* source_ptr = source.data();
  // The pointers are guaranteed not to be aligned, so dest is in the previous
  // 4-byte block.
  uint32* dest_ptr = dest.data();
  const int shift = dest.shift();
  // Has ones on the part of the 8-byte block in front of dest.
  const uint32 mask = (~0U) << shift;
  int steps = bit_count / 32;
  // We move forward in 4-byte increments.
  while (steps--) {
    // Clear out the end (high) bits of the destination.
    *dest_ptr &= ~mask;
    // Copy the appropriate part of the source onto destination.
    *dest_ptr |= (*source_ptr & (mask >> shift)) << shift;
    ++dest_ptr;
    // Now the second part (beginning of the byte for dest, end for source).
    *dest_ptr &= mask;
    *dest_ptr |= (*source_ptr & (~mask << (32 - shift))) >> (32 - shift);
    ++source_ptr;
  }
  // Finish the remaining bits.
  dest += (bit_count & ~31);
  source += (bit_count & ~31);
  bit_count &= 31;
  while (bit_count --) {
    *dest = *source;
    ++dest;
    ++source;
  }
}

void FillFrom(bool* dest, const bool* source, size_t byte_count) {
  memcpy(dest, source, byte_count * sizeof(*dest));
}

// TODO(onufry): Change to the "compare to zero" SIMD operator.
void SafeFillFrom(bool* dest, const bool* source, size_t byte_count) {
  for (int i = 0; i < byte_count; ++i) dest[i] = normalize(source[i]);
}

// TODO(onufry): Change the inner loop to use PMOVSKB SIMD instruction, from
// http://www.tommesani.com/SSEPrimer.html
void FillFrom(bit_ptr dest, const bool* source, size_t bit_count) {
  int source_position = 0;
  // Deal with the unaligned bits first.
  while (dest.shift() != 0 && bit_count-- > 0) {
    *dest = source[source_position++];
    ++dest;
  }
  source += source_position;
  source_position = 0;
  // Now deal with the bulk of the bits in loops-of-32.
  uint32* dest_data = dest.data();
  for (int i = 0; i < bit_count / 32; ++i) {
    *dest_data = 0;
    for (int m = 0; m < 32; ++m) {
      *dest_data |=
          (static_cast<uint32>(normalize(source[source_position++])) << m);
    }
    ++dest_data;
  }
  // Copy the remainder.
  // See comment in FillWithTrue.
  if ((bit_count & 31) != 0) {
    uint32 &final_data = dest.data()[bit_count / 32];
    final_data &= ~((1 << (bit_count & 31)) - 1);
    for (; source_position < bit_count; source_position++) {
      bool inserted_value = normalize(source[source_position]);
      final_data |= (static_cast<uint32>(inserted_value)
                     << (source_position & 31));
    }
  }
}

void SafeFillFrom(bit_ptr dest, const bool* source, size_t byte_count) {
  FillFrom(dest, source, byte_count);
}

// TODO(onufry): Refactor using a lookup table, as in this link:
// http://software.intel.com/en-us/forums/showthread.php?t=63531
void FillFrom(bool* dest, bit_const_ptr source, size_t bit_count) {
  while (!source.is_aligned() && bit_count > 0) {
    *dest = *source;
    ++dest;
    ++source;
    --bit_count;
  }
  const uint32* source_data = source.data();
  for (int i = 0; i < bit_count / 32; ++i) {
    for (int m = 0; m < 32; ++m) {
      *dest++ = (((*source_data) >> m) & 1);
    }
    ++source_data;
  }
  for (int m = 0; m < (bit_count & 31); ++m) {
    *dest++ = (((*source_data) >> m) & 1);
  }
}

size_t PopCount(bit_const_ptr source, size_t bit_count) {
  size_t result = 0;
  DCHECK(source.is_aligned());
  for (int i = 0; i < bit_count / 32; ++i) {
    result += __builtin_popcount(source.data()[i]);
  }
  // Se comment in FillWithTrue.
  if ((bit_count & 31) != 0) {
    result += __builtin_popcount(
        source.data()[bit_count / 32] & ((1 << (bit_count & 31)) - 1));
  }
  return result;
}

size_t PopCount(const bool* source, size_t byte_count) {
  size_t result = 0;
  for (int i = 0; i < byte_count; ++i) {
    result += !!(*source++);
  }
  return result;
}

}  // namespace bit_pointer

FailureOrVoid BoolBlock::TryReallocate(rowcount_t new_row_capacity) {
  for (int i = 0; i < column_count(); ++i) {
    FailureOrVoid reallocation_result = columns_[i].TryReallocate(
        new_row_capacity, allocator());
    view_.ResetColumn(i, columns_[i].mutable_data());
    if (reallocation_result.is_failure() && new_row_capacity < row_capacity()) {
      view_.set_row_count(new_row_capacity);
    }
    PROPAGATE_ON_FAILURE(reallocation_result);
  }
  view_.set_row_count(new_row_capacity);
  return Success();
}

}  // namespace supersonic
