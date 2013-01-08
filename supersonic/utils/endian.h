// Copyright 2005 Google Inc.
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
// ---
//
//
// Utility functions that depend on bytesex. We define htonll and ntohll,
// as well as "Google" versions of all the standards: ghtonl, ghtons, and
// so on. These functions do exactly the same as their standard variants,
// but don't require including the dangerous netinet/in.h.
//
// Buffer routines will copy to and from buffers without causing
// a bus error when the architecture requires differnt byte alignments
#ifndef UTIL_ENDIAN_ENDIAN_H_
#define UTIL_ENDIAN_ENDIAN_H_

#include <assert.h>

#include "supersonic/utils/casts.h"
#include "supersonic/utils/int128.h"
#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/port.h"

inline uint64 gbswap_64(uint64 host_int) {
#if defined(__GNUC__) && defined(__x86_64__) && !defined(__APPLE__)
  // Adapted from /usr/include/byteswap.h.  Not available on Mac.
  if (__builtin_constant_p(host_int)) {
    return __bswap_constant_64(host_int);
  } else {
    register uint64 result;
    __asm__("bswap %0" : "=r" (result) : "0" (host_int));
    return result;
  }
#elif defined(bswap_64)
  return bswap_64(host_int);
#else
  return static_cast<uint64>(bswap_32(static_cast<uint32>(host_int >> 32))) |
    (static_cast<uint64>(bswap_32(static_cast<uint32>(host_int))) << 32);
#endif  // bswap_64
}

#ifdef IS_LITTLE_ENDIAN

// Definitions for ntohl etc. that don't require us to include
// netinet/in.h. We wrap bswap_32 and bswap_16 in functions rather
// than just #defining them because in debug mode, gcc doesn't
// correctly handle the (rather involved) definitions of bswap_32.
// gcc guarantees that inline functions are as fast as macros, so
// this isn't a performance hit.
inline uint16 ghtons(uint16 x) { return bswap_16(x); }
inline uint32 ghtonl(uint32 x) { return bswap_32(x); }
inline uint64 ghtonll(uint64 x) { return gbswap_64(x); }

#elif defined IS_BIG_ENDIAN

// These definitions are simpler on big-endian machines
// These are functions instead of macros to avoid self-assignment warnings
// on calls such as "i = ghtnol(i);".  This also provides type checking.
inline uint16 ghtons(uint16 x) { return x; }
inline uint32 ghtonl(uint32 x) { return x; }
inline uint64 ghtonll(uint64 x) { return x; }

#else
#error "Unsupported bytesex: Either IS_BIG_ENDIAN or IS_LITTLE_ENDIAN must be defined"  // NOLINT
#endif  // bytesex

// This one is safe to take as it's an extension
#define htonll(x) ghtonll(x)

// ntoh* and hton* are the same thing for any size and bytesex,
// since the function is an involution, i.e., its own inverse.
#define gntohl(x) ghtonl(x)
#define gntohs(x) ghtons(x)
#define gntohll(x) ghtonll(x)
#define ntohll(x) htonll(x)

// We provide a unified FromHost API for all integral types and float,
// double types. If variable v's type is known to be one of these types,
// client can simply call the following function without worrying about its
// return type.
//     LittleEndian::FromHost(v), or BigEndian::FromHost(v)
// This unified FromHost API is useful inside template when the type of
// v is a template parameter.
//
// In order to unify all "IntType FromHostxx(ValueType)" API, we
// use the following trait class to automatically find the corresponding
// IntType given a ValueType, where IntType is an unsigned integer type
// with the same size of ValueType. The supported ValueTypes are uint8,
// uint16, uint32, uint64, int8, int16, int32, int64, bool, float, double.
//
// template <class ValueType>
// struct fromhost_value_type_traits {
//   typedef ValueType value_type;
//   typedef IntType int_type;
// }
//
// We don't provide the default implementation for this trait struct.
// So that if ValueType is not supported by FromHost API, it will give
// a compile time error.
template <class ValueType> struct fromhost_value_type_traits;

// General byte order converter class template. It provides a common
// implementation for LittleEndian::FromHost(ValueType) and
// BigEndian::FromHost(ValueType).
template <class EndianClass, typename ValueType>
class GeneralFormatConverter {
 public:
  static typename fromhost_value_type_traits<ValueType>::int_type
  FromHost(ValueType v);
};

// Utilities to convert numbers between the current hosts's native byte
// order and little-endian byte order
//
// Load/Store methods are alignment safe
class LittleEndian {
 public:
  // Conversion functions.
#ifdef IS_LITTLE_ENDIAN

  static uint16 FromHost16(uint16 x) { return x; }
  static uint16 ToHost16(uint16 x) { return x; }

  static uint32 FromHost32(uint32 x) { return x; }
  static uint32 ToHost32(uint32 x) { return x; }

  static uint64 FromHost64(uint64 x) { return x; }
  static uint64 ToHost64(uint64 x) { return x; }

  static bool IsLittleEndian() { return true; }

#elif defined IS_BIG_ENDIAN

  static uint16 FromHost16(uint16 x) { return bswap_16(x); }
  static uint16 ToHost16(uint16 x) { return bswap_16(x); }

  static uint32 FromHost32(uint32 x) { return bswap_32(x); }
  static uint32 ToHost32(uint32 x) { return bswap_32(x); }

  static uint64 FromHost64(uint64 x) { return gbswap_64(x); }
  static uint64 ToHost64(uint64 x) { return gbswap_64(x); }

  static bool IsLittleEndian() { return false; }

#endif /* ENDIAN */

  // Unified LittleEndian::FromHost(ValueType v) API.
  template <class ValueType>
  static typename fromhost_value_type_traits<ValueType>::int_type
  FromHost(ValueType v) {
    return GeneralFormatConverter<LittleEndian, ValueType>::FromHost(v);
  }

  // Functions to do unaligned loads and stores in little-endian order.
  static uint16 Load16(const void *p) {
    return ToHost16(UNALIGNED_LOAD16(p));
  }

  static void Store16(void *p, uint16 v) {
    UNALIGNED_STORE16(p, FromHost16(v));
  }

  static uint32 Load32(const void *p) {
    return ToHost32(UNALIGNED_LOAD32(p));
  }

  static void Store32(void *p, uint32 v) {
    UNALIGNED_STORE32(p, FromHost32(v));
  }

  static uint64 Load64(const void *p) {
    return ToHost64(UNALIGNED_LOAD64(p));
  }

  // Build a uint64 from 1-8 bytes.
  // 8 * len least significant bits are loaded from the memory with
  // LittleEndian order. The 64 - 8 * len most significant bits are
  // set all to 0.
  // In latex-friendly words, this function returns:
  //     $\sum_{i=0}^{len-1} p[i] 256^{i}$, where p[i] is unsigned.
  //
  // This function is equivalent with:
  // uint64 val = 0;
  // memcpy(&val, p, len);
  // return ToHost64(val);
  // TODO(user): write a small benchmark and benchmark the speed
  // of a memcpy based approach.
  //
  // For speed reasons this function does not work for len == 0.
  // The caller needs to guarantee that 1 <= len <= 8.
  static uint64 Load64VariableLength(const void * const p, int len) {
    assert(len >= 1 && len <= 8);
    const char * const buf = static_cast<const char * const>(p);
    uint64 val = 0;
    --len;
    do {
      val = (val << 8) | buf[len];
      // (--len >= 0) is about 10 % faster than (len--) in some benchmarks.
    } while (--len >= 0);
    // No ToHost64(...) needed. The bytes are accessed in little-endian manner
    // on every architecture.
    return val;
  }

  static void Store64(void *p, uint64 v) {
    UNALIGNED_STORE64(p, FromHost64(v));
  }

  static uint128 Load128(const void *p) {
    return uint128(
        ToHost64(UNALIGNED_LOAD64(reinterpret_cast<const uint64 *>(p) + 1)),
        ToHost64(UNALIGNED_LOAD64(p)));
  }

  static void Store128(void *p, const uint128 v) {
    UNALIGNED_STORE64(p, FromHost64(Uint128Low64(v)));
    UNALIGNED_STORE64(reinterpret_cast<uint64 *>(p) + 1,
                      FromHost64(Uint128High64(v)));
  }

  // Build a uint128 from 1-16 bytes.
  // 8 * len least significant bits are loaded from the memory with
  // LittleEndian order. The 128 - 8 * len most significant bits are
  // set all to 0.
  static uint128 Load128VariableLength(const void *p, int len) {
    if (len <= 8) {
      return uint128(Load64VariableLength(p, len));
    } else {
      return uint128(
          Load64VariableLength(static_cast<const char *>(p) + 8, len - 8),
          Load64(p));
    }
  }

  // Load & Store in machine's word size.
  static uword_t LoadUnsignedWord(const void *p) {
    if (sizeof(uword_t) == 8)
      return Load64(p);
    else
      return Load32(p);
  }

  static void StoreUnsignedWord(void *p, uword_t v) {
    if (sizeof(v) == 8)
      Store64(p, v);
    else
      Store32(p, v);
  }
};

// Utilities to convert numbers between the current hosts's native byte
// order and big-endian byte order (same as network byte order)
//
// Load/Store methods are alignment safe
class BigEndian {
 public:
#ifdef IS_LITTLE_ENDIAN

  static uint16 FromHost16(uint16 x) { return bswap_16(x); }
  static uint16 ToHost16(uint16 x) { return bswap_16(x); }

  static uint32 FromHost32(uint32 x) { return bswap_32(x); }
  static uint32 ToHost32(uint32 x) { return bswap_32(x); }

  static uint64 FromHost64(uint64 x) { return gbswap_64(x); }
  static uint64 ToHost64(uint64 x) { return gbswap_64(x); }

  static bool IsLittleEndian() { return true; }

#elif defined IS_BIG_ENDIAN

  static uint16 FromHost16(uint16 x) { return x; }
  static uint16 ToHost16(uint16 x) { return x; }

  static uint32 FromHost32(uint32 x) { return x; }
  static uint32 ToHost32(uint32 x) { return x; }

  static uint64 FromHost64(uint64 x) { return x; }
  static uint64 ToHost64(uint64 x) { return x; }

  static bool IsLittleEndian() { return false; }

#endif /* ENDIAN */

  // Unified BigEndian::FromHost(ValueType v) API.
  template <class ValueType>
  static typename fromhost_value_type_traits<ValueType>::int_type
  FromHost(ValueType v) {
    return GeneralFormatConverter<BigEndian, ValueType>::FromHost(v);
  }

  // Functions to do unaligned loads and stores in big-endian order.
  static uint16 Load16(const void *p) {
    return ToHost16(UNALIGNED_LOAD16(p));
  }

  static void Store16(void *p, uint16 v) {
    UNALIGNED_STORE16(p, FromHost16(v));
  }

  static uint32 Load32(const void *p) {
    return ToHost32(UNALIGNED_LOAD32(p));
  }

  static void Store32(void *p, uint32 v) {
    UNALIGNED_STORE32(p, FromHost32(v));
  }

  static uint64 Load64(const void *p) {
    return ToHost64(UNALIGNED_LOAD64(p));
  }

  // Build a uint64 from 1-8 bytes.
  // 8 * len least significant bits are loaded from the memory with
  // BigEndian order. The 64 - 8 * len most significant bits are
  // set all to 0.
  // In latex-friendly words, this function returns:
  //     $\sum_{i=0}^{len-1} p[i] 256^{i}$, where p[i] is unsigned.
  //
  // This function is equivalent with:
  // uint64 val = 0;
  // memcpy(&val, p, len);
  // return ToHost64(val);
  // TODO(user): write a small benchmark and benchmark the speed
  // of a memcpy based approach.
  //
  // For speed reasons this function does not work for len == 0.
  // The caller needs to guarantee that 1 <= len <= 8.
  static uint64 Load64VariableLength(const void * const p, int len) {
    assert(len >= 1 && len <= 8);
    uint64 val = Load64(p);
    uint64 mask = 0;
    --len;
    do {
      mask = (mask << 8) | 0xff;
      // (--len >= 0) is about 10 % faster than (len--) in some benchmarks.
    } while (--len >= 0);
    return val & mask;
  }

  static void Store64(void *p, uint64 v) {
    UNALIGNED_STORE64(p, FromHost64(v));
  }

  static uint128 Load128(const void *p) {
    return uint128(
        ToHost64(UNALIGNED_LOAD64(p)),
        ToHost64(UNALIGNED_LOAD64(reinterpret_cast<const uint64 *>(p) + 1)));
  }

  static void Store128(void *p, const uint128 v) {
    UNALIGNED_STORE64(p, FromHost64(Uint128High64(v)));
    UNALIGNED_STORE64(reinterpret_cast<uint64 *>(p) + 1,
                      FromHost64(Uint128Low64(v)));
  }

  // Build a uint128 from 1-16 bytes.
  // 8 * len least significant bits are loaded from the memory with
  // BigEndian order. The 128 - 8 * len most significant bits are
  // set all to 0.
  static uint128 Load128VariableLength(const void *p, int len) {
    if (len <= 8) {
      return uint128(Load64VariableLength(static_cast<const char *>(p)+8,
                                          len));
    } else {
      return uint128(
          Load64VariableLength(p, len-8),
          Load64(static_cast<const char *>(p)+8));
    }
  }

  // Load & Store in machine's word size.
  static uword_t LoadUnsignedWord(const void *p) {
    if (sizeof(uword_t) == 8)
      return Load64(p);
    else
      return Load32(p);
  }

  static void StoreUnsignedWord(void *p, uword_t v) {
    if (sizeof(uword_t) == 8)
      Store64(p, v);
    else
      Store32(p, v);
  }
};  // BigEndian

// Network byte order is big-endian
typedef BigEndian NetworkByteOrder;

//////////////////////////////////////////////////////////////////////
// Implementation details: Clients can stop reading here.
//
// Define ValueType->IntType mapping for the unified
// "IntType FromHost(ValueType)" API. The mapping is implemented via
// fromhost_value_type_traits trait struct. Every legal ValueType has its own
// specialization. There is no default body for this trait struct, so that
// any type that is not supported by the unified FromHost API
// will trigger a compile time error.
#define FROMHOST_TYPE_MAP(ITYPE, VTYPE) \
template<> \
struct fromhost_value_type_traits<VTYPE> { \
  typedef VTYPE value_type; \
  typedef ITYPE int_type; \
};

FROMHOST_TYPE_MAP(uint8, uint8);
FROMHOST_TYPE_MAP(uint8, int8);
FROMHOST_TYPE_MAP(uint16, uint16);
FROMHOST_TYPE_MAP(uint16, int16);
FROMHOST_TYPE_MAP(uint32, uint32);
FROMHOST_TYPE_MAP(uint32, int32);
FROMHOST_TYPE_MAP(uint64, uint64);
FROMHOST_TYPE_MAP(uint64, int64);
FROMHOST_TYPE_MAP(uint32, float);
FROMHOST_TYPE_MAP(uint64, double);
FROMHOST_TYPE_MAP(uint8, bool);
#undef FROMHOST_TYPE_MAP

// Default implementation for the unified FromHost(ValueType) API, which
// handles all integral types (ValueType is one of uint8, int8, uint16, int16,
// uint32, int32, uint64, int64). Compiler will remove the switch case
// branches and unnecessary static_cast, when the template is expanded.
template <class EndianClass, typename ValueType>
typename fromhost_value_type_traits<ValueType>::int_type
GeneralFormatConverter<EndianClass, ValueType>::FromHost(ValueType v) {
  switch (sizeof(ValueType)) {
    case 1:
      return static_cast<uint8>(v);
      break;
    case 2:
      return EndianClass::FromHost16(static_cast<uint16>(v));
      break;
    case 4:
      return EndianClass::FromHost32(static_cast<uint32>(v));
      break;
    case 8:
      return EndianClass::FromHost64(static_cast<uint64>(v));
      break;
    default:
      LOG(FATAL) << "Unexpected value size: " << sizeof(ValueType);
  }
}

// Specialization of the unified FromHost(ValueType) API, which handles
// float types (ValueType is float).
template<class EndianClass>
class GeneralFormatConverter<EndianClass, double> {
 public:
  static typename fromhost_value_type_traits<double>::int_type
  FromHost(double v) {
    return EndianClass::FromHost64(bit_cast<uint64>(v));
  }
};

// Specialization of the unified FromHost(ValueType) API, which handles
// double types (ValueType is double).
template<class EndianClass>
class GeneralFormatConverter<EndianClass, float> {
 public:
  static typename fromhost_value_type_traits<float>::int_type
  FromHost(float v) {
    return EndianClass::FromHost32(bit_cast<uint32>(v));
  }
};

#endif  // UTIL_ENDIAN_ENDIAN_H_
