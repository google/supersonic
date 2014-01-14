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
// Overrides of C++ operators, so that they have saner semantics (e.g. signed
// vs unsigned comparison), and are usable as parameter templates.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }
#include <type_traits>

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/endian.h"
#include "supersonic/utils/hash/hash.h"

namespace supersonic {
namespace operators {

// Identical to Cast, but used in a context when the copy is made without
// changing the type.
struct Copy {
  template<typename T>
  T operator()(const T& arg) const { return arg; }
};

// Cast is a by-product of assigning the result to a variable of a different
// type.
struct Cast : public Copy {
};

// Used for a Date to Datetime cast.
struct DateToDatetime {
  int64 operator()(const int32 arg) const { return arg * 24LL * 3600000000LL; }
};

struct Negate {
  int32 operator()(const int32 arg) const { return -arg; }
  int64 operator()(const int64 arg) const { return -arg; }
  float operator()(const float arg) const { return -arg; }
  double operator()(const double arg) const { return -arg; }

  int64 operator()(const uint32 arg) const { return -static_cast<int64>(arg); }
  int64 operator()(const uint64 arg) const { return -static_cast<int64>(arg); }
};

struct Plus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a + b; }
};

struct Minus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a - b; }
};

struct Multiply {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a * b; }
};

struct Divide {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a / b; }
};

struct Modulus {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a % b; }

  // Specializations for DOUBLE and FLOAT.
  int64 operator()(const double& a, const double& b) const {
    return static_cast<int64>(a) % static_cast<int64>(b);
  }
  int64 operator()(const float& a, const float& b) const {
    return static_cast<int64>(a) % static_cast<int64>(b);
  }
};

struct IsOdd {
  template<typename T>
  bool operator()(const T& arg) const { return arg % 2; }

  // Specializations for DOUBLE and FLOAT.
  bool operator()(const double& arg) const {
    return static_cast<int64>(arg) % 2;
  }
  bool operator()(const float& arg) const {
    return static_cast<int64>(arg) % 2;
  }
};

struct IsEven {
  template<typename T>
  bool operator()(const T& arg) const {
    IsOdd is_odd;
    return !is_odd(arg);
  }
};

struct And {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return a && b; }
};

struct Xor {
  bool operator()(bool a, bool b) const {
    // The reason we do it this way is we want to be safe in the case
    // there are values different than 0 and 1 in the input.
    return !a != !b;
  }
};

struct AndNot {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return (!a) && b; }
};

struct Or {
  template<typename T>
  bool operator()(const T& a, const T& b) const { return a || b; }
};

struct Not {
  template<typename T>
  bool operator()(const T& a) const { return !a; }
};

struct BitwiseAnd {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a & b; }
};

struct BitwiseOr {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a | b; }
};

struct BitwiseXor {
  template<typename T>
  T operator()(const T& a, const T& b) const { return a ^ b; }
};

struct BitwiseAndNot {
  template<typename T>
  T operator()(const T& a, const T& b) const { return (~a) & b; }
};

struct BitwiseNot {
  template<typename T>
  T operator()(const T& a) const { return ~a; }
};

struct LeftShift {
  template<typename T1, typename T2>
  T1 operator()(const T1& a, const T2& b) { return a << b; }
};

struct RightShift {
  template<typename T1, typename T2>
  T1 operator()(const T1& a, const T2& b) { return a >> b; }
};

struct Equal {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const { return a == b; }

  // Specializations to resolve signed-vs-unsigned.

  bool operator()(const int32& a, const uint32&b) const {
    return a >= 0 && static_cast<uint32>(a) == b;
  }
  bool operator()(const int32& a, const uint64&b) const {
    return a >= 0 && static_cast<uint32>(a) == b;
  }
  bool operator()(const int64& a, const uint32&b) const {
    return a >= 0 && static_cast<uint64>(a) == b;
  }
  bool operator()(const int64& a, const uint64&b) const {
    return a >= 0 && static_cast<uint64>(a) == b;
  }
  bool operator()(const uint32& a, const int32&b) const {
    return b >= 0 && a == static_cast<uint32>(b);
  }
  bool operator()(const uint32& a, const int64&b) const {
    return b >= 0 && a == static_cast<uint64>(b);
  }
  bool operator()(const uint64& a, const int32&b) const {
    return b >= 0 && a == static_cast<uint32>(b);
  }
  bool operator()(const uint64& a, const int64&b) const {
    return b >= 0 && a == static_cast<uint64>(b);
  }
};

struct NotEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Equal equal;
    return !equal(a, b);
  }
};

struct Less {
  // We want to order DataType values based on their textual representation.
  // However, this would create an inconsistency (loss of transitivity) if we
  // could also compare (for ordering) DataType values with values of other
  // types, so we forbid those mixed type comparisons.
  bool operator()(const DataType& a, const DataType& b) const {
    return DataType_Name(a) < DataType_Name(b);
  }

  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    // Disabling mixed-type DataType vs other-type comparisons. See the comment
    // above.
    static_assert(!std::is_same<T1, DataType>::value &&
                  !std::is_same<T2, DataType>::value,
                  "Less for DataType and other type is disabled");
    return a < b;
  }

  // Specializations to resolve signed-vs-unsigned.

  bool operator()(const int32& a, const uint32&b) const {
    return a < 0 || static_cast<uint32>(a) < b;
  }
  bool operator()(const int32& a, const uint64&b) const {
    return a < 0 || static_cast<uint32>(a) < b;
  }
  bool operator()(const int64& a, const uint32&b) const {
    return a < 0 || static_cast<uint64>(a) < b;
  }
  bool operator()(const int64& a, const uint64&b) const {
    return a < 0 || static_cast<uint64>(a) < b;
  }
  bool operator()(const uint32& a, const int32&b) const {
    return b >= 0 && a < static_cast<uint32>(b);
  }
  bool operator()(const uint32& a, const int64&b) const {
    return b >= 0 && a < static_cast<uint32>(b);
  }
  bool operator()(const uint64& a, const int32&b) const {
    return b >= 0 && a < static_cast<uint32>(b);
  }
  bool operator()(const uint64& a, const int64&b) const {
    return b >= 0 && a < static_cast<uint64>(b);
  }
};

struct Greater {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return less(b, a);
  }
};

struct LessOrEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return !less(b, a);
  }
};

struct GreaterOrEqual {
  template<typename T1, typename T2>
  bool operator()(const T1& a, const T2& b) const {
    Less less;
    return !less(a, b);
  }
};

// The following functions are copied from util/hash/murmur.cc so that we could
// use them inlined.
static uint64 ShiftMix(uint64 val) {
  return val ^ (val >> 47);
}

static inline uint64 LoadBytes(const char * const buf, int len) {
  DCHECK_LT(len, 9);
  uint64 val = 0;
  --len;
  do {
    val = (val << 8) | buf[len];
  } while (--len >= 0);
  // (--len >= 0) is about 10 % faster in the small string ubenchmarks
  // than (len--).
  return val;
}

inline uint64 MurmurHash64(const char *buf, const size_t len) {
  static const uint64 mul = 0xc6a4a7935bd1e995ULL;
  // Let's remove the bytes not divisible by the sizeof(uint64).
  // This allows the inner loop to process the data as 64 bit integers.
  const int len_aligned = len & ~0x7;
  const char * const end = buf + len_aligned;
  uint64 hash = len * mul;
  for (const char *p = buf; p != end; p += 8) {
    const uint64 data = ShiftMix(LittleEndian::Load64(p) * mul) * mul;
    hash ^= data;
    hash *= mul;
  }
  if ((len & 0x7) != 0) {
    const uint64 data = LoadBytes(end, len & 0x7);
    hash ^= data;
    hash *= mul;
  }
  hash = ShiftMix(hash) * mul;
  hash = ShiftMix(hash);
  return hash;
}

struct Hash {
  template<typename T>
  size_t operator()(const T& v) const {
    // Handle enums by conversion to the appropriate integral type.
    std::hash<typename std::conditional<
           std::is_enum<T>::value,
           typename std::conditional<
               sizeof(T) <= sizeof(int32),
               int32,
               int64>::type,
           T>::type> hasher;
    return hasher(v);
  }
  size_t operator()(const float& v) const {
    std::hash<int32> hasher;
    return hasher(*reinterpret_cast<const int32*>(&v));
  }
  size_t operator()(const double& v) const {
    std::hash<int64> hasher;
    return hasher(*reinterpret_cast<const int64*>(&v));
  }
  size_t operator()(const bool& v) const {
    // Arbitrary relative primes.
    return v ? 23 : 34;
  }
  size_t operator()(const StringPiece& v) const {
    return MurmurHash64(v.data(), v.size());
  }
};

}  // namespace operators

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_OPERATORS_H_
