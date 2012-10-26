// Copyright 2009 Google Inc. All Rights Reserved.
//         jyrki@google.com (Jyrki Alakuijala)

#include "supersonic/utils/hash/murmur.h"

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/endian.h"

namespace util_hash {

namespace {

// Load the last remaining bytes in a little-endian manner (the byte that
// with the highest index ends up in the highest bits in the return value)
// into a uint64 to be used for checksumming those bytes that could not be
// directly loaded as a uint64.
inline uint64 LoadBytes(const char * const buf, int len) {
  DCHECK_LT(len, 9);
  uint64 val = 0;
  --len;
  do {
    val = (val << 8) | reinterpret_cast<const uint8 *>(buf)[len];
  } while (--len >= 0);
  // (--len >= 0) is about 10 % faster in the small string ubenchmarks
  // than (len--).
  return val;
}

// We need to mix some of the bits that get propagated and mixed into the
// high bits by multiplication back into the low bits. 17 last bits get
// a more efficiently mixed with this.
inline uint64 ShiftMix(uint64 val) {
  return val ^ (val >> 47);
}

}  // namespace


uint64 MurmurHash64WithSeed(const char *buf,
                            const size_t len,
                            const uint64 seed) {
  static const uint64 mul = 0xc6a4a7935bd1e995ULL;
  // Let's remove the bytes not divisible by the sizeof(uint64).
  // This allows the inner loop to process the data as 64 bit integers.
  const int len_aligned = len & ~0x7;
  const char * const end = buf + len_aligned;
  uint64 hash = seed ^ (len * mul);
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

// MurmurHash128 is a 128 bit hashing variant of the Murmur 2.0 algorithm.
// The low 64 bits are computed exactly the same way as in MurmurHash64.
// The higher 64 bits are computed parasitically as a sequence of xors
// through the intermediate hash values. The correlaction between the
// high and low bits with the last data values is reduced by using a separate
// mixture multiplier for the high bits.
uint128 MurmurHash128WithSeed(const char *buf,
                              const size_t len,
                              uint128 seed) {
  // Murmur 2.0 multiplication constant.
  static const uint64 mul = 0xc6a4a7935bd1e995ULL;

  // Initialize the hashing value.
  uint64 hash = Uint128High64(seed) ^ (len * mul);

  // hash2 will be xored by hash during the the hash computation iterations.
  // In the end we use an alternative mixture multiplier for mixing
  // the bits in hash2.
  uint64 hash2 = Uint128Low64(seed);

  // Let's remove the bytes not divisible by the sizeof(uint64).
  // This allows the inner loop to process the data as 64 bit integers.
  const int len_aligned = len & ~0x7;
  const char * const end = buf + len_aligned;

  for (const char *p = buf; p != end; p += 8) {
    // Manually unrolling this loop 2x did not help on Intel Core 2.
    hash ^= ShiftMix(LittleEndian::Load64(p) * mul) * mul;
    hash *= mul;
    hash2 ^= hash;
  }
  if ((len & 0x7) != 0) {
    const uint64 data = LoadBytes(end, len & 0x7);
    hash ^= data;
    hash *= mul;
    hash2 ^= hash;
  }
  hash = ShiftMix(hash) * mul;
  hash2 ^= hash;
  hash = ShiftMix(hash);

  // mul2 is a prime just above golden ratio. mul2 is used to ensure that the
  // impact of the last few bytes is different to the upper and lower 64 bits.
  static const uint64 mul2 = 0x9e3779b97f4a7835ULL;
  hash2 = ShiftMix(hash2 * mul2) * mul2;

  return uint128(hash2, hash);
}

}  // namespace util_hash
