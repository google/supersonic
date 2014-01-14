// Copyright 2004 Google Inc.
// All Rights Reserved.
//
//

#include <iostream>
#include "supersonic/utils/int128.h"
#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"

const uint128_pod kuint128max = {
    static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF)),
    static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF))
};

// Long division/modulo for uint128. Simple, but probably slow.
//
// See e.g. "Integer division (unsigned) with remainder" here:
//     http://en.wikipedia.org/wiki/Division_algorithm
//
// Builds the quotient bitwise from MSB to LSB, accumulating the remainder along
// the way.
void div_mod_impl(const uint128& dividend, const uint128& divisor,
                  uint128* quotient_ret, uint128* remainder_ret) {
  uint128 quotient = 0;
  uint128 remainder = 0;

  if (divisor == 0) {
    LOG(FATAL) << "Division or mod by zero: dividend.hi=" << dividend.hi_
               << ", lo=" << dividend.lo_;
  } else {
    // Build remainder MSB-to-LSB from the dividend's bits.
    // Splitting into two loops has a roughly 40% improvement on the benchmark.
    if (dividend.hi_ != 0) {
      // Mask the dividend with 'cur_bit'.
      for (uint64 cur_bit = static_cast<uint64>(1) << 63;
           cur_bit != 0; cur_bit >>= 1) {
        remainder.lo_ <<= 1;
        if ((dividend.hi_ & cur_bit) != 0) {
          ++remainder.lo_;
        }
        if (remainder >= divisor) {
          remainder.lo_ -= divisor.lo_;
          quotient.hi_ |= cur_bit;
        }
        // Loop invariant:
        //   pos := cur_bit << 64
        //   dividend == quotient * divisor + remainder * pos + dividend % pos
      }
    }
    for (uint64 cur_bit = static_cast<uint64>(1) << 63;
         cur_bit != 0; cur_bit >>= 1) {
      remainder <<= 1;
      if ((dividend.lo_ & cur_bit) != 0) {
        ++remainder.lo_;
      }
      if (remainder >= divisor) {
        remainder -= divisor;
        quotient.lo_ |= cur_bit;
      }
      // Loop invariant:
      //   pos := cur_bit
      //   dividend == quotient * divisor + remainder * pos + dividend % pos
    }
  }
  *quotient_ret = quotient;
  *remainder_ret = remainder;
}

uint128& uint128::operator/=(const uint128& divisor) {
  uint128 quotient = 0;
  uint128 remainder = 0;
  div_mod_impl(*this, divisor, &quotient, &remainder);
  *this = quotient;
  return *this;
}
uint128& uint128::operator%=(const uint128& divisor) {
  uint128 quotient = 0;
  uint128 remainder = 0;
  div_mod_impl(*this, divisor, &quotient, &remainder);
  *this = remainder;
  return *this;
}

std::ostream& operator<<(std::ostream& o, const uint128& b) {
  return (o << b.hi_ << "::" << b.lo_);
}
