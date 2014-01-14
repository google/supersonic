// Copyright 2010 Google Inc. All Rights Reserved.
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
// Author: ptab@google.com (Piotr Tabor)

#ifndef SUPERSONIC_EXPRESSION_VECTOR_SIMD_OPERATORS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_SIMD_OPERATORS_H_

#ifdef __SSE2__
#include <mmintrin.h>   // intrinsics for MMX instructions
#include <xmmintrin.h>  // intrinsics for SSE instructions
#include <emmintrin.h>  // intrinsics for SSE2 instructions

namespace supersonic {
namespace simd_operators {

// Declares functor named like 'OperationClassName',  specialized to work with
// given 'type', that is using given intrinsic 'simd_function' (integers).
#define SIMD_OPERATION(OperationClassName, type, SimdType, simd_function)      \
  template <> struct OperationClassName<type, type> {                          \
    typedef SimdType simd_arg;                                                 \
    SimdType operator()(const SimdType left, const SimdType right) const {     \
      return simd_function(left, right);                                       \
    }                                                                          \
  };

// --------------------- Arithmetic --------------------------------------------

template <typename LeftType, typename RightType> struct SimdPlus;
SIMD_OPERATION(SimdPlus, int32,  __m128i, _mm_add_epi32);  // ADD 4x32bit s/u
SIMD_OPERATION(SimdPlus, uint32, __m128i, _mm_add_epi32);  // ADD 4x32bit s/u
SIMD_OPERATION(SimdPlus, int64,  __m128i, _mm_add_epi64);  // ADD 2x64bit s/u
SIMD_OPERATION(SimdPlus, uint64, __m128i, _mm_add_epi64);  // ADD 2x64bit s/u
SIMD_OPERATION(SimdPlus, float,  __m128 , _mm_add_ps);     // ADD 4xfloats
SIMD_OPERATION(SimdPlus, double, __m128d, _mm_add_pd);     // ADD 2xdoubles

template <typename LeftType, typename RightType> struct SimdSubtract;
SIMD_OPERATION(SimdSubtract, int32,  __m128i, _mm_sub_epi32);
SIMD_OPERATION(SimdSubtract, uint32, __m128i, _mm_sub_epi32);
SIMD_OPERATION(SimdSubtract, int64,  __m128i, _mm_sub_epi64);
SIMD_OPERATION(SimdSubtract, uint64, __m128i, _mm_sub_epi64);
SIMD_OPERATION(SimdSubtract, float,  __m128 , _mm_sub_ps);
SIMD_OPERATION(SimdSubtract, double, __m128d, _mm_sub_pd);

template <typename LeftType, typename RightType> struct SimdMultiply;
SIMD_OPERATION(SimdMultiply, float,  __m128 , _mm_mul_ps);
SIMD_OPERATION(SimdMultiply, double, __m128d, _mm_mul_pd);
// TODO(ptab): Use mullo to multiply 32bit * 32bit = 32 bit (SSE4)
// TODO(ptab): Use _mm_mul_epi32, _mm_unpacklo_epi32 to get 32bit*32bit=64 bit

template <typename LeftType, typename RightType> struct SimdDivide;
SIMD_OPERATION(SimdDivide, float,  __m128 , _mm_div_ps);
SIMD_OPERATION(SimdDivide, double, __m128d, _mm_div_pd);

// ------------------------ Logical --------------------------------------------

template <typename LeftType, typename RightType> struct SimdOr;
SIMD_OPERATION(SimdOr, bool,   __m128i, _mm_or_si128);  // Bitwise OR
SIMD_OPERATION(SimdOr, int32,  __m128i, _mm_or_si128);  // Bitwise OR
SIMD_OPERATION(SimdOr, uint32, __m128i, _mm_or_si128);  // Bitwise OR
SIMD_OPERATION(SimdOr, int64,  __m128i, _mm_or_si128);  // Bitwise OR
SIMD_OPERATION(SimdOr, uint64, __m128i, _mm_or_si128);  // Bitwise OR

template <typename LeftType, typename RightType> struct SimdAnd;
SIMD_OPERATION(SimdAnd, bool,   __m128i, _mm_and_si128);  // Bitwise AND
SIMD_OPERATION(SimdAnd, int32,  __m128i, _mm_and_si128);  // Bitwise AND
SIMD_OPERATION(SimdAnd, uint32, __m128i, _mm_and_si128);  // Bitwise AND
SIMD_OPERATION(SimdAnd, int64,  __m128i, _mm_and_si128);  // Bitwise AND
SIMD_OPERATION(SimdAnd, uint64, __m128i, _mm_and_si128);  // Bitwise AND

template <typename LeftType, typename RightType> struct SimdAndNot;
SIMD_OPERATION(SimdAndNot, bool,   __m128i, _mm_andnot_si128);
SIMD_OPERATION(SimdAndNot, int32,  __m128i, _mm_andnot_si128);
SIMD_OPERATION(SimdAndNot, uint32, __m128i, _mm_andnot_si128);
SIMD_OPERATION(SimdAndNot, int64,  __m128i, _mm_andnot_si128);
SIMD_OPERATION(SimdAndNot, uint64, __m128i, _mm_andnot_si128);

template <typename LeftType, typename RightType> struct SimdXor;
SIMD_OPERATION(SimdXor, bool, __m128i, _mm_xor_si128);

// ----------------------- Comparisons -----------------------------------------

// TODO(ptab): Comparisons are not homogeneous. Add support for not homogeneous
// template <typename LeftType, typename RightType> struct SimdEqual;
// SIMD_OPERATION(SimdEqual, bool,   __m128i, _mm_cmpeq_epi8);
// SIMD_OPERATION(SimdEqual, int32,  __m128i, _mm_cmpeq_epi32);
// SIMD_OPERATION(SimdEqual, uint32, __m128i, _mm_cmpeq_epi32);

// template <typename LeftType, typename RightType> struct SimdGreater;
// SIMD_OPERATION(SimdGreater, bool,   __m128i, _mm_cmpgt_epi8);
// SIMD_OPERATION(SimdGreater, int32,  __m128i, _mm_cmpgt_epi32);

// template <typename LeftType, typename RightType> struct SimdLess;
// SIMD_OPERATION(SimdLess, bool,   __m128i, _mm_cmplt_epi8);
// SIMD_OPERATION(SimdLess, int32,  __m128i, _mm_cmplt_epi32);
// TODO(ptab): Use _mm_cmpeq_ps, _mm_cmpnlt_ps.
// TODO(ptab): Add support for integral unsigned types.
// TODO(ptab): Integral LessOrEqual and GreaterOrEqual can be composed.

// --------------------------- Bitwise -----------------------------------------
// TODO(ptab): Implement bitwise (and, or, xor, shifts) using SIMD

// -------------------------- Unary --------------------------------------------
// TODO(ptab): Implement unary operations:
//     Casts: _mm_castpd_ps, _mm_castpd_si128, _mm_castps_pd, _mm_castps_si128
//            _mm_castsi128_ps, _mm_castsi128_pd, _mm_andnot_si128,
//     Arithmetic: _mm_sqrt_ss, _mm_sqrt_ps, negative
//     Logical: _mm_not_si128

}  // namespace operators
}  // namespace supersonic

#endif  // __SSE2__

#endif  // SUPERSONIC_EXPRESSION_VECTOR_SIMD_OPERATORS_H_
