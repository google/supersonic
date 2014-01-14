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

// Provides expression primitives that works on vectors of values,
// supports indirection vectors and are compatible for SIMD processing.

#ifndef SUPERSONIC_EXPRESSION_VECTOR_VECTOR_PRIMITIVES_H_
#define SUPERSONIC_EXPRESSION_VECTOR_VECTOR_PRIMITIVES_H_

#include <string.h>
#ifdef  __SSE2__
#include <emmintrin.h>
#include <xmmintrin.h>
#endif
#include <algorithm>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Type used to describe indexes used by indirection vectors.
class Arena;

typedef int64 index_t;

// ===================== IndexResolvers ======================================
// IndexResolver is a policy to translate position using indirection vector
// or use it directly.

// Doesn't use indirectionVector.
struct DirectIndexResolver {
  index_t operator()(const index_t* const indirection,
                     const index_t index) const {
    return index;
  }
};

// Uses indirectionVector.
struct IndirectIndexResolver {
  index_t operator()(const index_t* const indirection,
                     const index_t index) const {
    return indirection[index];
  }
};

// ================ Vectorized wrappers for the operations =====================

// ---------------------- VectorBinaryOperation --------------------------------
template <OperatorId operation_type,
          typename IndexResolverLeft, typename IndexResolverRight,
          DataType result_type, DataType left_type, DataType right_type,
          bool needs_allocator>
struct VectorBinaryPrimitive {};

// generic version
template <OperatorId operation_type,
          typename IndexResolverLeft, typename IndexResolverRight,
          DataType result_type, DataType left_type, DataType right_type>
struct VectorBinaryPrimitive<operation_type,
                             IndexResolverLeft, IndexResolverRight,
                             result_type, left_type, right_type, false> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;

  typedef typename BinaryExpressionTraits<operation_type>::basic_operator
      Operator;

  // Returns true if operation has been successful.
  bool operator()(const LeftCppType* left, const RightCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  ResultCppType* result,
                  Arena* arena) const {
    IndexResolverLeft  index_resolver_left;
    IndexResolverRight index_resolver_right;
    Operator operation;
    for (index_t i = 0; i < size; ++i) {
      result[i] = operation(
        left[index_resolver_left(indirection_left, i)],
        right[index_resolver_right(indirection_right, i)]);
    }
    return true;
  }

  // Returns true if operation has been successful.
  bool operator()(const LeftCppType* left, const RightCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  bool_const_ptr skip_list,
                  ResultCppType* result,
                  Arena* arena) const {
    IndexResolverLeft  index_resolver_left;
    IndexResolverRight index_resolver_right;
    Operator operation;
    for (index_t i = 0; i < size; ++i) {
      if (!*skip_list) {
        result[i] = operation(
          left[index_resolver_left(indirection_left, i)],
          right[index_resolver_right(indirection_right, i)]);
      }
      ++skip_list;
    }
    return true;
  }
};

// generic version with allocation
template <OperatorId operation_type,
          typename IndexResolverLeft, typename IndexResolverRight,
          DataType result_type, DataType left_type, DataType right_type>
struct VectorBinaryPrimitive <operation_type,
                              IndexResolverLeft, IndexResolverRight,
                              result_type, left_type, right_type, true> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;

  typedef typename BinaryExpressionTraits<operation_type>::basic_operator
      Operator;

  // Returns true if operation has been successful.
  bool operator()(const LeftCppType* left, const RightCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  ResultCppType* result,
                  Arena* arena) const {
    IndexResolverLeft  index_resolver_left;
    IndexResolverRight index_resolver_right;
    Operator operation;
    for (index_t i = 0; i < size; ++i) {
      result[i] = operation(
        left[index_resolver_left(indirection_left, i)],
        right[index_resolver_right(indirection_right, i)],
        arena);
    }
    return true;
  }

  // Returns true if operation has been successful.
  bool operator()(const LeftCppType* left, const RightCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  bool_const_ptr skip_list,
                  ResultCppType* result,
                  Arena* arena) const {
    IndexResolverLeft  index_resolver_left;
    IndexResolverRight index_resolver_right;
    Operator operation;
    for (index_t i = 0; i < size; ++i) {
      if (!*skip_list) {
        result[i] = operation(
          left[index_resolver_left(indirection_left, i)],
          right[index_resolver_right(indirection_right, i)],
          arena);
      }
      ++skip_list;
    }
    return true;
  }
};

#ifdef __SSE2__

// Different simd operations (single precision, double precision, integer)
// operate on different registers. SimdLoader is unified interface to
// load/store data in right CPU register.
template <typename DataType> struct SimdLoader;

template <> struct SimdLoader<__m128i> {
  __m128i LoadAligned(const void* arg) const {
    return _mm_load_si128(static_cast<const __m128i*>(arg));
  };
  void StoreAligned(const __m128i src, void* dst) const {
    _mm_store_si128(reinterpret_cast<__m128i*>(dst), src);
  };
};

template <> struct SimdLoader<__m128> {
  __m128 LoadAligned(const void* const arg) const {
    return _mm_load_ps(reinterpret_cast<const float*>(arg));
  };
  void StoreAligned(const __m128 src, void* dst) const {
    _mm_store_ps(reinterpret_cast<float*>(dst), src);
  };
};

template <> struct SimdLoader<__m128d> {
  __m128d LoadAligned(const void* const arg) const {
    return _mm_load_pd(reinterpret_cast<const double*>(arg));
  };
  void StoreAligned(const __m128d src, void* dst) const {
    _mm_store_pd(reinterpret_cast<double*>(dst), src);
  };
};

template<OperatorId operation_type, typename DataCppType>
void EvaluateSimd(const DataCppType* left, const DataCppType* right,
                  const index_t size, DataCppType* result) {
  typedef typename SimdTraits<operation_type, DataCppType>::simd_operator
      SimdOperator;
  typedef SimdLoader<typename SimdOperator::simd_arg> SimdLoaderType;
  SimdOperator operation_simd;
  SimdLoaderType simd_loader;

  // SIMD works if the vectors are aligned.
  DCHECK_EQ(0, reinterpret_cast<uint64>(left) % 16);
  DCHECK_EQ(0, reinterpret_cast<uint64>(right) % 16);
  DCHECK_EQ(0, reinterpret_cast<uint64>(result) % 16);

  const int step = 16 / sizeof(DataCppType);
  DCHECK_EQ(0, size % step);

  typename SimdOperator::simd_arg xmm1, xmm2, xmm3;
  for (index_t i = 0; i < size; i += step) {
    xmm1 = simd_loader.LoadAligned(left + i);
    xmm2 = simd_loader.LoadAligned(right + i);
    xmm3 = operation_simd(xmm1, xmm2);
    simd_loader.StoreAligned(xmm3, result + i);
  }
}

#else   // __SSE2__

template<OperatorId operation_type, typename DataCppType>
void EvaluateNoSimd(const DataCppType* left, const DataCppType* right,
                    const index_t size, DataCppType* result);

template<OperatorId operation_type, typename DataCppType>
void EvaluateSimd(const DataCppType* left, const DataCppType* right,
                  const index_t size, DataCppType* result) {
  EvaluateNoSimd(left, right, size, result);
}

#endif  // __SSE2__

template<OperatorId operation_type, typename DataCppType>
void EvaluateNoSimd(const DataCppType* left, const DataCppType* right,
                    const index_t size, DataCppType* result) {
  typename BinaryExpressionTraits<operation_type>::basic_operator op;
  for (index_t i = 0; i < size; ++i) {
    result[i] = op(left[i], right[i]);
  }
}

template<OperatorId operation_type, typename DataCppType>
void EvaluateNoSimd(const DataCppType* left, const DataCppType* right,
                    const index_t size, bool_const_ptr skip_list,
                    DataCppType* result) {
  typename BinaryExpressionTraits<operation_type>::basic_operator op;
  for (index_t i = 0; i < size; ++i) {
    if (!*skip_list) result[i] = op(left[i], right[i]);
    ++skip_list;
  }
}

template<OperatorId operation_type, typename DataCppType,
         bool simd_supported> struct DirectEvaluator;

template<OperatorId operation_type, typename DataCppType>
struct DirectEvaluator<operation_type, DataCppType, false> {
  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t size, DataCppType* result) {
    EvaluateNoSimd<operation_type, DataCppType>(left, right, size, result);
    return true;
  }
  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t size, bool_const_ptr skip_list,
                  DataCppType* result) {
      EvaluateNoSimd<operation_type, DataCppType>(left, right, size, skip_list,
                                                  result);
      return true;
  }
};

template<OperatorId operation_type, typename DataCppType>
struct DirectEvaluator<operation_type, DataCppType, true> {
  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t size, DataCppType* result) const {
    const uint64 left_offset = reinterpret_cast<uint64>(left) % 16;
    if ((left_offset == reinterpret_cast<uint64>(right) % 16)
     && (left_offset == reinterpret_cast<uint64>(result) % 16)) {
      const int16 step = 16 / sizeof(DataCppType);
      if (left_offset == 0) {
        const index_t nsize = (size / step) * step;
        // Ideal case - the vectors are already 16-aligned
        EvaluateSimd<operation_type, DataCppType>(left, right, nsize, result);
        // Tail.
        EvaluateNoSimd<operation_type, DataCppType>(
            left + nsize, right + nsize, size - nsize, result + nsize);
      } else {
        int offset = 16 - left_offset;
        if (offset % sizeof(DataCppType) == 0) {
          // We are able to skip heads of the vectors to align them.
          // Prefix -> how many items to skip.
          const int prefix = offset / sizeof(DataCppType);
          const int nsize = max(0LL, ((size - prefix) / step) * step);
          EvaluateNoSimd<operation_type, DataCppType>(
              left, right, prefix < size ? prefix : size, result);
          EvaluateSimd<operation_type, DataCppType>(
              left + prefix, right + prefix, nsize, result + prefix);
          // Tail.
          EvaluateNoSimd<operation_type, DataCppType>(
              left  + prefix + nsize, right + prefix + nsize,
              max(size - nsize - prefix, 0LL), result + prefix + nsize);
        } else {
          // Cannot use SIMD, because result, right and left inputs have
          // alignment that cannot be corrected by by iteration over the prefix
          EvaluateNoSimd<operation_type, DataCppType>(
              left, right, size, result);
        }
      }
    } else {
      // Cannot use SIMD, because result, right and left inputs are differently
      // aligned
      EvaluateNoSimd<operation_type, DataCppType>(left, right, size, result);
    }
    return true;
  }

  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t size, bool_const_ptr skip_list,
                  DataCppType* result) const {
    EvaluateNoSimd<operation_type, DataCppType>(left, right, size, skip_list,
                                                result);
    return true;
  }
};

// Specialization for direct-indexing and homogeneous types.
// Uses fast SIMD instructions for supported operators.
// TODO(ptab): Could be extended to support not homogeneous types
template<OperatorId operation_type, DataType data_type>
struct VectorBinaryPrimitive<operation_type,
                             DirectIndexResolver, DirectIndexResolver,
                             data_type, data_type, data_type, false> {
  typedef typename TypeTraits<data_type>::cpp_type DataCppType;

  // Returns true if operation has been successful.
  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  DataCppType* result,
                  Arena* arena) const {
    typedef SimdTraits<operation_type, DataCppType> SimdTraits;
    DirectEvaluator<operation_type, DataCppType,
                    SimdTraits::simd_supported> evaluator;
    return evaluator(left, right, size, result);
  }

  // Returns true if operation has been successful.
  bool operator()(const DataCppType* left, const DataCppType* right,
                  const index_t* indirection_left,
                  const index_t* indirection_right,
                  const index_t size,
                  bool_const_ptr skip_list,
                  DataCppType* result,
                  Arena* arena) const {
    typedef SimdTraits<operation_type, DataCppType> SimdTraits;
    DirectEvaluator<operation_type, DataCppType,
                    SimdTraits::simd_supported> evaluator;
    return evaluator(left, right, size, skip_list, result);
  }
};

// --------------------- VectorUnaryPrimitive ---------------------------------
template <OperatorId operation_type, typename IndexResolver,
          DataType result_type, DataType input_type>
struct VectorUnaryPrimitive {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;
  typedef typename UnaryExpressionTraits<operation_type>::basic_operator
      Operator;

  // Returns true if operation has been successful.
  bool operator()(const InputCppType* input,
                  const index_t* indirection,
                  const index_t size,
                  ResultCppType* result,
                  Arena* arena) const {
    IndexResolver  index_resolver;
    Operator operation;
    for (index_t i = 0; i < size; i++) {
      result[i] = operation(input[index_resolver(indirection, i)]);
    }
    return true;
  }
};

// Fast direct COPY implementation.
template <DataType result_type, DataType input_type>
struct VectorUnaryPrimitive<OPERATOR_COPY, DirectIndexResolver,
                            result_type, input_type> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;

  // Returns true if operation has been successful.
  bool operator()(const InputCppType* input,
                  const index_t* indirection,
                  const index_t size,
                  ResultCppType* result) const {
    memcpy(result, input, size * sizeof(InputCppType));
    return true;
  }
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_VECTOR_PRIMITIVES_H_
