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

#include "supersonic/expression/vector/vector_logic.h"

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/expression/vector/vector_primitives.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {
namespace vector_logic {

template<OperatorId op>
inline void BoolLogic(const bool* left,
                      const bool* right,
                      size_t row_count,
                      bool* result) {
  // We delegate the calculation to a vector binary primitive, which
  // encapsulates SIMD.
  VectorBinaryPrimitive<op, DirectIndexResolver, DirectIndexResolver,
      BOOL, BOOL, BOOL, false> bool_operator;
  bool res = bool_operator(left, right, NULL, NULL, row_count, result, NULL);
  DCHECK(res) << "Error in " << BinaryExpressionTraits<op>::name()
              << " boolean vector primitive calculation.";
}

void Or(const bool* left,
        const bool* right,
        size_t row_count,
        bool* result) {
  BoolLogic<OPERATOR_OR>(left, right, row_count, result);
}

void And(const bool* left,
         const bool* right,
         size_t row_count,
         bool* result) {
  BoolLogic<OPERATOR_AND>(left, right, row_count, result);
}

void AndNot(const bool* left,
            const bool* right,
            size_t row_count,
            bool* result) {
  BoolLogic<OPERATOR_AND_NOT>(left, right, row_count, result);
}

void Not(const bool* input,
         size_t row_count,
         bool* result) {
  // This is a rather stupid hack, but it's still quicker than doing this
  // row by row.
  bit_pointer::FillWithTrue(result, row_count);
  AndNot(input, result, row_count, result);
}

template<OperatorId op>
inline void BitLogic(bit_pointer::bit_const_ptr left,
                     bit_pointer::bit_const_ptr right,
                     size_t row_count,
                     bit_pointer::bit_ptr result) {
  // We delegate the calculation to a vector binary primitive, which
  // encapsulates SIMD.
  // TODO(onufry): Proper behaviour for non-aligned and even non-equi-aligned
  // vectors has to be added.
  DCHECK(left.is_aligned());
  DCHECK(right.is_aligned());
  DCHECK(result.is_aligned());
  VectorBinaryPrimitive<op, DirectIndexResolver, DirectIndexResolver,
      UINT32, UINT32, UINT32, false> bit_operator;
  bool res = bit_operator(left.data(), right.data(), NULL, NULL,
                          (row_count + 31) / 32, result.data(), NULL);
  DCHECK(res) << "Error in " << BinaryExpressionTraits<op>::name()
              << "binary vector primitive calculation.";
}

void Or(bit_pointer::bit_const_ptr left,
        bit_pointer::bit_const_ptr right,
        size_t row_count,
        bit_pointer::bit_ptr result) {
  BitLogic<OPERATOR_BITWISE_OR>(left, right, row_count, result);
}

void And(bit_pointer::bit_const_ptr left,
         bit_pointer::bit_const_ptr right,
         size_t row_count,
         bit_pointer::bit_ptr result) {
  BitLogic<OPERATOR_BITWISE_AND>(left, right, row_count, result);
}

void AndNot(bit_pointer::bit_const_ptr left,
            bit_pointer::bit_const_ptr right,
            size_t row_count,
            bit_pointer::bit_ptr result) {
  BitLogic<OPERATOR_BITWISE_ANDNOT>(left, right, row_count, result);
}

void Not(bit_pointer::bit_const_ptr left,
         size_t row_count,
         bit_pointer::bit_ptr result) {
  // TODO(onufry): SIMD this, or at least proceed 64 bits at a time.
  while (row_count--) {
    *result = !*left;
    ++result;
    ++left;
  }
}

}  // namespace vector_logic
}  // namespace supersonic
