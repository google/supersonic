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
// A set of simple vectorized operators. Each takes a number of pointers to
// input arrays,  the number of rows, and a pointer to an array in which the
// results are to be stored, and performs the appropriate operation.
//
//

#ifndef SUPERSONIC_EXPRESSION_VECTOR_VECTOR_LOGIC_H_
#define SUPERSONIC_EXPRESSION_VECTOR_VECTOR_LOGIC_H_

#include <stddef.h>

#include "supersonic/base/infrastructure/bit_pointers.h"

namespace supersonic {

namespace vector_logic {

// left || right.
void Or(const bool* left,
        const bool* right,
        size_t row_count,
        bool* result);
// left && right.
void And(const bool* left,
         const bool* right,
         size_t row_count,
         bool* result);
// (!left) && right.
void AndNot(const bool* left,
            const bool* right,
            size_t row_count,
            bool* result);

// !left.
// NOTE(onufry): This is not SIMD, it's way slower than the previous three.
void Not(const bool* left,
         size_t row_count,
         bool* result);

// left | right.
void Or(bit_pointer::bit_const_ptr left,
        bit_pointer::bit_const_ptr right,
        size_t bit_count,
        bit_pointer::bit_ptr result);
// left & right.
void And(bit_pointer::bit_const_ptr left,
         bit_pointer::bit_const_ptr right,
         size_t bit_count,
         bit_pointer::bit_ptr result);
// (~left) & right.
void AndNot(bit_pointer::bit_const_ptr left,
            bit_pointer::bit_const_ptr right,
            size_t bit_count,
            bit_pointer::bit_ptr result);

// ~left.
// TODO(onufry): This is not SIMD, it's way slower than the previous three. Add
// SIMD.
void Not(bit_pointer::bit_const_ptr left,
         size_t row_count,
         bit_pointer::bit_ptr result);

}  // namespace vector_logic
}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_VECTOR_LOGIC_H_
