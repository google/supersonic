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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#ifndef SUPERSONIC_EXPRESSION_VECTOR_COLUMN_VALIDITY_CHECKERS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_COLUMN_VALIDITY_CHECKERS_H_

// For trunc, to check the integrity of a double argument.
#include <math.h>
#include <stddef.h>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/bit_pointers.h"

namespace supersonic {

// ----------------------------  FAILERS ----------------------------
// General note - we are counting failures instead of breaking on each failure
// due to a hunch of mine that in the standard case (that is, no failures) this
// should be quicker, as it avoids branching.
// TODO(onufry): check this, and possibly change the semantics.

namespace failers {

// TODO(onufry): maybe replace at least some of these checkers by SIMD
// operations.

// Returns the number of non-positive, non-null entries in the data.
struct IsNonPositiveFailer {
  template<typename T>
  int operator()(const T* input_data,
                 bool_const_ptr input_is_null,
                 size_t row_count) {
    int failures = 0;
    if (input_is_null == NULL) {
      for (int i = 0; i < row_count; ++i) failures += (input_data[i] <= 0);
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures += (input_data[i] <= 0 && !input_is_null[i]);
      }
    }
    return failures;
  }
};

// Returns the number of negative, non-null entries in the data.
struct IsNegativeFailer {
  template<typename T>
  int operator()(const T* input_data,
                 bool_const_ptr input_is_null,
                 size_t row_count) {
    int failures = 0;
    if (input_is_null == NULL) {
      for (int i = 0; i < row_count; ++i) failures += (input_data[i] < 0);
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures += (!input_is_null[i] && input_data[i] < 0);
      }
    }
    return failures;
  }
};

// Returns the number of rows where the right field is zero and not null.
struct SecondColumnZeroFailer {
  template<typename T1, typename T2>
  int operator()(const T1* left_data,
                 bool_const_ptr left_is_null,
                 const T2* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count) {
    int failures = 0;
    if (right_is_null == NULL) {
      for (int i = 0; i < row_count; ++i) failures += (right_data[i] == 0);
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures += (!right_is_null[i] && right_data[i] == 0);
      }
    }
    return failures;
  }
};

// Returns the number of rows where the right field is negative and not null.
struct SecondColumnNegativeFailer {
  template<typename T1, typename T2>
  int operator()(const T1* left_data,
                 bool_const_ptr left_is_null,
                 const T2* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count) {
    int failures = 0;
    if (right_is_null == NULL) {
      for (int i = 0; i < row_count; ++i) failures += (right_data[i] < 0);
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures += (!right_is_null[i] && right_data[i] < 0);
      }
    }
    return failures;
  }
};

// Returns the number of rows where the first column is negative and not null,
// while the second is not integer. Used in the Power function.
struct FirstColumnNegativeAndSecondNonIntegerFailer {
  template<typename T1, typename T2>
  int operator()(const T1* left_data,
                 bool_const_ptr left_is_null,
                 const T2* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count) {
    int failures = 0;
    if (right_is_null == NULL) {
      for (int i = 0; i < row_count; ++i)
        failures += ((right_data[i] != trunc(right_data[i]))
                      && (left_data[i] < 0));
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures +=  !left_is_null[i]
                     && !right_is_null[i]
                     && left_data[i] < 0
                     && (right_data[i] != trunc(right_data[i]));
      }
    }
    return failures;
  }
};

// Returns the number of rows where the second or third column is negative.
struct SecondOrThirdColumnNegativeFailer {
  template<typename T1, typename T2, typename T3>
  int operator()(const T1* left_data,
                 bool_const_ptr left_is_null,
                 const T2* middle_data,
                 bool_const_ptr middle_is_null,
                 const T3* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count) {
    int failures = 0;
    for (int i = 0; i < row_count; ++i) {
      failures += (((middle_is_null == NULL || !middle_is_null[i]) &&
                    middle_data[i] < 0LL) ||
                   ((right_is_null == NULL || !right_is_null[i]) &&
                    right_data[i] < 0LL));
    }
    return failures;
  }
};

}  // namespace failers

// ----------------------  Nullers  ----------------------------

namespace nullers {

// Sets result_is_null for all the fields where the input is not positive.
struct IsNonPositiveNuller {
  template<typename T>
  void operator()(const T* input_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= (input_data[i] <= 0);
    }
  }
};

// Sets result_is_null for all the fields where the input is negative.
struct IsNegativeNuller {
  template<typename T>
  void operator()(const T* input_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= (input_data[i] < 0);
    }
  }
};

// Sets result_is_null for all the rows where the second field is zero.
struct SecondColumnZeroNuller {
  template<typename T1, typename T2>
  void operator()(const T1* left_data,
                  const T2* right_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= (right_data[i] == 0);
    }
  }
};

// Sets result_is_null for all the rows where the second field is negative.
struct SecondColumnNegativeNuller {
  template<typename T1, typename T2>
  void operator()(const T1* left_data,
                  const T2* right_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= (right_data[i] < 0);
    }
  }
};

// Sets result_is_null for all the rows where the first column is negative and
// not null, while the second is not integer. Used in the Power function.
struct FirstColumnNegativeAndSecondNonIntegerNuller {
  template<typename T1, typename T2>
  void operator()(const T1* left_data,
                  const T2* right_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= ((right_data[i] != trunc(right_data[i]))
                            && (left_data[i] < 0));
    }
  }
};

}  // namespace nullers
}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_COLUMN_VALIDITY_CHECKERS_H_
