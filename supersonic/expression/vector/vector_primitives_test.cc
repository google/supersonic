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

#include "supersonic/expression/vector/vector_primitives.h"

#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/memory/memory.h"
#include "gtest/gtest.h"

namespace supersonic {

TEST(VectorBinaryPrimitiveTest, AddDirect) {
  VectorBinaryPrimitive<OPERATOR_ADD, DirectIndexResolver, DirectIndexResolver,
                        INT32, INT32, INT32, false> addOperation;
  const int32 input1[]   = {1, INT_MAX, -5, 0, INT_MAX, 13};
  const int32 input2[]   = {1, INT_MIN,  0, 0,       1, 14};
  int32 result[6];
  result[5] = 451;

  addOperation(input1, input2, NULL, NULL, 5, result, NULL);
  ASSERT_EQ(2, result[0]);
  ASSERT_EQ(-1, result[1]);
  ASSERT_EQ(-5, result[2]);
  ASSERT_EQ(0, result[3]);
  ASSERT_EQ(INT_MIN, result[4]);

  // We asked to calculate 5 first items only.
  // The 6th should have not been touch
  ASSERT_EQ(451, result[5]);
}

TEST(VectorBinaryPrimitiveTest, AddIndirect) {
  VectorBinaryPrimitive<OPERATOR_ADD, DirectIndexResolver,
      IndirectIndexResolver, INT32, INT32, INT32, false> addOperation;
  const int32 input1[]   = {1, INT_MAX, -5, 0, INT_MAX};
  const int32 input2[]   = {1, INT_MIN,  0, 0,       1};
  const index_t indirect[] = {4, 3, 2, 1, 0};
  int32 result[5];

  addOperation(input1, input2, NULL, indirect, 5, result, NULL);
  ASSERT_EQ(2, result[0]);
  ASSERT_EQ(INT_MAX, result[1]);
  ASSERT_EQ(-5, result[2]);
  ASSERT_EQ(INT_MIN, result[3]);
  ASSERT_EQ(INT_MIN, result[4]);
}


template <typename data_type> struct Normalize {
  data_type operator()(data_type arg) { return arg;}
  void NormalizeVector(data_type* data, index_t size) const {}
};

template <> struct Normalize<bool> {
  bool operator()(bool arg) const {
    // This volatile is important. In -c opt mode optimizer is skipping the
    // normalization process.
    volatile int b = arg;
    return b > 0 ? true : false;
  }
  void NormalizeVector(bool* data, index_t size) const {
    for (int i = 0; i < size; ++i) {
      data[i] = operator()(data[i]);
    }
  };
};

template <> struct Normalize<double> {
  double operator()(double arg) const {
    return  isnan(arg) ? 0.451 : arg;  // We want to have  nan == nan.
  };
  void NormalizeVector(double* data, index_t size) const {}
};

template <> struct Normalize<float> {
  float operator()(float arg) const {
    return  isnan(arg) ? 0.451 : arg;  // We want to have  nan == nan.
  };
  void NormalizeVector(float* data, index_t size) const {}
};

//============================= General tests =================================

// Set of strange values that will cause many overflows in arithmetic.
const int SIZE_PATTERN = 16;
const int32 LEFT_PATTERN[SIZE_PATTERN] =
    {0xdead, 0xff00, 0x0000, 0x00ff, 0x0f0f, 0xffff, 0xcccc, 0xcafe,
     0xcccc, 0xffff, 0x0000, 0x9999, 0x6666, 0xefff, 0xefff, 0xefef};
const int32 RIGHT_PATTERN[SIZE_PATTERN] =
    {0xbeaf, 0xff00, 0x0000, 0xff00, 0xf0f0, 0xffff, 0xcccc, 0xbabe,
     0xcccc, 0xefff, 0x1234, 0x6666, 0x0001, 0xefff, 0xefff, 0xefef};

class AbstractPrimitiveTest {
 protected:
  void FillWithData(void* dst, int dst_size_bytes, const int32* pattern) {
      memcpy(dst, pattern,
             min(static_cast<int64>(dst_size_bytes),
                 static_cast<int64>(SIZE_PATTERN * sizeof(*pattern))));
      for (int i = SIZE_PATTERN * sizeof(*pattern); i < dst_size_bytes; ++i) {
        (reinterpret_cast<char*>(dst))[i] = static_cast<char>(i);
      }
    }

  template <typename CppDataType>
  CppDataType *MallocAlignedPlusOffset(int size, int offset) {
    const int new_size = (((size*sizeof(CppDataType)+offset)+15)/16)*16;
    CppDataType *res;
    posix_memalign(reinterpret_cast<void**>(&res), 16, new_size);
    return reinterpret_cast<CppDataType*>(
        reinterpret_cast<uint64>(res) + offset);
  }

  void DemallocAligned(void *ptr) {
    free(reinterpret_cast<void**>((reinterpret_cast<uint64>(ptr) / 16) * 16));
  }
};

template <OperatorId operation_type,
          DataType result_type, DataType left_type, DataType right_type>
class VectorBinaryPrimitiveTest : public testing::Test, AbstractPrimitiveTest {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;
  typedef typename BinaryExpressionTraits<operation_type>::basic_operator Op;

 public:
  void RunDirectSimdTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          DirectIndexResolver, DirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 0);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 0);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    computer(left, right, NULL, NULL, size, result, NULL);
    VerifyDirect(left, right, size, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunDirectSkipRightZeroTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          DirectIndexResolver, DirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 0);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 0);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    bool_array skip_list_data;
    skip_list_data.Reallocate(size, HeapBufferAllocator::Get());
    bool_ptr skip_list = skip_list_data.mutable_data();
    for (int i = 0; i < size; ++i) {
      skip_list[i] = (right[i] == 0);
    }
    computer(left, right, NULL, NULL, size, skip_list, result, NULL);
    VerifyDirect(left, right, size, skip_list, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunDirectSimdWithOffsetTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          DirectIndexResolver, DirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 8);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 8);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 8);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    computer(left, right, NULL, NULL, size, result, NULL);
    VerifyDirect(left, right, size, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunDirectNoSimdWithDiffrentOffsetTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          DirectIndexResolver, DirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 8);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 0);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 8);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    computer(left, right, NULL, NULL, size, result, NULL);
    VerifyDirect(left, right, size, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunDirectNoSimdWithStrangeOffsetTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          DirectIndexResolver, DirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 1);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 1);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 1);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    computer(left, right, NULL, NULL, size, result, NULL);
    VerifyDirect(left, right, size, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunIndirectTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          IndirectIndexResolver, IndirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 0);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 0);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    std::unique_ptr<index_t[]> left_ind(new index_t[size]);
    std::unique_ptr<index_t[]> right_ind(new index_t[size]);
    for (int i = 0; i < size; ++i) {
      left_ind[i] = size - i - 1;
      right_ind[i] = (i * 451) % size;
    }

    computer(left, right, left_ind.get(), right_ind.get(), size, result, NULL);
    VerifyIndirect(left, right, left_ind.get(), right_ind.get(), size, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

  void RunIndirectSkipRightZeroTest(int size) {
    VectorBinaryPrimitive<operation_type,
                          IndirectIndexResolver, IndirectIndexResolver,
                          result_type, left_type, right_type, false> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    LeftCppType*   left   = MallocAlignedPlusOffset<LeftCppType>(size, 0);
    RightCppType*  right  = MallocAlignedPlusOffset<RightCppType>(size, 0);
    FillWithDataLeft(left, size);
    FillWithDataRight(right, size);

    std::unique_ptr<index_t[]> left_ind(new index_t[size]);
    std::unique_ptr<index_t[]> right_ind(new index_t[size]);
    for (int i = 0; i < size; ++i) {
      left_ind[i] = size - i - 1;
      right_ind[i] = (i * 451) % size;
    }

    bool_array skip_list_data;
    skip_list_data.Reallocate(size, HeapBufferAllocator::Get());
    bool_ptr skip_list = skip_list_data.mutable_data();
    for (int i = 0; i < size; ++i) {
      skip_list[i] = (right[right_ind[i]] == 0);
    }

    computer(left, right, left_ind.get(), right_ind.get(), size,
             skip_list, result, NULL);
    VerifyIndirect(left, right, left_ind.get(), right_ind.get(), size,
                   skip_list, result);

    DemallocAligned(result);
    DemallocAligned(left);
    DemallocAligned(right);
  }

 protected:
  void VerifyDirect(const LeftCppType *left, const RightCppType *right,
                    int size,
                    const ResultCppType *result) {
    VerifyDirect(left, right, size, bool_ptr(NULL), result);
  }

  void VerifyDirect(const LeftCppType *left, const RightCppType *right,
                    int size, bool_const_ptr skip_list,
                    const ResultCppType *result) {
      Op op;
      Normalize<ResultCppType> norm;
      for (int i = 0; i < size; ++i) {
        if (!(skip_list != NULL && skip_list[i])) {
          ASSERT_EQ(norm(op(left[i], right[i])), norm(result[i]))
            << "i=" << i << "; left[i]=" << left[i] << "; right[i]=" << right[i]
            << "; size=" << size;
        }
      }
  }

  void VerifyIndirect(const LeftCppType* left, const RightCppType* right,
                      const index_t* left_ind, const index_t* right_ind,
                      int size,
                      const ResultCppType* result) {
    VerifyIndirect(left, right, left_ind, right_ind, size, bool_ptr(NULL),
                   result);
  }

  void VerifyIndirect(const LeftCppType* left, const RightCppType* right,
                      const index_t* left_ind, const index_t* right_ind,
                      int size, bool_const_ptr skip_list,
                      const ResultCppType* result) {
      Op op;
      Normalize<ResultCppType> norm;
      for (int i = 0; i < size; ++i) {
        if (!(skip_list != NULL && skip_list[i])) {
          ASSERT_EQ(norm(op(left[left_ind[i]], right[right_ind[i]])),
                    norm(result[i]))
              << "i=" << i
              << "; left[left_ind[i]]=" << left[left_ind[i]]
              << "; right[right_ind[i]]=" << right[right_ind[i]]
              << "; left_ind[i]=" << left_ind[i]
              << "; right_ind[i]=" << right_ind[i]
              << "; size=" << size;
        }
      }
    }


  void FillWithDataLeft(LeftCppType* left, int size) {
    FillWithData(left, size * sizeof(*left), LEFT_PATTERN);
    Normalize<LeftCppType> norm;
    norm.NormalizeVector(left, size);
  }

  void FillWithDataRight(RightCppType* right, int size) {
    FillWithData(right, size * sizeof(*right), RIGHT_PATTERN);
    Normalize<RightCppType> norm;
    norm.NormalizeVector(right, size);
  };
};

#define TEST_F_BINARY_PRIMITIVE(op, res_type, ltype, rtype)                    \
  typedef VectorBinaryPrimitiveTest<op, res_type, ltype, rtype>                \
      VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype;         \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunDirectSimdTest) {                                                  \
    for (int i = 0; i < 50; ++i) {RunDirectSimdTest(i);}                       \
  }                                                                            \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunDirectSimdWithOffsetTest) {                                        \
    for (int i = 0; i < 50; ++i) {RunDirectSimdWithOffsetTest(i);}             \
  }                                                                            \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunDirectNoSimdWithStrangeOffsetTest) {                               \
      for (int i = 0; i < 50; ++i) {RunDirectNoSimdWithStrangeOffsetTest(i);}  \
    }                                                                          \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunDirectNoSimdWithDiffrentOffsetTest) {                              \
      for (int i = 0; i < 50; ++i) {RunDirectNoSimdWithDiffrentOffsetTest(i);} \
    }                                                                          \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunDirectSkipRightZeroTest) {                                         \
      for (int i = 0; i < 50; ++i) {RunDirectSkipRightZeroTest(i);}            \
    }                                                                          \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunIndirectTest) {                                                    \
      for (int i = 0; i < 50; ++i) {RunIndirectTest(i);}                       \
    }                                                                          \
  TEST_F(VectorBinaryPrimitiveTest_##op##_##res_type##_##ltype##_##rtype,      \
         RunIndirectSkipRightZeroTest) {                                       \
      for (int i = 0; i < 50; ++i) {RunIndirectSkipRightZeroTest(i);}          \
    }

// ======================= General unary tests =================================

template <OperatorId operation_type, DataType result_type, DataType arg_type>
class VectorUnaryPrimitiveTest : public testing::Test, AbstractPrimitiveTest {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<arg_type>::cpp_type ArgCppType;
  typedef typename UnaryExpressionTraits<operation_type>::basic_operator Op;

 public:
  void RunDirectTest(int size) {
    VectorUnaryPrimitive<operation_type, DirectIndexResolver,
                         result_type, arg_type> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    ArgCppType*    arg    = MallocAlignedPlusOffset<ArgCppType>(size, 0);
    FillWithData(arg, size * sizeof(*arg), LEFT_PATTERN);

    computer(arg, NULL, size, result);
    VerifyDirect(arg, size, result);

    DemallocAligned(result);
    DemallocAligned(arg);
  }


  void RunIndirectTest(int size) {
    VectorUnaryPrimitive<operation_type, IndirectIndexResolver,
                         result_type, arg_type> computer;

    ResultCppType* result = MallocAlignedPlusOffset<ResultCppType>(size, 0);
    ArgCppType*    arg    = MallocAlignedPlusOffset<ArgCppType>(size, 0);
    FillWithData(arg, size * sizeof(*arg), LEFT_PATTERN);

    std::unique_ptr<index_t[]> ind(new index_t[size]);
    for (int i = 0; i < size; ++i) {
      ind[i] = size - i;
    }

    computer(arg,  ind.get(), size, result, NULL);
    VerifyIndirect(arg, ind.get(), size, result);

    DemallocAligned(result);
    DemallocAligned(arg);
  }

 protected:
  void VerifyDirect(const ArgCppType* arg,
                    int size,
                    const ResultCppType *result) {
    Op op;
    Normalize<ResultCppType> norm;
    for (int i = 0; i < size; ++i) {
      ASSERT_EQ(norm(op(arg[i])), norm(result[i]))
          << "i=" << i << "; arg[i]=" << arg[i];
    }
  }

  void VerifyIndirect(const ArgCppType* arg,
                      const index_t* ind,
                      int size,
                      const ResultCppType *result) {
    Op op;
    Normalize<ResultCppType> norm;
    for (int i = 0; i < size; ++i) {
      ASSERT_EQ(norm(op(arg[ind[i]])),
                norm(result[i]))
          << "i=" << i
          << "; arg[ind[i]]=" << arg[ind[i]]
          << "; ind[i]=" << ind[i];
    }
  }
};

#define TEST_F_UNARY_PRIMITIVE(op, res_type, arg_type)                         \
  typedef VectorUnaryPrimitiveTest<op, res_type, arg_type>                     \
      VectorUnaryPrimitiveTest_##op##_##res_type##_##arg_type;                 \
  TEST_F(VectorUnaryPrimitiveTest_##op##_##res_type##_##arg_type,              \
         RunDirectTest) {                                                      \
    for (int i = 0; i < 50; ++i) {RunDirectTest(i);}                           \
  }                                                                            \
  TEST_F(VectorUnaryPrimitiveTest_##op##_##res_type##_##arg_type,              \
         RunIndirectTest) {                                                    \
    for (int i = 0; i < 50; ++i) {RunIndirectTest(i);}                         \
  }

TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, BOOL,   BOOL,   BOOL);
TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, INT32,  INT32,  INT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, UINT32, UINT32, UINT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, INT64,  INT64,  INT64);
TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, DOUBLE, DOUBLE, DOUBLE);
TEST_F_BINARY_PRIMITIVE(OPERATOR_ADD, FLOAT,  FLOAT,  FLOAT);

TEST_F_BINARY_PRIMITIVE(OPERATOR_SUBTRACT, INT32,  INT32,  INT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_SUBTRACT, UINT32, UINT32, UINT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_SUBTRACT, INT64,  INT64,  INT64);
TEST_F_BINARY_PRIMITIVE(OPERATOR_SUBTRACT, DOUBLE, DOUBLE, DOUBLE);
TEST_F_BINARY_PRIMITIVE(OPERATOR_SUBTRACT, FLOAT,  FLOAT,  FLOAT);

TEST_F_BINARY_PRIMITIVE(OPERATOR_MULTIPLY, INT32,  INT32,  INT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_MULTIPLY, UINT32, UINT32, UINT32);
TEST_F_BINARY_PRIMITIVE(OPERATOR_MULTIPLY, INT64,  INT64,  INT64);
TEST_F_BINARY_PRIMITIVE(OPERATOR_MULTIPLY, DOUBLE, DOUBLE, DOUBLE);
TEST_F_BINARY_PRIMITIVE(OPERATOR_MULTIPLY, FLOAT,  FLOAT,  FLOAT);

typedef VectorBinaryPrimitiveTest<OPERATOR_DIVIDE_SIGNALING,
                                  INT32, INT32, INT32>
     VectorBinaryPrimitiveTest_DIVIDE_INT32_INT32_INT32;
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_INT32_INT32_INT32,
       RunDirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunDirectSkipRightZeroTest(i);
  }
}
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_INT32_INT32_INT32,
       RunIndirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunIndirectSkipRightZeroTest(i);
  }
}

typedef VectorBinaryPrimitiveTest<OPERATOR_DIVIDE_SIGNALING,
                                  UINT32, UINT32, UINT32>
     VectorBinaryPrimitiveTest_DIVIDE_UINT32_UINT32_UINT32;
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_UINT32_UINT32_UINT32,
       RunDdirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunDirectSkipRightZeroTest(i);
  }
}
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_UINT32_UINT32_UINT32,
       RunIndirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunIndirectSkipRightZeroTest(i);
  }
}

typedef VectorBinaryPrimitiveTest<OPERATOR_DIVIDE_SIGNALING,
                                  INT64, INT64, INT64>
     VectorBinaryPrimitiveTest_DIVIDE_INT64_INT64_INT64;
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_INT64_INT64_INT64,
       RunDirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunDirectSkipRightZeroTest(i);
  }
}
TEST_F(VectorBinaryPrimitiveTest_DIVIDE_INT64_INT64_INT64,
       RunIndirectSkipRightZeroTest) {
  for (int i = 0; i < 50; ++i) {
    RunIndirectSkipRightZeroTest(i);
  }
}

TEST_F_BINARY_PRIMITIVE(OPERATOR_DIVIDE_SIGNALING, DOUBLE, DOUBLE, DOUBLE);
TEST_F_BINARY_PRIMITIVE(OPERATOR_DIVIDE_SIGNALING, FLOAT,  FLOAT,  FLOAT);

TEST_F_BINARY_PRIMITIVE(OPERATOR_OR,  BOOL, BOOL, BOOL);
TEST_F_BINARY_PRIMITIVE(OPERATOR_AND, BOOL, BOOL, BOOL);
TEST_F_BINARY_PRIMITIVE(OPERATOR_AND_NOT, BOOL, BOOL, BOOL);

TEST_F_UNARY_PRIMITIVE(OPERATOR_COPY, DATE, DATE);

}  // namespace supersonic
