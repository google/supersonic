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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// Provides factories to build templated bound expressions. I expect this
// will only ever be included in abstract_expressions.h, and separate it for
// readability reasons.

#ifndef SUPERSONIC_EXPRESSION_TEMPLATED_BOUND_EXPRESSION_FACTORY_H_
#define SUPERSONIC_EXPRESSION_TEMPLATED_BOUND_EXPRESSION_FACTORY_H_

#include <stddef.h>
#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/abstract_bound_expressions.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Resolves a BoundExpression child to be of type result_type.
// If promote is set, may perform a numerical promotion.
// Assumes ownership of the child.
//
// Promotion may be a bit counter-intuitive. What can happen is:
// - any two integer types can be silently converted one to the other,
// - any integer type can be converted to a double or a float,
// - DATE can be converted to DATETIME.
class BufferAllocator;

FailureOrOwned<BoundExpression> ResolveTypePromotion(BoundExpression* child,
                                                     DataType result_type,
                                                     BufferAllocator* allocator,
                                                     rowcount_t row_capacity,
                                                     bool promote);

// Calculates the common datatype for two types. Fails if the datatypes cannot
// be reconciled.
// TODO(onufry): Move this function to utils.
FailureOr<DataType> CalculateCommonType(DataType t1, DataType t2);
// A convenience wrapper for CalculateCommonType(left.type, right.type).
FailureOr<DataType> CalculateCommonExpressionType(BoundExpression* left,
                                                  BoundExpression* right);

// ----------------------- Unary Expression Factories -----------------

// Assumes ownership of the child.
template<OperatorId op, DataType from_type, DataType to_type>
FailureOrOwned<BoundExpression> CreateTypedBoundUnaryExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* child) {
  CHECK_EQ(1, child->result_schema().attribute_count());
  const bool promote = UnaryExpressionTraits<op>::supports_promotions;
  FailureOrOwned<BoundExpression> promoted = ResolveTypePromotion(
      child, from_type, allocator, row_capacity, promote);
  if (promoted.is_failure()) return promoted;
  return AbstractBoundUnary<op, from_type, to_type>(promoted.release(),
                                                    allocator,
                                                    row_capacity);
}

struct UnaryExpressionFactory {
 public:
  virtual FailureOrOwned<BoundExpression> create_expression(
      BufferAllocator* const allocator, rowcount_t row_capacity,
      BoundExpression* child) = 0;

  virtual ~UnaryExpressionFactory() {}
};

// A factory of unary expressions. The sense of having it as a separate
// entity is to avoid passing all the arguments through all the switches and
// helper functions of dynamic schema determination. It creates
// AbstractBoundExpressions with appropriate template parameters.
//
// The contract is that all the functions returning a pointer to a factory will
// return NULL on argument-type mismatch, which will be later interpreted into
// an Exception by the RunUnaryFactory function.
//
// TODO(onufry): This behaviour should be changed to return a Result, with the
// description of the type mismatch.
//
// TODO(onufry): All the contents of this file could be converted to
// appropriate template magic using types_infrastructure.h.
template<OperatorId op, DataType from_type, DataType to_type>
struct SpecializedUnaryFactory : public UnaryExpressionFactory {
 public:
  virtual FailureOrOwned<BoundExpression> create_expression(
      BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) {
    return CreateTypedBoundUnaryExpression<op, from_type, to_type>(
       allocator, row_capacity, child);
  }
};

// The output type is to be the signed version of the (numeric) input type.
template<OperatorId op>
UnaryExpressionFactory* CreateSignedTypeUnaryFactory(DataType type) {
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, INT32, INT32>();
    case INT64: return new SpecializedUnaryFactory<op, INT64, INT64>();
    case UINT32: return new SpecializedUnaryFactory<op, UINT32, INT32>();
    case UINT64: return new SpecializedUnaryFactory<op, UINT64, INT64>();
    case FLOAT: return new SpecializedUnaryFactory<op, FLOAT, FLOAT>();
    case DOUBLE: return new SpecializedUnaryFactory<op, DOUBLE, DOUBLE>();
    default: return NULL;
  }
}

// The input type is the specified integer type.
template<OperatorId op, DataType to_type>
UnaryExpressionFactory* CreateIntegerInputUnaryFactory(DataType type) {
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, INT32, to_type>();
    case INT64: return new SpecializedUnaryFactory<op, INT64, to_type>();
    case UINT32: return new SpecializedUnaryFactory<op, UINT32, to_type>();
    case UINT64: return new SpecializedUnaryFactory<op, UINT64, to_type>();
    default: return NULL;
  }
}

// The input type is the specified numeric type.
template<OperatorId op, DataType to_type>
UnaryExpressionFactory* CreateNumericInputUnaryFactory(DataType type) {
  switch (type) {
    case FLOAT: return new SpecializedUnaryFactory<op, FLOAT, to_type>();
    case DOUBLE: return new SpecializedUnaryFactory<op, DOUBLE, to_type>();
    default: return CreateIntegerInputUnaryFactory<op, to_type>(type);
  }
}

// The output type is the specified numeric type.
template<OperatorId op, DataType from_type>
UnaryExpressionFactory* CreateNumericOutputUnaryFactory(DataType type) {
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, from_type, INT32>();
    case UINT32: return new SpecializedUnaryFactory<op, from_type, UINT32>();
    case INT64: return new SpecializedUnaryFactory<op, from_type, INT64>();
    case UINT64: return new SpecializedUnaryFactory<op, from_type, UINT64>();
    case FLOAT: return new SpecializedUnaryFactory<op, from_type, FLOAT>();
    case DOUBLE: return new SpecializedUnaryFactory<op, from_type, DOUBLE>();
    default: return NULL;
  }
}

// The input and output types are the specified numeric types.
template<OperatorId op>
UnaryExpressionFactory* CreateNumericUnaryFactory(DataType in_type,
                                                  DataType out_type) {
  switch (out_type) {
    case INT32: return CreateNumericInputUnaryFactory<op, INT32>(in_type);
    case INT64: return CreateNumericInputUnaryFactory<op, INT64>(in_type);
    case UINT32: return CreateNumericInputUnaryFactory<op, UINT32>(in_type);
    case UINT64: return CreateNumericInputUnaryFactory<op, UINT64>(in_type);
    case FLOAT: return CreateNumericInputUnaryFactory<op, FLOAT>(in_type);
    case DOUBLE: return CreateNumericInputUnaryFactory<op, DOUBLE>(in_type);
    default: return NULL;
  }
}

// The input type is the specified type.
template<OperatorId op, DataType to_type>
UnaryExpressionFactory* CreateArbitraryInputUnaryFactory(DataType type) {
  switch (type) {
    case BOOL: return new SpecializedUnaryFactory<op, BOOL, to_type>();
    case DATE: return new SpecializedUnaryFactory<op, DATE, to_type>();
    case DATETIME: return new SpecializedUnaryFactory<op, DATETIME, to_type>();
    case STRING: return new SpecializedUnaryFactory<op, STRING, to_type>();
    case BINARY: return new SpecializedUnaryFactory<op, BINARY, to_type>();
    case ENUM: return new SpecializedUnaryFactory<op, ENUM, to_type>();
    case DATA_TYPE:
        return new SpecializedUnaryFactory<op, DATA_TYPE, to_type>();
    default: return CreateNumericInputUnaryFactory<op, to_type>(type);
  }
}

// The input type is the specified type, which has to be floating point (used
// by ROUND). The output type is given by the template parameter.
// TODO(onufry): move this over to math_expressions.cc.
template<OperatorId op, DataType to_type>
UnaryExpressionFactory* CreateFloatingInputUnaryFactory(DataType type) {
  switch (type) {
    case FLOAT: return new SpecializedUnaryFactory<op, FLOAT, to_type>();
    case DOUBLE: return new SpecializedUnaryFactory<op, DOUBLE, to_type>();
    default: return NULL;
  }
}

// The input is given by the template parameter, the output is given at runtime.
template<OperatorId op, DataType from_type>
UnaryExpressionFactory* CreateArbitraryOutputUnaryFactory(DataType type) {
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, from_type, INT32>();
    case INT64: return new SpecializedUnaryFactory<op, from_type, INT64>();
    case UINT32: return new SpecializedUnaryFactory<op, from_type, UINT32>();
    case UINT64: return new SpecializedUnaryFactory<op, from_type, UINT64>();
    case FLOAT: return new SpecializedUnaryFactory<op, from_type, FLOAT>();
    case DOUBLE: return new SpecializedUnaryFactory<op, from_type, DOUBLE>();
    case BOOL: return new SpecializedUnaryFactory<op, from_type, BOOL>();
    case DATE: return new SpecializedUnaryFactory<op, from_type, DATE>();
    case DATETIME:
        return new SpecializedUnaryFactory<op, from_type, DATETIME>();
    case STRING: return new SpecializedUnaryFactory<op, from_type, STRING>();
    case BINARY: return new SpecializedUnaryFactory<op, from_type, BINARY>();
    case ENUM: return new SpecializedUnaryFactory<op, from_type, ENUM>();
    case DATA_TYPE:
        return new SpecializedUnaryFactory<op, from_type, DATA_TYPE>();
  }
  return NULL;  // This should never be used.
}

// The input type is given on the input, the output type is equal to the input
// type. The input has to be floating point.
template<OperatorId op>
UnaryExpressionFactory* CreateFloatingUnaryFactory(DataType type) {
  switch (type) {
    case FLOAT: return new SpecializedUnaryFactory<op, FLOAT, FLOAT>();
    case DOUBLE: return new SpecializedUnaryFactory<op, DOUBLE, DOUBLE>();
    default: return NULL;
  }
}

// Takes ownership of both child and factory. Destroys the factory after
// using it to create the bound expression. Deals with errors on factory
// creation (which are signalled by factory_ptr being NULL).
FailureOrOwned<BoundExpression> RunUnaryFactory(
    UnaryExpressionFactory* factory_ptr,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr,
    const string& operation_name);

// Takes ownership of the child and passes it along to RunUnaryFactory.
// Does not handle errors (signalize by CreateFactory returning NULL), that's
// done in RunUnaryFactory.
// Input has to be numeric, output is the signed version of input.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateUnarySignedNumericExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory =
      CreateSignedTypeUnaryFactory<op>(GetExpressionType(child_ptr));
  // TODO(onufry): Whenever BoundExpression will support a ToString function,
  // it will be better to pass something like
  // UnaryExpressionTratis<op>::FormatDescription(child_ptr->ToString(verbose))
  // instead of simply name (in this and other similar functions).
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// Input is integer, output is specified by the template parameter.
template<OperatorId op, DataType to_type>
FailureOrOwned<BoundExpression> CreateUnaryIntegerInputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory =
      CreateIntegerInputUnaryFactory<op, to_type>(GetExpressionType(child_ptr));
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is numeric, the output type is given by the argument.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateUnaryNumericExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* child_ptr, DataType to_type) {
  UnaryExpressionFactory* factory =
      CreateNumericUnaryFactory<op>(GetExpressionType(child_ptr), to_type);
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is numeric, the output is given by the template parameter.
template<OperatorId op, DataType to_type>
FailureOrOwned<BoundExpression> CreateUnaryNumericInputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory = CreateNumericInputUnaryFactory
      <op, to_type>(GetExpressionType(child_ptr));
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is arbitrary, the output is given by the template parameter.
template<OperatorId op, DataType to_type>
FailureOrOwned<BoundExpression> CreateUnaryArbitraryInputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory = CreateArbitraryInputUnaryFactory
      <op, to_type>(GetExpressionType(child_ptr));
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is a floating point type, the output type is equal to the input
// type.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateUnaryFloatingExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory = CreateFloatingUnaryFactory<op>(
      GetExpressionType(child_ptr));
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is a floating point type, the output is given by the template
// parameter.
template<OperatorId op, DataType to_type>
FailureOrOwned<BoundExpression> CreateUnaryFloatingInputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr) {
  UnaryExpressionFactory* factory = CreateFloatingInputUnaryFactory
      <op, to_type>(GetExpressionType(child_ptr));
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// See comments to CreateUnarySignedNumericExpression.
// The input is given by the template argument, the output is arbitrary and
// given at run-time.
template<OperatorId op, DataType from_type>
FailureOrOwned<BoundExpression> CreateUnaryArbitraryOutputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity, BoundExpression* child_ptr, DataType to_type) {
  UnaryExpressionFactory* factory = CreateArbitraryOutputUnaryFactory
      <op, from_type>(to_type);
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

template<OperatorId op, DataType from_type>
FailureOrOwned<BoundExpression> CreateUnaryNumericOutputExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr,
    DataType to_type) {
  UnaryExpressionFactory* factory =
      CreateNumericOutputUnaryFactory<op, from_type>(to_type);
  return RunUnaryFactory(factory, allocator, row_capacity, child_ptr,
                         UnaryExpressionTraits<op>::name());
}

// ---------------------- Binary Expression Factories ------------------

// Creates a AbstractBoundBinaryExpression. This is the default behaviour for
// the CreateBoundBinaryExpression method of AbstractBinaryExpression.
//
// Separated as a stand-alone function and put here so it can be reused by
// binary expressions that make a dynamic determination of their schemata.
template<OperatorId op, DataType left_type, DataType right_type,
    DataType to_type>
FailureOrOwned<BoundExpression> CreateTypedBoundBinaryExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* left_ptr, BoundExpression* right_ptr) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  CHECK_EQ(1, left->result_schema().attribute_count());
  CHECK_EQ(1, right->result_schema().attribute_count());
  bool promote = BinaryExpressionTraits<op>::supports_promotions;
  FailureOrOwned<BoundExpression> left_promoted = ResolveTypePromotion(
      left.release(), left_type, allocator, row_capacity, promote);
  PROPAGATE_ON_FAILURE(left_promoted);
  FailureOrOwned<BoundExpression> right_promoted = ResolveTypePromotion(
      right.release(), right_type, allocator, row_capacity, promote);
  PROPAGATE_ON_FAILURE(right_promoted);
  return CreateAbstractBoundBinaryExpression<op, left_type, right_type,
      to_type>(left_promoted.release(),  right_promoted.release(),
               allocator, row_capacity);
}

// See UnaryExpressionFactory for comments.
class BinaryExpressionFactory {
 public:
  virtual FailureOrOwned<BoundExpression> create_expression(
      BufferAllocator* const allocator, rowcount_t row_capacity,
      BoundExpression* left, BoundExpression* right) = 0;

  virtual ~BinaryExpressionFactory() {}
};

template<OperatorId op, DataType left_type, DataType right_type,
    DataType to_type>
class SpecializedBinaryFactory : public BinaryExpressionFactory {
 public:
  virtual FailureOrOwned<BoundExpression> create_expression(
      BufferAllocator* const allocator, rowcount_t row_capacity,
      BoundExpression* left, BoundExpression* right) {
    return CreateTypedBoundBinaryExpression<op, left_type, right_type, to_type>(
       allocator, row_capacity, left, right);
  }
};

// All the input types and the output are to be equal and integer.
template<OperatorId op>
BinaryExpressionFactory* CreateThreeEqualIntegerTypesBinaryFactory(
    DataType type) {
  switch (type) {
    case INT32: return new SpecializedBinaryFactory<op, INT32, INT32, INT32>();
    case INT64: return new SpecializedBinaryFactory<op, INT64, INT64, INT64>();
    case UINT32:
      return new SpecializedBinaryFactory<op, UINT32, UINT32, UINT32>();
    case UINT64:
      return new SpecializedBinaryFactory<op, UINT64, UINT64, UINT64>();
    default: return NULL;
  }
}

// All the input types and the output are to be equal and numeric.
template<OperatorId op>
BinaryExpressionFactory* CreateThreeEqualNumericTypesBinaryFactory(
    DataType type) {
  switch (type) {
    case FLOAT: return new SpecializedBinaryFactory<op, FLOAT, FLOAT, FLOAT>();
    case DOUBLE:
      return new SpecializedBinaryFactory<op, DOUBLE, DOUBLE, DOUBLE>();
    default: return CreateThreeEqualIntegerTypesBinaryFactory<op>(type);
  }
}

// All the input types are to be equal.
template<OperatorId op>
BinaryExpressionFactory* CreateThreeEqualTypesBinaryFactory(DataType type) {
  switch (type) {
    case DATE: return new SpecializedBinaryFactory<op, DATE, DATE, DATE>();
    case DATETIME:
      return new SpecializedBinaryFactory<op, DATETIME, DATETIME, DATETIME>();
    case BOOL: return new SpecializedBinaryFactory<op, BOOL, BOOL, BOOL>();
    case STRING:
      return new SpecializedBinaryFactory<op, STRING, STRING, STRING>();
    case BINARY:
      return new SpecializedBinaryFactory<op, BINARY, BINARY, BINARY>();
    case DATA_TYPE:
      return
          new SpecializedBinaryFactory<op, DATA_TYPE, DATA_TYPE, DATA_TYPE>();
    default: return CreateThreeEqualNumericTypesBinaryFactory<op>(type);
  }
}

// Takes ownership of both children and factory. Destroys the factory after
// using it to create the bound expression. Deals with errors on factory
// creation (which are signalled by factory_ptr being NULL).
FailureOrOwned<BoundExpression> RunBinaryFactory(
    BinaryExpressionFactory* factory_ptr,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    const string& operation_name);


// Creates an expression with three equal types (on both inputs and the
// output), with possible casts. Example: IFNULL.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateBinaryEqualTypesExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* left_ptr, BoundExpression* right_ptr) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  FailureOr<DataType> common_type = CalculateCommonExpressionType(left.get(),
                                                                  right.get());
  PROPAGATE_ON_FAILURE(common_type);
  BinaryExpressionFactory* factory = CreateThreeEqualTypesBinaryFactory<op>(
      common_type.get());
  // TODO(onufry): it would be better to pass a FormatDescription instead of
  // name once BoundExpressions support ToString.
  return RunBinaryFactory(factory, allocator, row_capacity, left.release(),
                          right.release(), BinaryExpressionTraits<op>::name());
}

// Standard numeric expression - the inputs are numeric and the output is the
// smallest containing type.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateBinaryNumericExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* left_ptr, BoundExpression* right_ptr) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  FailureOr<DataType> common_type = CalculateCommonExpressionType(left.get(),
                                                                  right.get());
  PROPAGATE_ON_FAILURE(common_type);
  BinaryExpressionFactory* factory =
      CreateThreeEqualNumericTypesBinaryFactory<op>(common_type.get());
  return RunBinaryFactory(factory, allocator, row_capacity, left.release(),
                          right.release(), BinaryExpressionTraits<op>::name());
}

// Standard integer exprsesion - the inputs are integer, the output - their
// smallest containing type. Example - MODULUS.
template<OperatorId op>
FailureOrOwned<BoundExpression> CreateBinaryIntegerExpression(
    BufferAllocator* const allocator, rowcount_t row_capacity,
    BoundExpression* left_ptr, BoundExpression* right_ptr) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  FailureOr<DataType> common_type = CalculateCommonExpressionType(left.get(),
                                                                  right.get());
  PROPAGATE_ON_FAILURE(common_type);
  BinaryExpressionFactory* factory =
      CreateThreeEqualIntegerTypesBinaryFactory<op>(common_type.get());
  return RunBinaryFactory(factory, allocator, row_capacity, left.release(),
                          right.release(), BinaryExpressionTraits<op>::name());
}

// ----------------------- Ternary factories --------------------------------

// Creates a AbstractBoundTernaryExpression. This is the default behaviour for
// the CreateBoundTernaryExpression method of AbstractTernaryExpression.
//
// Separated as a stand-alone function and put here so it can be reused by
// ternary expressions that make a dynamic determination of their schemata.
template<OperatorId op, DataType left_type, DataType middle_type,
    DataType right_type, DataType to_type>
FailureOrOwned<BoundExpression> CreateTypedBoundTernaryExpression(
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* left_ptr,
    BoundExpression* middle_ptr,
    BoundExpression* right_ptr) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> middle(middle_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  CHECK_EQ(1, left->result_schema().attribute_count());
  CHECK_EQ(1, middle->result_schema().attribute_count());
  CHECK_EQ(1, right->result_schema().attribute_count());
  bool promote = TernaryExpressionTraits<op>::supports_promotions;
  FailureOrOwned<BoundExpression> left_promoted = ResolveTypePromotion(
      left.release(), left_type, allocator, row_capacity, promote);
  PROPAGATE_ON_FAILURE(left_promoted);
  FailureOrOwned<BoundExpression> middle_promoted = ResolveTypePromotion(
      middle.release(), middle_type, allocator, row_capacity, promote);
  PROPAGATE_ON_FAILURE(middle_promoted);
  FailureOrOwned<BoundExpression> right_promoted = ResolveTypePromotion(
      right.release(), right_type, allocator, row_capacity, promote);
  PROPAGATE_ON_FAILURE(right_promoted);
  return CreateAbstractBoundTernaryExpression<op, left_type, middle_type,
         right_type, to_type>(left_promoted.release(),
                              middle_promoted.release(),
                              right_promoted.release(),
                              allocator,
                              row_capacity);
}

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_TEMPLATED_BOUND_EXPRESSION_FACTORY_H_
