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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// Abstract templates for standard operation expressions.

#ifndef SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_EXPRESSIONS_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"

#include "supersonic/expression/templated/abstract_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/base/exception/result.h"

#include "supersonic/expression/infrastructure/expression_utils.h"

namespace supersonic {

template<OperatorId op>
class AbstractUnaryExpression : public UnaryExpression {
 public:
  explicit AbstractUnaryExpression(const Expression* const argument)
      : UnaryExpression(argument) {}

  virtual string ToString(bool verbose) const {
    return UnaryExpressionTraits<op>::FormatDescription(
        child_expression_->ToString(verbose));
  }

  DISALLOW_COPY_AND_ASSIGN(AbstractUnaryExpression);
};

// Class for elementary unary expressions. Does not support additional
// memory allocations.
// TODO(onufry) - write separate abstracts that do allocate memory.
//
// Usage - to define a new unary operation, you need to:
//   - add the actual struct to perform the operation. This usually goes into
//     expression_evaluators.h. If your function could possibly fail, you also
//     need a is_invalid checker function.
//   - add the appropriate field to protocol (proto/expression.proto).
//   - add the appropriate field to protocol (internal/internal.proto).
//   - add an expression trait (UnaryExpressionTraits specialization) in
//     expression_traits.h, refer to the functions defined above.
//   - add a declaration of a creator of your expression in the appropriate
//     file (probably something like public/foobar_expressions.h) and the
//     definition (which will simply invoke the creator of
//     AbstractUnaryExpression) in foobar_expressions.cc.
//   - you may also want to typedef the specialization (so instead of writing
//     AbstractUnaryExpression<FOO, BAR> you will write FooExpression) at the
//     end of this file.
//   - add a case to the giant switch in build_expression_from_proto.cc.
// And you're done. The column evaluation and expression binding happens for
// free :)
//
// The types are self-explanatory - you should give the expected input
// datatypes. For expressions where promotion is possible, you should give
// the widest type you expect to accept (usually either DOUBLE or INT64) and
// a promotion will be performed.
//
// A sample of usage may be found for FromUnixTime, in date_expressions.cc/.h
template<OperatorId op, DataType from_type, DataType to_type>
class TypedUnaryExpression : public AbstractUnaryExpression<op> {
 public:
  explicit TypedUnaryExpression(const Expression* const argument) :
    AbstractUnaryExpression<op>(argument) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const {
    return CreateTypedBoundUnaryExpression<op, from_type, to_type>(
        allocator, row_capacity, child);
  }

  DISALLOW_COPY_AND_ASSIGN(TypedUnaryExpression);
};

// A version of the above (with usage as above), where the input type is
// numeric, and the output type is to be the signed version of the input type
// (note that at the types are determined at binding time (so, runtime), they
// are not template parameters). Is used in the NEGATE expression.
template<OperatorId op>
class SignedUnaryExpression : public AbstractUnaryExpression<op> {
 public:
  explicit SignedUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<op>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    return CreateUnarySignedNumericExpression<op>(allocator, row_capacity,
                                                  child);
  }

  DISALLOW_COPY_AND_ASSIGN(SignedUnaryExpression);
};

// Another variant. Here the input is integer, and the output - boolean.
// Applied in, e.g., the IS_ODD expression.
template<OperatorId op>
class IntegerCheckUnaryExpression : public AbstractUnaryExpression<op> {
 public:
  explicit IntegerCheckUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<op>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    return CreateUnaryIntegerInputExpression<op, BOOL>(allocator,
                                                       row_capacity, child);
  }

  DISALLOW_COPY_AND_ASSIGN(IntegerCheckUnaryExpression);
};

// Another variant. The input is numeric, the output given by the template
// parameter.
template<OperatorId op, DataType to_type>
class NumericToTemplateUnaryExpression : public AbstractUnaryExpression<op> {
 public:
  explicit NumericToTemplateUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<op>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    return CreateUnaryNumericInputExpression<op, to_type>(allocator,
                                                          row_capacity, child);
  }

  DISALLOW_COPY_AND_ASSIGN(NumericToTemplateUnaryExpression);
};

// Another variant. Here the input is arbitrary, the output - STRING. Applied
// by TOSTRING (and only by TOSTRING).
class ToStringUnaryExpression
    : public AbstractUnaryExpression<OPERATOR_TOSTRING> {
 public:
  explicit ToStringUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<OPERATOR_TOSTRING>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    if (child->result_schema().attribute(0).type() == STRING)
      return Success(child);
    return CreateUnaryArbitraryInputExpression<OPERATOR_TOSTRING, STRING>(
        allocator, row_capacity, child);
  }

  DISALLOW_COPY_AND_ASSIGN(ToStringUnaryExpression);
};

// Another variant. If the input is integer, the expression is passed along,
// for floating point types the output is equal to the input. Used by ROUND.
template<OperatorId op>
class RoundUnaryExpression : public AbstractUnaryExpression<op> {
 public:
  explicit RoundUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<op>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    if (GetTypeInfo(child->result_schema().attribute(0).type()).is_integer())
      return Success(child);
    return CreateUnaryFloatingExpression<op>(allocator, row_capacity, child);
  }
};

// Another variant. If the input is integer, the expression is passed along,
// for floating point types the output is the template argument. Used by
// ROUND_TO_INT.
template<OperatorId op, DataType to_type>
class RoundToIntUnaryExpression
    : public AbstractUnaryExpression<op> {
 public:
  explicit RoundToIntUnaryExpression(const Expression* const child)
      : AbstractUnaryExpression<op>(child) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema, BufferAllocator* const allocator,
      rowcount_t row_capacity, BoundExpression* child) const {
    if (GetTypeInfo(child->result_schema().attribute(0).type()).is_integer())
      return Success(child);
    return CreateUnaryFloatingInputExpression<op, to_type>(allocator,
                                                           row_capacity, child);
  }

  DISALLOW_COPY_AND_ASSIGN(RoundToIntUnaryExpression);
};

// ---------------------- Binary Abstract Expression ------------

template<OperatorId op>
class AbstractBinaryExpression : public BinaryExpression {
 public:
  AbstractBinaryExpression(const Expression* const left,
                           const Expression* const right)
      : BinaryExpression(left, right) {}

  virtual string ToString(bool verbose) const {
    return BinaryExpressionTraits<op>::FormatDescription(
        left_->ToString(verbose), right_->ToString(verbose));
  }

  DISALLOW_COPY_AND_ASSIGN(AbstractBinaryExpression);
};

// An abstract binary expression class. Usage similar to the unary case,
// for samples see the binary substring operator (in string_expressions).
template<OperatorId op, DataType left_type, DataType right_type,
         DataType to_type>
class TypedBinaryExpression : public AbstractBinaryExpression<op> {
 public:
  TypedBinaryExpression(const Expression* const left,
                        const Expression* const right) :
    AbstractBinaryExpression<op>(left, right) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    return CreateTypedBoundBinaryExpression<op, left_type, right_type,
        to_type>(allocator, row_capacity, left, right);
  }

  DISALLOW_COPY_AND_ASSIGN(TypedBinaryExpression);
};

// A variant of the above where the inputs are to be of the same type (or can
// be cast to the same type) and the ouput will again be that type.
// Note that as the types are determined during binding (thus, runtime), they
// are not template parameters.
template<OperatorId op>
class EqualTypesBinaryExpression : public AbstractBinaryExpression<op> {
 public:
  EqualTypesBinaryExpression(const Expression* const left,
                             const Expression* const right) :
    AbstractBinaryExpression<op>(left, right) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    return CreateBinaryEqualTypesExpression<op>(allocator, row_capacity,
                                                left, right);
  }

  DISALLOW_COPY_AND_ASSIGN(EqualTypesBinaryExpression);
};

// Another variation. Here the inputs are to be numeric, and the output will be
// of the smallest containing type (in case of INT64 and UINT64 the default is
// INT64).
template<OperatorId op>
class NumericBinaryExpression : public AbstractBinaryExpression<op> {
 public:
  NumericBinaryExpression(const Expression* const left,
                          const Expression* const right) :
    AbstractBinaryExpression<op>(left, right) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    return CreateBinaryNumericExpression<op>(allocator, row_capacity,
                                             left, right);
  }

  DISALLOW_COPY_AND_ASSIGN(NumericBinaryExpression);
};

// Another variation. The inputs are integers, the output will be the smallest
// common containing type. Example: MODULUS.
template<OperatorId op>
class IntegerBinaryExpression : public AbstractBinaryExpression<op> {
 public:
  IntegerBinaryExpression(const Expression* const left,
                          const Expression* const right) :
    AbstractBinaryExpression<op>(left, right) {}

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    return CreateBinaryIntegerExpression<op>(allocator, row_capacity,
                                             left, right);
  }

  DISALLOW_COPY_AND_ASSIGN(IntegerBinaryExpression);
};

// ---------------------- Ternary Abstract Expression ------------
// An abstract ternary expression class. Usage similar to the unary case,
// for samples see the makedate operator (in date_expressions).
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType to_type>
class TypedTernaryExpression : public TernaryExpression {
 public:
  TypedTernaryExpression(const Expression* const left,
                         const Expression* const middle,
                         const Expression* const right)
    : TernaryExpression(left, middle, right) {}

  virtual string ToString(bool verbose) const {
    return TernaryExpressionTraits<op>::FormatDescription(
        left_->ToString(verbose),
        middle_->ToString(verbose),
        right_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundTernaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* middle,
      BoundExpression* right) const;

  DISALLOW_COPY_AND_ASSIGN(TypedTernaryExpression);
};

// Standard bound expression creator. Assumes children are bound. Assumes
// ownership.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType to_type>
FailureOrOwned<BoundExpression> TypedTernaryExpression<op, left_type,
    middle_type, right_type, to_type>::
    CreateBoundTernaryExpression(const TupleSchema& input_schema,
                                 BufferAllocator* const allocator,
                                 rowcount_t row_capacity,
                                 BoundExpression* left_ptr,
                                 BoundExpression* middle_ptr,
                                 BoundExpression* right_ptr) const {
  return CreateTypedBoundTernaryExpression<op, left_type, middle_type,
      right_type, to_type>(allocator, row_capacity,
                           left_ptr, middle_ptr, right_ptr);
}

}  // end namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_EXPRESSIONS_H_
