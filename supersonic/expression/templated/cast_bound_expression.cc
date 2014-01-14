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

#include "supersonic/expression/templated/cast_bound_expression.h"

#include <stddef.h>
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/abstract_bound_expressions.h"

namespace supersonic {

class BufferAllocator;

namespace {

// ---------------------------- Bound Cast classes -----------------------------

// A projecting cast is a cast that does not modify or copy the underlying data,
// just changes its internal interpretation (for instance STRING to BINARY casts
// can be performed in this manner).
// Does not allocate memory for the results.
template<DataType from_type, DataType to_type>
class BoundProjectingCastExpression : public BoundExpression {
 public:
  explicit BoundProjectingCastExpression(BoundExpression* child_ptr)
      : BoundExpression(CreateCastSchema(child_ptr)),
        child_(child_ptr) {}

  virtual ~BoundProjectingCastExpression() {}

  virtual rowcount_t row_capacity() const { return child_->row_capacity(); }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    EvaluationResult child_result = child_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(child_result);
    my_view()->set_row_count(child_result.get().row_count());
    // The child's skip vector should be, by contract, set to our skip vector.
    my_view()->mutable_column(0)->Reset(child_result.get().column(0).data(),
                                        child_result.get().column(0).is_null());
    return Success(my_view());
  }

  virtual bool is_constant() const { return child_->is_constant(); }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    child_->CollectReferredAttributeNames(referred_attribute_names);
  }

 private:
  const std::unique_ptr<BoundExpression> child_;

  static TupleSchema CreateCastSchema(BoundExpression* child) {
    string name = StringPrintf("CAST_%s_TO_%s(%s)",
                               GetTypeInfo(from_type).name().c_str(),
                               GetTypeInfo(to_type).name().c_str(),
                               GetExpressionName(child).c_str());
    return CreateSchema(name, to_type, child);
  }
};

// A creator for ProjectingCast expressions.
template<DataType from_type, DataType to_type>
FailureOrOwned<BoundExpression> BoundProjectingCast(BoundExpression* child) {
  std::unique_ptr<BoundExpression> child_ptr(child);
  FailureOrVoid check = CheckAttributeCount(string("Cast"),
                                            child_ptr->result_schema(),
                                            1);
  PROPAGATE_ON_FAILURE(check);
  return Success(new BoundProjectingCastExpression<from_type, to_type>(
      child_ptr.release()));
}

// A standard cast expression that allocates memory for the results.
template<DataType from_type, DataType to_type>
FailureOrOwned<BoundExpression> BoundCast(BoundExpression* child,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count) {
  return AbstractBoundUnary<OPERATOR_CAST_QUIET, from_type, to_type>(
      child, allocator, max_row_count);
}

// A down-cast expression. At the moment implemented as a standard cast, in the
// future to be equipped with range-checks.
template<DataType from_type, DataType to_type>
FailureOrOwned<BoundExpression> BoundDownCast(BoundExpression* child,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count) {
  // TODO(onufry): Add range-checks when downcasting.
  return AbstractBoundUnary<OPERATOR_CAST_QUIET, from_type, to_type>(
      child, allocator, max_row_count);
}

// A DateToDatetime cast, implemented as a separate operator. Multiplies the
// date by the number of microseconds in a day.
FailureOrOwned<BoundExpression> BoundDateToDatetime(BoundExpression* child,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count) {
  return AbstractBoundUnary<OPERATOR_DATE_TO_DATETIME, DATE, DATETIME>(
      child, allocator, max_row_count);
}

// ------------------------ Exception factories --------------------------------

// Exception factories are used to create an exception with the appropriate
// message (hopefully helpful to the user), which can be then thrown as a
// result of a binding failure of a cast due to attribute type mismatch.
Exception* IllicitCastToString(DataType from_type, const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to STRING in %s. Perhaps you meant to use "
                   "the ToString expression (to be found in "
                   "string_expressions.h)?",
                   GetTypeInfo(from_type).name().c_str(),
                   description.c_str()));
}

// A generic "illicit cast", where we have nothing intelligent to say. The
// default fallback behaviour (it always suggests ToString if the cast was to
// STRING).
Exception* IllicitCast(DataType from_type,
                       DataType to_type,
                       const string& description) {
  if (to_type == STRING) return IllicitCastToString(from_type, description);
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to %s in %s.",
                   GetTypeInfo(from_type).name().c_str(),
                   GetTypeInfo(to_type).name().c_str(),
                   description.c_str()));
}

Exception* IllicitCastNumericToDateType(DataType from_type,
                                        DataType to_type,
                                        const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to %s in %s. Perhaps you meant to use one of"
                   " the DATE or DATETIME creating functions in "
                   "date_expressions.h (like FromUnixTime)?",
                   GetTypeInfo(from_type).name().c_str(),
                   GetTypeInfo(to_type).name().c_str(),
                   description.c_str()));
}

Exception* IllicitCastNumericToBool(DataType from_type,
                                    const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to BOOL in %s. Perhaps you meant to use the "
                   "NumericToBool expression (to be found in "
                   "custom_expressions.h)?",
                   GetTypeInfo(from_type).name().c_str(),
                   description.c_str()));
}

// A switch for illicit casts from numeric types.
Exception* IllicitCastFromNumeric(DataType from_type,
                                 DataType to_type,
                                 const string& description) {
  switch (to_type) {
    case STRING: return IllicitCastToString(from_type, description);
    case BINARY: return IllicitCast(from_type, BINARY, description);
    case DATE: return IllicitCastNumericToDateType(
        from_type, DATE, description);
    case DATETIME: return IllicitCastNumericToDateType(
        from_type, DATETIME, description);
    case BOOL: return IllicitCastNumericToBool(from_type, description);
    case DATA_TYPE: return IllicitCast(from_type, DATA_TYPE, description);
    default: LOG(FATAL) << "Unexpected type encountered when trying to cast";
  }
  return NULL;  // Not going to be called.
}

Exception* IllicitCastFromString(DataType to_type, const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast STRING to %s in %s. Perhaps you meant to use "
                   "one of the ParseString expressions (to be found in "
                   "elementary_expressions.h)?",
                   GetTypeInfo(to_type).name().c_str(),
                   description.c_str()));
}

// This is not handled as a downcast, as we do not allow even explicit
// downcasts from floating-point numbers to integers.
Exception* IllicitFloatingToIntegerCast(DataType from_type,
                                        DataType to_type,
                                        const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to %s in %s. To perform a rounding, use one "
                   "of the RoundToInt functions from math_expressions.h.",
                   GetTypeInfo(from_type).name().c_str(),
                   GetTypeInfo(to_type).name().c_str(),
                   description.c_str()));
}

Exception* IllicitDownCast(DataType from_type,
                           DataType to_type,
                           const string& description) {
  return new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Cannot cast %s to %s in %s. Implicit downcasts are "
                   "disallowed in Supersonic, to obtain a downcast use an "
                   "explicit Cast expression (to be found in "
                   "elementary_expressions.h).",
                   GetTypeInfo(from_type).name().c_str(),
                   GetTypeInfo(to_type).name().c_str(),
                   description.c_str()));
}

Exception* IllicitCastFromDateType(DataType from_type,
                                   DataType to_type,
                                   const string& description) {
  if (GetTypeInfo(to_type).is_numeric()) {
    return new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StringPrintf("Cannot cast %s to %s in %s. Perhaps you meant to use "
                     "UnixTimeStamp, or one of the other date conversion "
                     "expressions from date_expressions.h?",
                     GetTypeInfo(from_type).name().c_str(),
                     GetTypeInfo(to_type).name().c_str(),
                     description.c_str()));
  }
  if (to_type == DATE) {
    return new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StringPrintf("Cannot cast %s to %s in %s. To downcast Date types, use"
                     " the RoundTo* functions from date_expressions.h.",
                     GetTypeInfo(from_type).name().c_str(),
                     GetTypeInfo(to_type).name().c_str(),
                     description.c_str()));
  }
  return IllicitCast(from_type, to_type, description);
}

Exception* IllicitCastFromBool(DataType to_type,
                               const string& description) {
  if (GetTypeInfo(to_type).is_numeric()) {
    return new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StringPrintf("Cannot cast BOOL to %s in %s. Perhaps you meant to use "
                     "the BoolToNumeric function (to be found in "
                     "custom_expressions.h)?",
                     GetTypeInfo(to_type).name().c_str(),
                     description.c_str()));
  }
  return IllicitCast(BOOL, to_type, description);
}

// ------------------ Bound Cast Creators for TypeSpecialization ---------------

// The template magic for TypeSpecialization (which assures a compile-time error
// if some datatype is not supported). This is a stateful functor, wrapping
// a function call to some cast binding function.
struct BoundCastCreator {
  BoundCastCreator(BufferAllocator* allocator,
                   rowcount_t row_capacity,
                   BoundExpression* child,
                   DataType to_type,
                   bool is_implicit,
                   const string& description)
      : allocator_(allocator),
        row_capacity_(row_capacity),
        child_ptr(child),
        to_type_(to_type),
        is_implicit_(is_implicit),
        description_(description) {}

  template<DataType from_type>
  FailureOrOwned<BoundExpression> operator()() const;

  BufferAllocator* allocator_;
  rowcount_t row_capacity_;
  BoundExpression* child_ptr;
  DataType to_type_;
  bool is_implicit_;
  string description_;
};


template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<INT32>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  switch (to_type_) {
    case INT32: return Success(child_.release());
    case UINT32: return BoundProjectingCast<INT32, UINT32>(child_.release());
    case INT64: return BoundCast<INT32, INT64>(child_.release(), allocator_,
                                               row_capacity_);
    case UINT64: return BoundCast<INT32, UINT64>(child_.release(), allocator_,
                                                 row_capacity_);
    case FLOAT: return BoundCast<INT32, FLOAT>(child_.release(), allocator_,
                                               row_capacity_);
    case DOUBLE: return BoundCast<INT32, DOUBLE>(child_.release(), allocator_,
                                                 row_capacity_);
    case STRING:
    case BINARY:
    case DATE:
    case DATETIME:
    case BOOL:
    case ENUM:
    case DATA_TYPE:
        THROW(IllicitCastFromNumeric(INT32, to_type_, description_));
    // No default.
  }
  return Success(child_.release());  // Will never be called.
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<UINT32>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  switch (to_type_) {
    case INT32: return BoundProjectingCast<UINT32, INT32>(child_.release());
    case UINT32: return Success(child_.release());
    case INT64: return BoundCast<UINT32, INT64>(child_.release(), allocator_,
                                                row_capacity_);
    case UINT64: return BoundCast<UINT32, UINT64>(child_.release(), allocator_,
                                                  row_capacity_);
    case FLOAT: return BoundCast<UINT32, FLOAT>(child_.release(), allocator_,
                                                row_capacity_);
    case DOUBLE: return BoundCast<UINT32, DOUBLE>(child_.release(), allocator_,
                                                  row_capacity_);
    case STRING:
    case BINARY:
    case DATE:
    case DATETIME:
    case BOOL:
    case ENUM:
    case DATA_TYPE:
        THROW(IllicitCastFromNumeric(UINT32, to_type_, description_));
    // No default.
  }
  return Success(child_.release());  // Will never be called.
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<INT64>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == INT64) return Success(child_.release());
  if (to_type_ == UINT64) return BoundProjectingCast<INT64, UINT64>(
      child_.release());
  if (to_type_ == INT32 || to_type_ == UINT32 || to_type_ == FLOAT) {
    if (is_implicit_) THROW(IllicitDownCast(INT64, to_type_, description_));
    // Explicit downcast, allowed.
    if (to_type_ == INT32) return BoundDownCast<INT64, INT32>(
        child_.release(), allocator_, row_capacity_);
    if (to_type_ == UINT32) return BoundDownCast<INT64, UINT32>(
        child_.release(), allocator_, row_capacity_);
    // The remaining case is FLOAT.
    return BoundDownCast<INT64, FLOAT>(child_.release(), allocator_,
                                       row_capacity_);
  }
  if (to_type_ == DOUBLE) return BoundCast<INT64, DOUBLE>(
      child_.release(), allocator_, row_capacity_);
  THROW(IllicitCastFromNumeric(INT64, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<UINT64>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == UINT64) return Success(child_.release());
  if (to_type_ == INT64) return BoundProjectingCast<UINT64, INT64>(
      child_.release());
  if (to_type_ == INT32 || to_type_ == UINT32 || to_type_ == FLOAT) {
    if (is_implicit_) THROW(IllicitDownCast(UINT64, to_type_, description_));
    // Explicit downcast, allowed.
    if (to_type_ == INT32) return BoundDownCast<UINT64, INT32>(
        child_.release(), allocator_, row_capacity_);
    if (to_type_ == UINT32) return BoundDownCast<UINT64, UINT32>(
        child_.release(), allocator_, row_capacity_);
    // The remaining case is FLOAT.
    return BoundDownCast<UINT64, FLOAT>(child_.release(), allocator_,
                                        row_capacity_);
  }
  if (to_type_ == FLOAT) return BoundCast<UINT64, FLOAT>(
      child_.release(), allocator_, row_capacity_);
  if (to_type_ == DOUBLE) return BoundCast<UINT64, DOUBLE>(
      child_.release(), allocator_, row_capacity_);
  THROW(IllicitCastFromNumeric(UINT64, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<FLOAT>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  // Casts to floating point types.
  if (to_type_ == FLOAT) return Success(child_.release());
  if (to_type_ == DOUBLE)
    return BoundCast<FLOAT, DOUBLE>(child_.release(), allocator_,
                                    row_capacity_);
  // Casts to integral types (forbidden).
  if (GetTypeInfo(to_type_).is_numeric())
    THROW(IllicitFloatingToIntegerCast(FLOAT, to_type_, description_));
  // Other casts.
  THROW(IllicitCastFromNumeric(FLOAT, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<DOUBLE>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  // Casts to floating point types.
  if (to_type_ == DOUBLE) return Success(child_.release());
  if (to_type_ == FLOAT) {
    if (is_implicit_) {
      THROW(IllicitDownCast(DOUBLE, FLOAT, description_));
    } else {
      return BoundDownCast<DOUBLE, FLOAT>(child_.release(), allocator_,
                                          row_capacity_);
    }
  }
  // Casts to integral types (forbidden).
  if (GetTypeInfo(to_type_).is_integer())
    THROW(IllicitFloatingToIntegerCast(DOUBLE, to_type_, description_));
  // Other casts.
  THROW(IllicitCastFromNumeric(DOUBLE, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<STRING>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == STRING) return Success(child_.release());
  if (to_type_ == BINARY) return BoundProjectingCast<STRING, BINARY>(
      child_.release());
  THROW(IllicitCastFromString(to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<BINARY>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == BINARY) return Success(child_.release());
  if (to_type_ == STRING) return BoundProjectingCast<BINARY, STRING>(
      child_.release());
  THROW(IllicitCast(BINARY, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<DATE>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == DATE) return Success(child_.release());
  if (to_type_ == DATETIME) return BoundDateToDatetime(
      child_.release(), allocator_, row_capacity_);
  THROW(IllicitCastFromDateType(DATE, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<DATETIME>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == DATETIME) return Success(child_.release());
  THROW(IllicitCastFromDateType(DATE, to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<BOOL>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == BOOL) return Success(child_.release());
  THROW(IllicitCastFromBool(to_type_, description_));
}

template<>
FailureOrOwned<BoundExpression> BoundCastCreator::operator()<ENUM>() const {
  LOG(FATAL) << "Cast on ENUM is not supported. Should have been caught "
             << "higher up";
}

template<>
FailureOrOwned<BoundExpression>
    BoundCastCreator::operator()<DATA_TYPE>() const {
  std::unique_ptr<BoundExpression> child_(child_ptr);
  if (to_type_ == DATA_TYPE) return Success(child_.release());
  THROW(IllicitCast(DATA_TYPE, to_type_, description_));
}

}  // namespace

// The cast implementation, using TypeSpecialization.
FailureOrOwned<BoundExpression> BoundInternalCast(
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr,
    DataType to_type,
    bool is_implicit) {
  std::unique_ptr<BoundExpression> child(child_ptr);
  FailureOrVoid check = CheckAttributeCount(string("Cast"),
                                            child->result_schema(),
                                            1);
  PROPAGATE_ON_FAILURE(check);
  string description = StringPrintf("CAST_TO_%s(%s)",
                                    GetTypeInfo(to_type).name().c_str(),
                                    GetExpressionName(child.get()).c_str());
  DataType from_type = GetExpressionType(child.get());
  BoundCastCreator creator(allocator, row_capacity, child.release(),
                           to_type, is_implicit, description);
  return TypeSpecialization<FailureOrOwned<BoundExpression>, BoundCastCreator>(
      from_type, creator);
}

}  // namespace supersonic
