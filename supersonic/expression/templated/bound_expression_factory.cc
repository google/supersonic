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

#include "supersonic/expression/templated/bound_expression_factory.h"

#include <map>
using std::map;
#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/templated/cast_bound_expression.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

// If promote is set to false, will Fail unless child_ptr already is of type
// result_type (in which case it returns child_ptr).
// If promote is set to true, it will try to perform an implicit cast (by the
// rules allowed in cast_bound_expression.h).
// Assumes ownership, releases in case of success.
class BufferAllocator;

FailureOrOwned<BoundExpression> ResolveTypePromotion(BoundExpression* child_ptr,
                                                     DataType result_type,
                                                     BufferAllocator* allocator,
                                                     rowcount_t row_capacity,
                                                     bool promote) {
  std::unique_ptr<BoundExpression> child(child_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount("Implicit cast attempt",
                                           child->result_schema(), 1));
  if (GetExpressionType(child.get()) == result_type)
    return Success(child.release());
  if (!promote) THROW(new Exception(
      ERROR_ATTRIBUTE_TYPE_MISMATCH,
      StringPrintf("Implicit cast attempt from %s to %s of %s failed.",
                   GetTypeInfo(GetExpressionType(child.get())).name().c_str(),
                   GetTypeInfo(result_type).name().c_str(),
                   GetExpressionName(child.get()).c_str())));
  FailureOrOwned<BoundExpression> cast_child = BoundInternalCast(
      allocator, row_capacity, child.release(), result_type, true);
  PROPAGATE_ON_FAILURE(cast_child);
  return Success(cast_child.release());
}

// Encapsulation of type mapping.
class CommonTypeCalculator {
 public:
  CommonTypeCalculator() {
    output_type_mapping_[std::make_pair(DOUBLE, INT32)] = DOUBLE;
    output_type_mapping_[std::make_pair(DOUBLE, INT64)] = DOUBLE;
    output_type_mapping_[std::make_pair(DOUBLE, UINT32)] = DOUBLE;
    output_type_mapping_[std::make_pair(DOUBLE, UINT64)] = DOUBLE;
    output_type_mapping_[std::make_pair(DOUBLE, FLOAT)] = DOUBLE;

    output_type_mapping_[std::make_pair(FLOAT, INT32)] = FLOAT;
    output_type_mapping_[std::make_pair(FLOAT, UINT32)] = FLOAT;
    output_type_mapping_[std::make_pair(FLOAT, UINT64)] = DOUBLE;
    output_type_mapping_[std::make_pair(FLOAT, INT64)] = DOUBLE;

    output_type_mapping_[std::make_pair(INT64, INT32)] = INT64;
    output_type_mapping_[std::make_pair(INT64, UINT32)] = INT64;
    output_type_mapping_[std::make_pair(INT64, UINT64)] = INT64;

    output_type_mapping_[std::make_pair(UINT64, INT32)] = INT64;
    output_type_mapping_[std::make_pair(UINT64, UINT32)] = UINT64;

    output_type_mapping_[std::make_pair(UINT32, INT32)] = INT64;

    output_type_mapping_[std::make_pair(DATE, DATETIME)] = DATETIME;
  }

  FailureOr<DataType> CalculateCommonType(DataType t1, DataType t2) {
    if (t1 == t2) return Success(t1);
    std::map<pair<DataType, DataType>, DataType>::iterator result;
    result = output_type_mapping_.find(std::make_pair(t1, t2));
    if (result != output_type_mapping_.end()) return Success(result->second);
    result = output_type_mapping_.find(std::make_pair(t2, t1));
    if (result != output_type_mapping_.end()) return Success(result->second);
    THROW(new Exception(ERROR_ATTRIBUTE_TYPE_MISMATCH,
                        StrCat("Cannot reconcile types: ",
                               GetTypeInfo(t1).name(), " and ",
                               GetTypeInfo(t2).name(), ".")));
  }
 private:
  std::map<pair<DataType, DataType>, DataType> output_type_mapping_;
};

// TODO(onufry): Likely all instances of this could be replaced with the
// Expression version, check and replace.
FailureOr<DataType> CalculateCommonType(DataType t1, DataType t2) {
  static CommonTypeCalculator type_calculator;
  return type_calculator.CalculateCommonType(t1, t2);
}

FailureOr<DataType> CalculateCommonExpressionType(BoundExpression* left,
                                               BoundExpression* right) {
  DataType left_type = GetExpressionType(left);
  DataType right_type = GetExpressionType(right);
  FailureOr<DataType> common_type = CalculateCommonType(left_type, right_type);
  PROPAGATE_ON_FAILURE(common_type);
  return common_type;
}

FailureOrOwned<BoundExpression> RunUnaryFactory(
    UnaryExpressionFactory* factory_ptr,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child_ptr,
    const string& operation_name) {
  std::unique_ptr<BoundExpression> child(child_ptr);
  // Factory creation functions signalize a type mismatch with a NULL
  // return pointer.
  // TODO(onufry): factory functions should return a FailureOr<Factory> instead
  // of signalizing error with NULLs.
  if (factory_ptr == NULL) {
    LOG(WARNING) << "Binding failed due to lack of factory.";
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Factory creation of operation ", operation_name,
               " failed due to type mismatch.")));
  }
  std::unique_ptr<UnaryExpressionFactory> factory(factory_ptr);
  return factory->create_expression(allocator, row_capacity, child.release());
}

FailureOrOwned<BoundExpression> RunBinaryFactory(
    BinaryExpressionFactory* factory_ptr,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    const string& operation_name) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  if (factory_ptr == NULL) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Factory creation of operation ", operation_name,
               " failed due to type mismatch.")));
  }
  std::unique_ptr<BinaryExpressionFactory> factory(factory_ptr);
  return factory->create_expression(allocator, row_capacity, left.release(),
                                    right.release());
}

}  // namespace supersonic
