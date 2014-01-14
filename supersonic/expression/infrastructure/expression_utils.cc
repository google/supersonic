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

#include "supersonic/expression/infrastructure/expression_utils.h"

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

DataType GetExpressionType(const BoundExpression* expression) {
  DCHECK_EQ(1, expression->result_schema().attribute_count());
  return expression->result_schema().attribute(0).type();
}

const string& GetExpressionName(const BoundExpression* expression) {
  DCHECK_EQ(1, expression->result_schema().attribute_count());
  return expression->result_schema().attribute(0).name();
}

Nullability GetExpressionNullability(const BoundExpression* expression) {
  DCHECK_EQ(1, expression->result_schema().attribute_count());
  return expression->result_schema().attribute(0).nullability();
}

string GetMultiExpressionName(const BoundExpression* expression) {
  string result_description;
  const TupleSchema& schema = expression->result_schema();
  for (int i = 0; i < schema.attribute_count(); ++i) {
    if (i != 0) result_description.append(", ");
    result_description.append(schema.attribute(i).name());
  }
  return result_description;
}

FailureOrVoid CheckExpressionType(DataType expected,
                                  const BoundExpression* expression) {
  DCHECK(CheckAttributeCount("Expression type checking input",
                             expression->result_schema(),
                             1).is_success());
  if (GetExpressionType(expression) != expected) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Wrong type of argument supplied. Expected: ",
               GetTypeInfo(expected).name(), "; is: ",
               expression->result_schema().GetHumanReadableSpecification())));
  }
  return Success();
}

FailureOrVoid CheckExpressionListMembersType(
    DataType expected, const BoundExpressionList* expression_list) {
  for (int i = 0; i < expression_list->size(); ++i) {
    string name = StringPrintf("%d-th expression in an expression list", i);
    DCHECK(CheckAttributeCount(name,
                               expression_list->get(i)->result_schema(),
                               1).is_success());
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        CheckExpressionType(expected, expression_list->get(i)),
        StringPrintf("Wrong type of the %d-th argument", i),
        expression_list->get(i)->result_schema()
            .GetHumanReadableSpecification());
  }
  return Success();
}

// TODO(onufry): rethink this, probably change the operation_name parameter
// to "details", also add it to CheckExpression type. In any case, provide a
// consistent behaviour.
FailureOrVoid CheckAttributeCount(const string& operation_name,
                                  const TupleSchema& schema,
                                  int expected_attribute_count) {
  if (schema.attribute_count() != expected_attribute_count) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_COUNT_MISMATCH,
        StringPrintf("Failed to bind: %s expects %d attribute(s), got: %d "
                     "(%s)",
                     operation_name.c_str(), expected_attribute_count,
                     schema.attribute_count(),
                     schema.GetHumanReadableSpecification().c_str())));
  } else {
    return Success();
  }
}

FailureOrOwned<BoundExpression> CheckTypeAndPassAlong(
    BoundExpression* expression,
    DataType type) {
  FailureOrVoid check =
      CheckAttributeCount("No-op", expression->result_schema(), 1);
  if (check.is_success()) {
    check = CheckExpressionType(type, expression);
  }  // That's not an "else" - check could have become false.
  if (check.is_failure()) {
    return Failure(check.release_exception());
  }
  return Success(expression);
}

bool ExpressionIsNullable(const BoundExpression* const arg) {
  DCHECK_EQ(1, arg->result_schema().attribute_count());
  return arg->result_schema().attribute(0).is_nullable();
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         bool nullable) {
  return TupleSchema::Singleton(output_name, output_type,
                                nullable ? NULLABLE : NOT_NULLABLE);
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* arg) {
  return CreateSchema(output_name, output_type, ExpressionIsNullable(arg));
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* arg,
                         bool nullable) {
  return CreateSchema(output_name, output_type,
                      ExpressionIsNullable(arg) || nullable);
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* right) {
  return CreateSchema(output_name, output_type,
                      ExpressionIsNullable(left) ||
                      ExpressionIsNullable(right));
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* right,
                         bool nullable) {
  return CreateSchema(output_name, output_type,
                      ExpressionIsNullable(left) ||
                      ExpressionIsNullable(right)
                                           || nullable);
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* middle,
                         BoundExpression* right) {
  return CreateSchema(output_name, output_type,
                      ExpressionIsNullable(left) ||
                      ExpressionIsNullable(middle) ||
                      ExpressionIsNullable(right));
}

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* middle,
                         BoundExpression* right,
                         bool nullable) {
  return CreateSchema(output_name, output_type,
                      ExpressionIsNullable(left) ||
                      ExpressionIsNullable(middle) ||
                      ExpressionIsNullable(right) ||
                      nullable);
}

bool SelectivityIsGreaterThan(bool_const_ptr skip_vector,
                              const rowcount_t row_count,
                              const int selectivity_threshold) {
  // This follows from the subsequent calculation, but we don't want to waste
  // our time calculating the PopCount for very quick operations.
  if (selectivity_threshold == 0) return false;
  return (bit_pointer::PopCount(skip_vector, row_count) * 100 >
          row_count * (100 - selectivity_threshold));
}

Nullability GetExpressionListNullability(
    const BoundExpressionList* bound_expression_list) {
  for (int i = 0; i < bound_expression_list->size(); ++i) {
    if (GetExpressionNullability(bound_expression_list->get(i)) == NULLABLE) {
      return NULLABLE;
    }
  }
  return NOT_NULLABLE;
}

Nullability NullabilityOr(Nullability operand1, Nullability operand2) {
  if (operand1 == NULLABLE || operand2 == NULLABLE) {
    return NULLABLE;
  } else {
    return NOT_NULLABLE;
  }
}

string GetNameNotPresentInSchema(const TupleSchema& schema, string prefix) {
  return CreateUniqueName(schema, set<string>(), prefix);
}

string CreateUniqueName(const TupleSchema& schema,
                        const set<string>& additional_forbidden_name,
                        string prefix) {
  string unique_name;
  for (int i = 0; ; ++i) {
    unique_name = StrCat(prefix, i);
    if (schema.LookupAttributePosition(unique_name) >= 0 ||
        additional_forbidden_name.find(unique_name) !=
            additional_forbidden_name.end()) {
      // It's found.
      continue;
    } else {
      break;
    }
  }
  return unique_name;
}

}  // namespace supersonic
