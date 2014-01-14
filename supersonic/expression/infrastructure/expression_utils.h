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
//
// This file encompasses various helper classes and functions that should be
// available to all or most expressions, but should not be exposed in the
// public API.

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_EXPRESSION_UTILS_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_EXPRESSION_UTILS_H_

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BoundExpression;
class BoundExpressionList;

// Convenience shortcut for capturing and reporting wrong attribute count during
// bind.
// The operation name is the name of the operation which ordered the check
// (most likely the father of the operation the schema of which we check), we
// use it to create the appropriate error message.
FailureOrVoid CheckAttributeCount(const string& operation_name,
                                  const TupleSchema& schema,
                                  int expected_attribute_count);

// Convenience function, checks whether expression outputs expected type.
// Returns a Failure if not, a Success if yes. Assumes expression result has
// only one column (DCHECKs for it).
FailureOrVoid CheckExpressionType(DataType expected,
                                  const BoundExpression* expression);

// Convenience function, checks whether all members of an expression list
// output expected type. Returns a Failure if not, a Success if yes. Assumes
// each expression in the list has only one column (DCHECKs for it).
FailureOrVoid CheckExpressionListMembersType(
    DataType expected, const BoundExpressionList* expression_list);

// Convenience function. Returns the type of the expression. Assumes (and
// DCHECKS) that the expression has a single-column result schema.
DataType GetExpressionType(const BoundExpression* expression);

// This convenience function checks whether the input expression has a
// single-attribute result schema, and if the result type is appropriate.
// If yes, it wraps the expression in a success, otherwise returns a Failure.
// This is used mainly in creating cast-like expressions, that want to pass
// along the argument if the input type is equal to the output type.
FailureOrOwned<BoundExpression> CheckTypeAndPassAlong(
    BoundExpression* expression,
    DataType type);

// Convenience function. Returns the nullability of the expression. Assumes
// (and DCHECK) that the expression has a single-column result schema.
Nullability GetExpressionNullability(const BoundExpression* expression);

// Convenience function. Returns the name of the expressions. Assumes and
// DCHECKS that the expression has a single-column result schema.
const string& GetExpressionName(const BoundExpression* expression);

// Convenience function. Returns the name of the expressions. If
// the expression has multiple columns, they are delimited by ", ".
string GetMultiExpressionName(const BoundExpression* expression);

// Convenience function. Returns a TupleSchema, taking the output name and
// type, and the childen (one, two or three of them), and an optional
// nullability argument (the default is false). The children and the optional
// nullability are used to determine the nullability of the output expression
// (assuming that the nullability is determined by an OR of all the input
// nullabilities).
TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         bool nullable);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* arg);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* arg,
                         bool nullable);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* right);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* right,
                         bool nullable);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* middle,
                         BoundExpression* right);

TupleSchema CreateSchema(const string& output_name,
                         const DataType output_type,
                         BoundExpression* left,
                         BoundExpression* middle,
                         BoundExpression* right,
                         bool nullable);

// Checks whether the selectivity of the skip_vector (that is - the percentage
// of entries that are set to false) is greater than the given threshold.
bool SelectivityIsGreaterThan(bool_const_ptr skip_vector,
                              const rowcount_t row_count,
                              const int selectivity_threshold);

// Returns NULLABLE if any entry of the bound_expression_list is NULLABLE.
Nullability GetExpressionListNullability(
    const BoundExpressionList* bound_expression_list);

// Returns NULLABLE if either of the operands is(are) NULLABLE.
Nullability NullabilityOr(Nullability operand1, Nullability operand2);

// Finds a name not present in the schema that begins with the supplied prefix.
// This works by finding the least non-negative integer X such that if X
// is appended to the end of prefix, there is no attribute in schema with that
// name.
string GetNameNotPresentInSchema(const TupleSchema& schema, string prefix);

// Finds a name not present in the schema and in the set of strings given that
// begins with the supplied prefix. This works by finding the least non-negative
// integer X such that if X is appended to the end of prefix, there is no
// attribute in schema with that name.
string CreateUniqueName(const TupleSchema& schema,
                        const set<string>& additional_forbidden_name,
                        string prefix);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_EXPRESSION_UTILS_H_
