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
//
// Terminal expressions - constants, Null and Sequence.

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_EXPRESSIONS_H_

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/types.h"
// Needed for the ConstExpression class. This in turn is needed here to
// define the templated TypedConst.
#include "supersonic/expression/infrastructure/elementary_const_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

class RandomBase;

namespace supersonic {

// A typed null.
class Expression;

const Expression* Null(DataType type);

// Create typed constants.
const Expression* ConstInt32(const int32& value);
const Expression* ConstInt64(const int64& value);
const Expression* ConstUint32(const uint32& value);
const Expression* ConstUint64(const uint64& value);
const Expression* ConstFloat(const float& value);
const Expression* ConstDouble(const double& value);
const Expression* ConstBool(const bool& value);
const Expression* ConstDate(const int32& value);
const Expression* ConstDateTime(const int64& value);
const Expression* ConstString(const StringPiece& value);
const Expression* ConstBinary(const StringPiece& value);
const Expression* ConstDataType(const DataType& value);

// A templated version of creating typed constants.
template<DataType type>
const Expression* TypedConst(const typename TypeTraits<type>::cpp_type& value) {
  return new ConstExpression<type>(value);
}

// Creates an expression of type INT64 that will produce the sequence of
// consecutive integers when evaluated, starting at 0.
const Expression* Sequence();

// Create an expression of type INT32 that will produce a sequence of
// (pseudo-)random numbers produced by given random generator. Takes ownership
// of random_generator. The random generator class should support the Clone()
// method (the method shouldn't return NULL). Each produced bound expression
// will use the same starting state for the RNG (because of Clone()).
// Example: RandInt32(new MTRandom(seed))
// TODO(user): Generalize to other datatypes.
const Expression* RandInt32(RandomBase* random_generator);
// The default random number generator, using the MTRandom() generator.
const Expression* RandInt32();

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_TERMINAL_EXPRESSIONS_H_
