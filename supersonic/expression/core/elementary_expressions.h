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
// Elementary expressions: logical, relations, control, casts.

#ifndef SUPERSONIC_EXPRESSION_CORE_ELEMENTARY_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_ELEMENTARY_EXPRESSIONS_H_



#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Creates cast between types.
class Expression;
class ExpressionList;

const Expression* CastTo(DataType to_type, const Expression* const source);

// Parses a string to the specified data type.
// The quiet version returns garbage on invalid output, the nulling version
// gives NULL. If you are sure of your input, use the quiet version, it is
// faster.
// Whitespace at either end of the string to be parsed is accepted.
// DATETIME is parsed as in ConstDateTime().
const Expression* ParseStringQuiet(DataType to_type,
                                   const Expression* const source);

const Expression* ParseStringNulling(DataType to_type,
                                     const Expression* const source);

// ----------------------------------------------------------------------------
// Logical operators.

// The ternary if operator (equivalent to C++ "?:").
// Types of 'then' and 'otherwise' must be compatible (subject to standard
// promotions).
const Expression* If(const Expression* const condition,
                     const Expression* const then,
                     const Expression* const otherwise);

// The difference between the nulling and the standard if operator is in the
// treatment of NULL condition fields. The standard If treats them as false
// (this mimics the behaviour of MySql). The NullingIf returns a NULL for
// such fields (which is more consistent with general Supersonic standards.
const Expression* NullingIf(const Expression* const condition,
                            const Expression* const then,
                            const Expression* const otherwise);

// Creates an expression that will compute (a AND b) in ternary logic.
const Expression* And(const Expression* const a,
                      const Expression* const b);

// Creates an expression that will compute (!a AND b) in ternary logic.
const Expression* AndNot(const Expression* const left,
                         const Expression* const right);

// Creates an expression that will compute (a OR b) in ternary logic.
const Expression* Or(const Expression* const a,
                     const Expression* const b);

// Creates an expression that will compute (a XOR b) in ternary logic.
const Expression* Xor(const Expression* const a,
                      const Expression* const b);

// Creates an expression that will compute NOT(e) in ternary logic.
const Expression* Not(const Expression* const e);

// Creates a bool expression that will compute IS_NULL(e). The result is always
// non-NULL (i.e. either true or false).
const Expression* IsNull(const Expression* const e);

// Creates a bool expression that will compute NVL(e, substitute), which is:
// Returns e if it is not null; otherwise returns the substitute.
const Expression* IfNull(const Expression* const e,
                         const Expression* const substitute);

// CASE arg0 WHEN arg2 THEN arg3 WHEN arg4 THEN arg5 [...] ELSE arg1.
// Takes ownership of arguments.
const Expression* Case(const ExpressionList* const arguments);

// ----------------------------------------------------------------------------
// Bitwise operators.

// Creates an expression that will compute the bitwise not (the reversal of all
// the bits) of the input number. The input has to be integer, the output is of
// the same type as the input.
const Expression* BitwiseNot(const Expression* argument);

// The bitwise logical operators: And, AndNot (~a && b), Or and Xor. The inputs
// have to be both integer. The output is always the smallest common containing
// type.
const Expression* BitwiseAnd(const Expression* a,
                             const Expression* b);
const Expression* BitwiseAndNot(const Expression* a,
                                const Expression* b);
const Expression* BitwiseOr(const Expression* a,
                            const Expression* b);
const Expression* BitwiseXor(const Expression* a,
                             const Expression* b);

// Bitwise shifts. Both arguments have to be integers, the output type is the
// same as the type of the left hand side.
const Expression* ShiftLeft(const Expression* argument,
                            const Expression* shift);
const Expression* ShiftRight(const Expression* argument,
                             const Expression* shift);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_ELEMENTARY_EXPRESSIONS_H_
