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
// Expressions on strings.

#ifndef SUPERSONIC_EXPRESSION_CORE_REGEXP_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_REGEXP_EXPRESSIONS_H_

#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// Creates an expression that will convert any expression to VARCHAR.
class Expression;

// Performs partial regular expression matching, using RE2, on the specified
// string argument. Returns true if matched, false if not matched, NULL if
// the argument is NULL.
//
// Note: the argument order contravenes the standard SuperSonic order of
// "variable arguments at the end".
const Expression* RegexpPartialMatch(const Expression* str,
                                     const StringPiece& pattern);

// Performs full regular expression matching, using RE2, on the specified
// string argument. Returns true if matched, false if not matched, NULL if
// the argument is NULL.
//
// Note: the argument order contravenes the standard SuperSonic order of
// "variable arguments at the end".
const Expression* RegexpFullMatch(const Expression* str,
                                  const StringPiece& pattern);

// Replace all occurences of "needle" in "haystack" with "substitute".
// Needle can be a regular expression.
const Expression* RegexpReplace(const Expression* haystack,
                                const StringPiece& needle,
                                const Expression* substitute);

// Replace the first match of "pattern" in "str" with "rewrite". Within
// "rewrite", backslash-escaped digits (\1 to \9) can be used to insert text
// matching corresponding parenthesized group from the pattern.  \0 in
// "rewrite" refers to the entire matching text.
// If not matched, or if the argument is NULL, results in NULL.
//
// Currently not implemented.
const Expression* RegexpRewrite(const Expression* str,
                                const StringPiece& pattern,
                                const StringPiece& rewrite);

// Return the first substring of "str" matching "pattern". If "pattern" cannot
// be matched into substring, returns NULL.
const Expression* RegexpExtract(const Expression* str,
                                const StringPiece& pattern);

// Replace the first match of "pattern" in "str" with "rewrite". Within
// "rewrite", backslash-escaped digits (\1 to \9) can be used to insert text
// matching corresponding parenthesized group from the pattern.  \0 in
// "rewrite" refers to the entire matching text.
// If the argument is NULL, results in NULL. If the argument is not NULL but
// the pattern did not match, returns the default value.
//
// Currently not implemented.
const Expression* RegexpRewrite(const Expression* str,
                                const Expression* default_value,
                                const StringPiece& pattern,
                                const StringPiece& rewrite);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_REGEXP_EXPRESSIONS_H_
