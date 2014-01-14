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
// Definitions of non-standard expressions, which are implemented through
// templates on the column computer level.
//
// Do not add new evaluators here, unless you have a very good reason (in
// general, evaluators belong in the same directory as the corresponding
// expressions).

#ifndef SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_EVALUATORS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_EVALUATORS_H_

#include <math.h>
#include <cstdio>
#include <ctime>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/strings/stringpiece.h"

#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/expression/proto/operators.pb.h"

namespace supersonic {
namespace operators {

template<DataType input_type>
struct TypedToString {
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;
  TypedToString() {}
  StringPiece operator()(InputCppType input, Arena* arena) {
    // TODO(onufry): modify this code not to do unnecessary copying when
    // the default printers are modified to support writing directly to the
    // arena.
    result_.clear();
    PrintTyped<input_type>(input, &result_);
    size_t length = result_.length();
    char* new_str = static_cast<char*>(arena->AllocateBytes(length));
    strncpy(new_str, result_.data(), length);
    return StringPiece(new_str, length);
  }

 private:
  string result_;
};

template<DataType output_type>
struct TypedParseString {
  typedef typename TypeTraits<output_type>::cpp_type OutputCppType;
  explicit TypedParseString(const Attribute& attribute) {}

  void operator()(StringPiece input, OutputCppType* output, bool_ptr failure) {
    // We're making a copy of the input string here for the very stupid
    // reason that the parser expects a NULL-terminated string...
    // TODO(onufry): This should be changed.
    *failure = !ParseTyped<output_type>(input.as_string().c_str(), output);
  }
};

// Specialization for enums, to parse enum names stored in the attribute.
template<> struct TypedParseString<ENUM> {
  typedef typename TypeTraits<ENUM>::cpp_type OutputCppType;
  explicit TypedParseString(const Attribute& attribute)
      : enum_definition(attribute.enum_definition()) {}

  void operator()(StringPiece input, OutputCppType* output, bool_ptr failure) {
    FailureOr<int32> result = enum_definition.NameToNumber(input);
    if (result.is_success()) {
      *output = result.get();
    } else {
      *failure = result.is_failure();
    }
  }
  EnumDefinition enum_definition;
};

template<> struct TypedParseString<BINARY> {};
template<> struct TypedParseString<STRING> {};

}  // end namespace operators.
}  // end namespace supersonic.

#endif  // SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_EVALUATORS_H_
