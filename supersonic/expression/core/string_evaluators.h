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
//
// Definitions of individual operators for expressions defined in
// string_expressions.h.

#ifndef SUPERSONIC_EXPRESSION_CORE_STRING_EVALUATORS_H_
#define SUPERSONIC_EXPRESSION_CORE_STRING_EVALUATORS_H_

#include <stddef.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <cstring>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/utils/strings/ascii_ctype.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/strings/util.h"

namespace supersonic {
namespace operators {

struct SubstringTernary {
  StringPiece operator()(const StringPiece& input,
                         int64 position,
                         int64 length) {
    length = std::max(length, 0LL);
    if (position > 0LL) {
      position -= 1LL;
    } else {
      position += input.length();
      if (position < 0LL) position = 0LL;
    }
    return input.substr(position, length);
  }
};

struct SubstringBinary {
  StringPiece operator()(const StringPiece& input,
                         int64 position) {
    if (position > 0LL) {
      position -= 1LL;
    } else {
      position += input.length();
      if (position < 0LL) position = 0LL;
    }
    return input.substr(position);
  }
};

struct StringOffset {
  int32 operator()(const StringPiece& haystack,
                   const StringPiece& needle) {
    return haystack.find(needle) + 1;
  }
};

struct StringReplaceEvaluator {
  StringPiece operator()(const StringPiece& haystack,
                         const StringPiece& needle,
                         const StringPiece& substitute,
                         Arena* arena) {
    // TODO(onufry): When the Arena truncating a string, we should be able to
    // avoid the extra copy here. Also, we may consider a trick such as we use
    // in Regexp functions (passing a string in), to avoid string allocation
    // with each call.
    string s = StringReplace(haystack, needle, substitute, true);
    char* new_str = static_cast<char*>(arena->AllocateBytes(s.length() + 1));
    strncpy(new_str, s.c_str(), s.length());
    return StringPiece(new_str, s.length());
  }
};

struct Length {
  int32 operator()(const StringPiece& str) { return str.length(); }
};

struct Ltrim {
  StringPiece operator()(StringPiece str) {
    while (str.length() && str[0] == ' ') { str.remove_prefix(1); }
    return str;
  }
};

struct Rtrim {
  StringPiece operator()(StringPiece str) {
    size_t new_length = str.length();
    while (new_length && str[new_length - 1] == ' ') { --new_length; }
    return str.substr(0, new_length);
  }
};

struct Trim {
  StringPiece operator()(StringPiece str) {
    size_t length = str.length();
    size_t index = 0;
    while (index < length && str[index] == ' ') { ++index; }
    length -= index;
    while (length && str[index + length - 1] == ' ') { --length; }
    return str.substr(index, length);
  }
};

struct ToUpper {
  StringPiece operator()(StringPiece str, Arena* arena) {
    size_t length = str.length();
    char* new_str = static_cast<char*>(arena->AllocateBytes(length));
    CHECK_NOTNULL(new_str);
    for (int i = 0; i < length; ++i) {
      new_str[i] = ascii_toupper(str[i]);
    }
    return StringPiece(new_str, length);
  }
};

struct ToLower {
  StringPiece operator()(StringPiece str, Arena* arena) {
    size_t length = str.length();
    char* new_str = static_cast<char*>(arena->AllocateBytes(length));
    CHECK_NOTNULL(new_str);
    for (int i = 0; i < length; ++i) {
      new_str[i] = ascii_tolower(str[i]);
    }
    return StringPiece(new_str, length);
  }
};

}  // end namespace operators.
}  // end namespace supersonic.

#endif  // SUPERSONIC_EXPRESSION_CORE_STRING_EVALUATORS_H_
