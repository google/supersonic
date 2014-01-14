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

#ifndef SUPERSONIC_EXPRESSION_CORE_REGEXP_EVALUATORS_H_
#define SUPERSONIC_EXPRESSION_CORE_REGEXP_EVALUATORS_H_

#include <stddef.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <cstring>
#include <string>
namespace supersonic {using std::string; }
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/utils/strings/stringpiece.h"
#include <re2/re2.h>

namespace supersonic {
namespace operators {

struct RegexpFull {
  bool operator()(const RE2& pattern, const StringPiece &str) {
    // TODO(tkaftal): Try to avoid memory copying during the .ToString() calls.
    return RE2::FullMatch(str.ToString(), pattern);
  }
};

struct RegexpPartial {
  bool operator()(const RE2& pattern, const StringPiece &str) {
    // TODO(tkaftal): Try to avoid memory copying during the .ToString() calls.
    return RE2::PartialMatch(str.ToString(), pattern);
  }
};

struct RegexpReplace {
  // We want to use a buffer string, but not necessarily to allocate it with
  // each call to the operator - we will thus allocate it in the Evaluate
  // function, and then pass it as an argument.
  StringPiece operator()(const StringPiece& haystack,
                         const RE2& pattern,
                         const StringPiece& substitute,
                         string& buffer,  // NOLINT
                         Arena* arena) {
    buffer = haystack.as_string();
    // TODO(tkaftal): Try to avoid memory copying during the .ToString() calls.
    RE2::GlobalReplace(&buffer, pattern, substitute.ToString());
    char* new_str =
        static_cast<char*>(arena->AllocateBytes(buffer.length() + 1));
    strncpy(new_str, buffer.c_str(), buffer.length());
    return StringPiece(new_str, buffer.length());
  }
};

}  // end namespace operators.
}  // end namespace supersonic.

#endif  // SUPERSONIC_EXPRESSION_CORE_REGEXP_EVALUATORS_H_
