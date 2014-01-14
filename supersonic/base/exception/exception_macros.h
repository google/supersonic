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
// Convenience macros for propagating exceptions with stack traces. Kept in
// a separate file to minimize namespace pollution.
//
// NOTE(user): The 'do { ... } while (false)' idiom is used to ensure proper
// behavior within 'if-else' statements.
// See http://stackoverflow.com/questions/154136.
//
// NOTE(user): Fully-qualified class names are used to let these macros be
// used outside the Supersonic namespace.
//
// NOTE(user): All macro parameters are wrapped in parentheses, so that
// it parses correctly in presence of comma operators etc. Also, care is taken
// to ensure that each macro parameter is evaluated at most once.

#ifndef SUPERSONIC_BASE_EXCEPTION_EXCEPTION_MACROS_H_
#define SUPERSONIC_BASE_EXCEPTION_EXCEPTION_MACROS_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/strings/join.h"

// Returns early from a function, with a Failure with the specified
// exception, with a single new stack frame appended, corresponding to the
// place in code (filename, line, function name) where the macro was used.
// The exception is evaluated exactly once.
#define THROW(exception)                                                       \
  do {                                                                         \
    return ::supersonic::Failure((exception)->            \
        AddStackTraceElement(__FUNCTION__, __FILE__, __LINE__,                 \
                             "(thrown here)"));                                \
  } while (false)

// No-op if the result was a success. Otherwise, acts like THROW with the
// exception propagated from the result. If the message_prefix is not empty,
// prepends it to the exception message, followed by ': '. For example,
// if the message had been 'Foo', and the message_prefix is 'Bar', the
// resulting message will be 'Bar: Foo'.
// The message_prefix and stack_frame_context are evaluated only in case of
// exception, and only once.
#define PROPAGATE_ON_FAILURE_WITH_CONTEXT(result,                              \
                                          message_prefix,                      \
                                          stack_frame_context)                 \
  do {                                                                         \
    ::supersonic::Exception* for_type_matching = NULL;    \
    ::supersonic::Exception* exception =                  \
        ::common::ConvertException(                              \
             for_type_matching, (result).release_exception());                 \
    if (exception != NULL) {                                                   \
      string prefix(message_prefix);                                           \
      if (!prefix.empty()) {                                                   \
        exception->set_message(StrCat(prefix, ": ", exception->message()));    \
      }                                                                        \
      return ::supersonic::Failure((exception)->          \
          AddStackTraceElement(__FUNCTION__, __FILE__, __LINE__,               \
                               (stack_frame_context)));                        \
    }                                                                          \
  } while (false)

// No-op if the result was a success. Otherwise, acts like THROW with the
// exception propagated from the result. Uses the literal textual representation
// of the result parameter as the stack_frame_context.
// Use this macro in favor of PROPAGATE_ON_FAILURE_WITH_CONTEXT unless there is
// a good reason to attach a more specific contextual information to the stack
// frame.
#define PROPAGATE_ON_FAILURE(result)                                           \
  PROPAGATE_ON_FAILURE_WITH_CONTEXT((result), "", #result)

#endif  // SUPERSONIC_BASE_EXCEPTION_EXCEPTION_MACROS_H_
