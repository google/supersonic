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
// Encapsulation of errors.

#ifndef SUPERSONIC_BASE_EXCEPTION_EXCEPTION_H_
#define SUPERSONIC_BASE_EXCEPTION_EXCEPTION_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/walltime.h"
#include "supersonic/utils/exception/stack_trace.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace common {
  class StackTrace;
}  // namespace common

namespace supersonic {

// Represents an error during data processing.
// NOTE: this class is NOT intended for subclassing. Exception 'types' are
// differentiated by their associated error codes.
// Rationale:
// Exception type hierarchies are most useful in open-ended APIs, where the
// users are likely to define new exception types. This is not the case in
// Supersonic. And, in fact, introducing full-blown exception type hierarchy
// would cause a number of hurdles. Particularly:
// (1) To support the few important cases where we copy exceptions, we would
// need a polymorphic 'Clone' supported by every subclass in the hierarchy. It
// would be difficult to enforce at compile time, and erroneous omissions
// would lead to subtle run-time bugs.
// (2) Determining exception types would require some type of dynamic type
// checking. RTTI is banned by the C++ style guide.
// All in all, going with the the simpler solution for now; will revisit should
// new data show its deficiencies.
class Exception {
 public:
  Exception(const ReturnCode code, const string& message) {
    serial_.set_timestamp(GetCurrentTimeMicros());
    serial_.set_return_code(code);
    serial_.set_message(message);
  }

  // Returns the time at which the exception occurred. The returned number
  // denotes the number of microseconds since Epoch.
  int64 timestamp() const { return serial_.timestamp(); }

  // Returns the return code.
  ReturnCode return_code() const { return serial_.return_code(); }

  // Returns a brief, user-readable, self-contained message.
  const string& message() const { return serial_.message(); }

  // Creates a deep-copy of this exception.
  Exception* Clone() const { return new Exception(*this); }

  // Returns a short description of this exception, containing the error code
  // and message.
  string ToString() const {
    return ReturnCode_Name(return_code()) + ": " + message();
  }

  // Prints this exception and its backtrace to a string. The first line of
  // the output contains the result of the ToString() method for this object.
  // Remaining lines represent data previously recorded by subsequent calls to
  // AddStackTraceElement(...).
  string PrintStackTrace() const {
    string out = ToString() + "\n";
    common::AppendStackTraceDump(serial_.stack_trace(), &out);
    return out;
  }

  // Returns the stack trace of this exception. The first element in the trace
  // is the deep-most stack frame, where the exception has been thrown.
  const common::StackTrace& stack_trace() const {
    return serial_.stack_trace();
  }

  // Adds a new stack trace element to the stack trace.
  Exception* AddStackTraceElement(const StringPiece& function,
                                  const StringPiece& filename,
                                  int line,
                                  const StringPiece& context) {
    common::AddStackTraceElement(
        function, filename, line, context,
        serial_.mutable_stack_trace());
    return this;
  }

  // Sets the new message.
  // NOTE: this is a substitute for Java-like exception chaining. Since we
  // don't use full-blown polymorphic exception hierarchy (see the class
  // docstring for rationale), we don't need full-blown exception chaining,
  // as there isn't much sense to modify the error code as the exception is
  // propagated. It does make sense, however, to allow higher stack frames to
  // provide additional contextual information as to the cause of the exception.
  // We achieve this simply by allowing them to alter the message.
  void set_message(const string& message) { serial_.set_message(message); }

  // Creates the exception out of its serialized representation.
  static Exception* Deserialize(const SerializedException& serial) {
    return new Exception(serial);
  }

  // Writes the exception into a serialized representation.
  void Serialize(SerializedException* serial) {
    serial->CopyFrom(serial_);
  }

 private:
  // For deserialization.
  explicit Exception(const SerializedException& serial) : serial_(serial) {}

  SerializedException serial_;

  void operator=(const Exception&);  // DISALLOW_ASSIGN
  // Internally used.
  Exception(const Exception& other)
      : serial_(other.serial_) {}
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_EXCEPTION_EXCEPTION_H_
