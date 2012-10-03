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
// Specialization of common::FailureOr* classes for Supersonic.
// TODO(user): rename to failureor.h (in a dedicated CL).

#ifndef SUPERSONIC_BASE_EXCEPTION_RESULT_H_
#define SUPERSONIC_BASE_EXCEPTION_RESULT_H_

#include "supersonic/utils/exception/coowned_pointer.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"

namespace supersonic {

// Importing Success and Failure functions, so that they can be used directly
// in the supersonic namespace.
using common::Success;
using common::Failure;

// Adapters to turn exceptions into runtime crashes. Again, we simply re-export
// all the adapters from common/failureor.h.
using common::SucceedOrDie;

// For void results. Can be considered a quasi-specialization for
// FailureOr<void>.
//
// To create an instance of this class, call:
//     FailureOrVoid foo(Success()) or
//     FailureOrVoid foo(Failure(...)).
class FailureOrVoid : public common::FailureOrVoid<Exception> {
 public:
  FailureOrVoid(const common::FailurePropagator<Exception>& failure)   // NOLINT
      : common::FailureOrVoid<Exception>(failure) {}

  FailureOrVoid(const common::VoidPropagator& success)                 // NOLINT
      : common::FailureOrVoid<Exception>(success) {}
  // Copyable.
};

// For results passed by value.
//
// To create an instance of this class, call:
//     FailureOr<T> foo(Success(value)) or
//     FailureOr<T> foo(Failure(...)).
template<typename T>
class FailureOr : public common::FailureOr<T, Exception> {
 public:
  FailureOr(const common::FailurePropagator<Exception>& failure)       // NOLINT
      : common::FailureOr<T, Exception>(failure) {}

  template<typename ObservedResult>
  FailureOr(
      const common::ReferencePropagator<ObservedResult>& result)       // NOLINT
      : common::FailureOr<T, Exception>(result) {}

  template<typename ObservedResult>
  FailureOr(
      const common::ConstReferencePropagator<ObservedResult>& result)  // NOLINT
      : common::FailureOr<T, Exception>(result) {}
  // Copyable.
};

// For results passed by reference.
// NOTE: if Success, the referenced object must be valid for as long as
// FailureOrReference instances are in use, just as if this was a plain
// reference.
//
// To create an instance of this class, call:
//     FailureOrReference<T> foo(Success(variable)) or
//     FailureOrReference<T> foo(Failure(...)).
template<typename T>
class FailureOrReference : public common::FailureOrReference<T, Exception> {
 public:
  FailureOrReference(
      const common::FailurePropagator<Exception>& failure)             // NOLINT
      : common::FailureOrReference<T, Exception>(failure) {}

  template<typename ObservedResult>
  FailureOrReference(
      const common::ReferencePropagator<ObservedResult>& result)       // NOLINT
      : common::FailureOrReference<T, Exception>(result) {}

  template<typename ObservedResult>
  FailureOrReference(
      const common::ConstReferencePropagator<ObservedResult>& result)  // NOLINT
      : common::FailureOrReference<T, Exception>(result) {}
  // Copyable.
};

// For results passed by pointer w/ ownership transferred to the caller.
// The result is owned by this wrapper object until explicitly released.
// Semantics resemble the linked_ptr<T>.
//
// To create an instance of this class, call:
//     FailureOrOwned<T> foo(Success(owned_ptr)) or
//     FailureOrOwned<T> foo(Failure(...)).
template<typename T>
class FailureOrOwned : public common::FailureOrOwned<T, Exception> {
 public:
  FailureOrOwned(const common::FailurePropagator<Exception>& failure)  // NOLINT
      : common::FailureOrOwned<T, Exception>(failure) {}

  template<typename ObservedResult>
  FailureOrOwned(
      const common::ReferencePropagator<ObservedResult>& result)       // NOLINT
      : common::FailureOrOwned<T, Exception>(result) {}

  template<typename ObservedResult>
  FailureOrOwned(
      const common::ConstReferencePropagator<ObservedResult>& result)  // NOLINT
      : common::FailureOrOwned<T, Exception>(result) {}
  // Copyable.
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_EXCEPTION_RESULT_H_
