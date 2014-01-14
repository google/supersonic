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
// A basic interface for operations.
// Operations are factories of cursors and potentially other interfaces
// that are giving access to the operation output data.

#ifndef SUPERSONIC_CURSOR_BASE_OPERATION_H_
#define SUPERSONIC_CURSOR_BASE_OPERATION_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/lookup_index.h"

namespace supersonic {

class BufferAllocator;

class Operation {
 public:
  virtual ~Operation() {}

  // Sets an allocator to be used by subsequent invocations of CreateCursor().
  // Ownership of the allocator remains with the caller. The allocator must
  // live at least as long as any operation or cursor that is using it.
  // If cascade_to_children is set, the allocator should be propagated to
  // all operation's children.
  // If an operation wants to impose its additional memory limits, it should do
  // so by wrapping this allocator into another allocator, and passing that
  // wrapped allocator to its cursors.
  // Passing NULL as buffer_allocator resets the allocator to default / unset.
  virtual void SetBufferAllocator(BufferAllocator* buffer_allocator,
                                  bool cascade_to_children) = 0;

  // A version of SetBufferAllocator that doesn't change the allocator for the
  // operation(s) if it was already set. This is useful when we want some
  // subtrees of the operation tree to have different allocator.
  virtual void SetBufferAllocatorWhereUnset(BufferAllocator* buffer_allocator,
                                            bool cascade_to_children) = 0;

  // Creates a cursor if possible or returns a result with is_failure() set.
  // Ownership is transfered to the caller.
  // NOTE: Operation must outlive cursors returned by it.
  // NOTE: if you want to crash on failure rather than returning an exception,
  // wrap the invocation in SucceedOrDie(.).
  virtual FailureOrOwned<Cursor> CreateCursor() const = 0;

  // Helper for debugging; prints out the structure of this Operation.
  virtual void AppendDebugDescription(string* const target) const = 0;

  // Convenience methods.

  // Convenience wrapper for in-line printing of debug description.
  string DebugDescription() const {
    string result;
    AppendDebugDescription(&result);
    return result;
  }

 protected:
  // To allow instantiation in subclasses.
  Operation() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Operation);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_BASE_OPERATION_H_
