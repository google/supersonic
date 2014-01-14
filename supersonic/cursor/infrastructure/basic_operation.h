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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_OPERATION_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_OPERATION_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

class LookupIndex;

// Common base class for operations that:
// * handles children operations,
// * adds default implementations of CreateCursor factory,
// * stores BufferAllocator to be used by created cursors.
class BasicOperation : public Operation {
 public:
  virtual ~BasicOperation() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    THROW(new Exception(
        ERROR_NOT_IMPLEMENTED,
        "Operation <" + LazilyGetDebugDescription() +
        "> doesn't support Cursor interface"));
  }

  virtual FailureOrOwned<LookupIndex> CreateLookupIndex() {
    THROW(new Exception(
        ERROR_NOT_IMPLEMENTED,
        "Operation <" + LazilyGetDebugDescription() +
        "> doesn't support LookupIndex interface"));
  }

  virtual void AppendDebugDescription(string* const target) const;

  virtual void SetBufferAllocator(BufferAllocator* buffer_allocator,
                                  bool cascade_to_children) {
    DoSetBufferAllocator(buffer_allocator, true);
    if (cascade_to_children) {
      for (int i = 0; i < children_count(); ++i) {
        child_at(i)->SetBufferAllocator(buffer_allocator, true);
      }
    }
  }

  virtual void SetBufferAllocatorWhereUnset(BufferAllocator* buffer_allocator,
                                            bool cascade_to_children) {
    DoSetBufferAllocator(buffer_allocator, false);
    if (cascade_to_children) {
      for (int i = 0; i < children_count(); ++i) {
        child_at(i)->SetBufferAllocatorWhereUnset(buffer_allocator, true);
      }
    }
  }

 protected:
  // Base constructor for operations with no children.
  // Buffer allocator's ownership stays with the caller.
  BasicOperation()
      : buffer_allocator_(NULL),
        default_row_count_(Cursor::kDefaultRowCount) {}

  // Base constructor for operations with one child.
  // Takes ownership of the child.
  explicit BasicOperation(Operation* child)
      : buffer_allocator_(NULL),
        default_row_count_(Cursor::kDefaultRowCount) {
    children_.push_back(make_linked_ptr(child));
  }

  // Base constructor for operations with rwo children.
  // Takes ownership of the children.
  // Buffer allocator's ownership stays with the caller.
  BasicOperation(Operation* child1,
                 Operation* child2)
      : buffer_allocator_(NULL),
        default_row_count_(Cursor::kDefaultRowCount) {
    children_.push_back(make_linked_ptr(child1));
    children_.push_back(make_linked_ptr(child2));
  }
  // Base constructor for operations with arbitrary number of children.
  // Takes ownership of the children.
  // Buffer allocator's ownership stays with the caller.
  explicit BasicOperation(const vector<Operation*>& children)
      : buffer_allocator_(NULL),
        default_row_count_(Cursor::kDefaultRowCount) {
    for (int i = 0; i < children.size(); ++i) {
      children_.push_back(make_linked_ptr(children[i]));
    }
  }

  // Returns the n-th child. N must be smaller than children_count().
  Operation* child_at(const size_t position) const {
    DCHECK_LT(position, children_.size());
    return children_[position].get();
  }

  // Convenience shortcut for child_at(0) for operations that have exactly one
  // child (which is a common case).
  Operation* child() const {
    DCHECK_EQ(1, children_.size());
    return children_[0].get();
  }

  // Returns the total count of children.
  size_t children_count() const { return children_.size(); }

  // Retrieves the allocator. Returns HeapBufferAllocator::Get() if the
  // allocator wasn't set.
  BufferAllocator* buffer_allocator() const {
    return (buffer_allocator_ != NULL)
        ? buffer_allocator_
        : HeapBufferAllocator::Get();
  }

  // The override parameter determines if the function should set the allocator
  // if it was already set before.
  virtual void DoSetBufferAllocator(BufferAllocator* buffer_allocator,
                                    bool override) {
    if (buffer_allocator_ == NULL || override) {
      buffer_allocator_ = buffer_allocator;
    }
  }

  // Retrieves the default row count associated with this operation.
  size_t default_row_count() const { return default_row_count_; }

  // Sets a new default row count for this operation.
  void set_default_row_count(size_t default_row_count) {
    default_row_count_ = default_row_count;
  }

  // Debugging support. Returns a string that describes this operation. May be
  // overridden by subclasses. The default implementation returns operation's
  // non-qualified class name. The method is called at most once in any
  // operation's lifetime.
  virtual string DebugDescription() const;

 private:
  // Caching around GetDebugDescriptor.
  const string& LazilyGetDebugDescription() const;

  BufferAllocator* buffer_allocator_;
  size_t default_row_count_;

  vector<linked_ptr<Operation> > children_;
  mutable string debug_description_;

  DISALLOW_COPY_AND_ASSIGN(BasicOperation);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_OPERATION_H_
