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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_CURSOR_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_CURSOR_H_

#include <stddef.h>

#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include <atomic>
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

// Common base class for cursors that:
// * know their schema during construction.
// * possibly have children.
class BasicCursor : public Cursor {
 public:
  virtual ~BasicCursor() {}

  virtual const TupleSchema& schema() const { return schema_; }
  virtual void AppendDebugDescription(string* target) const;

  // Propagates interruption request to the children, and sets the interrupted_
  // status. Thread-safe wrt Next(). (But, doesn't introduce memory barriers,
  // i.e. doesn't guarantee happens-before).
  virtual void Interrupt() {
    interrupted_.store(true, std::memory_order_relaxed);
    for (int i = 0; i < children_.size(); ++i) {
      children_[i]->Interrupt();
    }
  }

  // Basic cursor's Transform() will run the callback on all of its children to
  // which it has access. This means that all classes derived from BasicCursor
  // which do not pass the ownership of its children to the base class have
  // to override Transform() with their own case-specific logic.
  virtual void ApplyToChildren(CursorTransformer* transformer) {
    for (int i = 0; i < children_.size(); ++i) {
      children_[i].reset(transformer->Transform(children_[i].release()));
    }
  }

 protected:
  // Base constructor for cursors with no children.
  explicit BasicCursor(const TupleSchema& schema)
      : schema_(schema),
        view_(schema),
        interrupted_(false) {}

  // Base constructor for cursors with one child. Takes ownership of the child.
  BasicCursor(const TupleSchema& schema, Cursor* child)
      : schema_(schema),
        view_(schema),
        interrupted_(false) {
    children_.push_back(make_linked_ptr(child));
  }

  // Base constructor for cursors with two children. Takes ownership of the
  // children.
  BasicCursor(const TupleSchema& schema, Cursor* child1, Cursor* child2)
      : schema_(schema),
        view_(schema),
        interrupted_(false) {
    children_.push_back(make_linked_ptr(child1));
    children_.push_back(make_linked_ptr(child2));
  }

  // Base constructor for cursors with arbitrary number of children.
  // Takes ownership of the children.
  BasicCursor(const TupleSchema& schema, const vector<Cursor*>& children)
      : schema_(schema),
        view_(schema),
        interrupted_(false) {
    for (int i = 0; i < children.size(); ++i) {
      children_.push_back(make_linked_ptr(children[i]));
    }
  }

  // Returns the n-th child. N must be smaller than children_count().
  Cursor* child_at(const size_t position) const {
    DCHECK_LT(position, children_.size());
    return children_[position].get();
  }

  // Convenience shortcut for child_at(0) for cursors that have exactly one
  // child (which is a common case).
  Cursor* child() const {
    DCHECK_EQ(1, children_.size());
    return children_[0].get();
  }

  // Returns the total count of children.
  size_t children_count() const { return children_.size(); }

  // Result View accessor.
  View* my_view() { return &view_; }

  // Debugging support. Returns a string that describes this cursor. May be
  // overridden by subclasses. The default implementation returns cursor's
  // non-qualified class name. The method is called at most once in any
  // cursor's lifetime.
  virtual string DebugDescription() const;

  // Checks whether the cursor has been interrupted. NOTE: for performance,
  // we're using a relaxed synchronization model. In practice, it means that
  // it might happen that a cursor doesn't see the flag set yet, even though
  // its children already have seen it.
  bool is_interrupted() const {
    return interrupted_.load(std::memory_order_relaxed);
  }

  // Intended use:
  // PROPAGATE_ON_FAILURE(ThrowIfInterrupted());
  FailureOrVoid ThrowIfInterrupted() const {
    if (is_interrupted()) THROW(new Exception(INTERRUPTED, ""));
    return Success();
  }

 private:
  // Caching around GetDebugDescriptor.
  const string& LazilyGetDebugDescription() const;

  TupleSchema schema_;
  View view_;
  vector<linked_ptr<Cursor> > children_;
  mutable string debug_description_;

  // Stores the interruption status for later use. Aimed at leaf cursors,
  // as they need to let the processing thread (calling Next()) know about
  // the interruption request. This flag provides a convenient means to do so.
  std::atomic<bool> interrupted_;

  DISALLOW_COPY_AND_ASSIGN(BasicCursor);
};

// A convenience base class for cursors that pass most calls directly to
// the delegate, decorated cursor.
class BasicDecoratorCursor : public Cursor {
 public:
  virtual ~BasicDecoratorCursor() {}

  virtual const TupleSchema& schema() const { return delegate()->schema(); }

  virtual ResultView Next(rowcount_t max_row_count) {
    return delegate()->Next(max_row_count);
  }

  virtual bool IsWaitingOnBarrierSupported() const {
    return delegate()->IsWaitingOnBarrierSupported();
  }

  virtual void Interrupt() { delegate()->Interrupt(); }

  virtual CursorId GetCursorId() const { return DECORATOR; }

  virtual void AppendDebugDescription(string* target) const {
    delegate()->AppendDebugDescription(target);
  }

 protected:
  explicit BasicDecoratorCursor(Cursor* delegate) : delegate_(delegate) {}

  Cursor* delegate() { return delegate_.get(); }
  const Cursor* delegate() const { return delegate_.get(); }

 private:
  std::unique_ptr<Cursor> delegate_;
  DISALLOW_COPY_AND_ASSIGN(BasicDecoratorCursor);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_BASIC_CURSOR_H_
