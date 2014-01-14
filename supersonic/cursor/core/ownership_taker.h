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
//
// Cursor wrappers that can assume ownership of arbitrary objects, deleting
// them when the cursor is destroyed.

#ifndef SUPERSONIC_CURSOR_CORE_OWNERSHIP_TAKER_H_
#define SUPERSONIC_CURSOR_CORE_OWNERSHIP_TAKER_H_

#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/proto/cursors.pb.h"

namespace supersonic {

template<typename Owned>
class OwnershipTaker : public Cursor {
 public:
  virtual ~OwnershipTaker() {}

  static OwnershipTaker* Create(Cursor* child, Owned* owned) {
    return new OwnershipTaker(child, owned);
  }

  virtual const TupleSchema& schema() const { return child_->schema(); }

  virtual ResultView Next(rowcount_t max_row_count) {
    return child_->Next(max_row_count);
  }

  virtual void Interrupt() { child_->Interrupt(); }

  virtual void AppendDebugDescription(string* target) const {
    child_->AppendDebugDescription(target);
  }

  virtual CursorId GetCursorId() const { return OWNERSHIP_TAKER; }

 private:
  OwnershipTaker(Cursor* child, Owned* owned)
      : owned_(owned),
        child_(child) {}
  // Defining owned_ field first, so it will outlive child_.
  std::unique_ptr<Owned> owned_;
  const std::unique_ptr<Cursor> child_;
  DISALLOW_COPY_AND_ASSIGN(OwnershipTaker);
};

namespace internal {

template<typename A, typename B>
pair<linked_ptr<A>, linked_ptr<B> >* new_linked_pair(A* a, B* b) {
  return new pair<linked_ptr<A>, linked_ptr<B> >(make_linked_ptr(a),
                                                 make_linked_ptr(b));
}

}  // namespace internal

// Take-ownership functions, for up to 6 arguments. (Can be extended further
// if needed). Return a cursor that, when itself destroyed, will destroy all
// the objects specified as arguments.

template<typename T>
Cursor* TakeOwnership(Cursor* child, T* owned) {
  return OwnershipTaker<T>::Create(child, owned);
}

template<typename A, typename B>
Cursor* TakeOwnership(Cursor* child, A* a, B* b) {
  return TakeOwnership(child, internal::new_linked_pair(a, b));
}

template<typename A, typename B, typename C>
Cursor* TakeOwnership(Cursor* child, A* a, B* b, C* c) {
  return TakeOwnership(child, internal::new_linked_pair(a, b), c);
}

template<typename A, typename B, typename C, typename D>
Cursor* TakeOwnership(Cursor* child, A* a, B* b, C* c, D* d) {
  return TakeOwnership(child,
                       internal::new_linked_pair(a, b),
                       internal::new_linked_pair(c, d));
}

template<typename A, typename B, typename C, typename D, typename E>
Cursor* TakeOwnership(Cursor* child, A* a, B* b, C* c, D* d, E* e) {
  return TakeOwnership(child,
                       internal::new_linked_pair(a, b),
                       internal::new_linked_pair(c, d), e);
}

template<typename A, typename B, typename C, typename D, typename E, typename F>
Cursor* TakeOwnership(Cursor* child, A* a, B* b, C* c, D* d, E* e, F* f) {
  return TakeOwnership(child,
                       internal::new_linked_pair(a, b),
                       internal::new_linked_pair(c, d),
                       internal::new_linked_pair(e, f));
}

// Takes a dynamically-allocated operation and 'turns it' into a cursor;
// i.e., it creates a cursor from it, and make it the owner of the operation.
// The operation gets deleted when the cursor is deleted. You should not use
// the operation directly after having passed it to this function.
inline FailureOrOwned<Cursor> TurnIntoCursor(Operation* operation) {
  std::unique_ptr<Operation> operation_ptr(operation);
  FailureOrOwned<Cursor> result = operation_ptr->CreateCursor();
  PROPAGATE_ON_FAILURE(result);
  return Success(TakeOwnership(result.release(), operation_ptr.release()));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_OWNERSHIP_TAKER_H_
