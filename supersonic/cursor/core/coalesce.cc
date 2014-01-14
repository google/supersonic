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

#include "supersonic/cursor/core/coalesce.h"

#include <stdint.h>

#include <memory>
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/core/generate.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/stl_util.h"

namespace supersonic {

class TupleSchema;

namespace {

class CoalesceCursor : public BasicCursor {
 public:
  // Takes ownership of child cursors and the projector
  // The child cursors must not be NULL and must have distinct attribute names.
  CoalesceCursor(const vector<Cursor*>& children,
                 const BoundMultiSourceProjector* projector)
      : BasicCursor(CHECK_NOTNULL(projector)->result_schema()),
        projector_(projector) {
    for (int i = 0; i < children.size(); ++i) {
      Cursor* child = children[i];
      DCHECK(child);
      inputs_.push_back(new CursorIterator(child));
    }
  }

  virtual ResultView Next(rowcount_t max_row_count);
  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    for (int i = 0; i < inputs_.size(); ++i) {
      inputs_[i]->ApplyToCursor(transformer);
    }
  }

  virtual CursorId GetCursorId() const { return COALESCE; }

 private:
  util::gtl::PointerVector<CursorIterator> inputs_;
  std::unique_ptr<const BoundMultiSourceProjector> projector_;

  DISALLOW_COPY_AND_ASSIGN(CoalesceCursor);
};

ResultView CoalesceCursor::Next(rowcount_t max_row_count) {
  const int inputs_count = inputs_.size();
  DCHECK_GT(inputs_count, 0);
  for (int i = 0; i < inputs_count; ++i) {
    CursorIterator* input = inputs_[i].get();
    if (!input->Next(max_row_count, false)) {
      for (int j = 0; j < i; ++j) {
        inputs_[j]->truncate(0);
      }
      return input->result();
    }
    const int view_row_count = input->view().row_count();
    DCHECK_GT(view_row_count, 0);
    if (view_row_count < max_row_count) {
      max_row_count = view_row_count;
    }
  }
  vector<const View*> views;
  for (int i = 0; i < inputs_count; ++i) {
    CursorIterator* input = inputs_[i].get();
    input->truncate(max_row_count);
    views.push_back(&input->view());
  }
  projector_->Project(views.begin(), views.end(), my_view());
  my_view()->set_row_count(max_row_count);
  return ResultView::Success(my_view());
}

class CoalesceOperation : public BasicOperation {
 public:
  virtual ~CoalesceOperation() {}

  explicit CoalesceOperation(const vector<Operation*>& children)
      : BasicOperation(children) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    vector<Cursor*> child_cursors(children_count());
    ElementDeleter child_cursors_deleter(&child_cursors);
    for (int i = 0; i < children_count(); ++i) {
      FailureOrOwned<Cursor> child_cursor = child_at(i)->CreateCursor();
      PROPAGATE_ON_FAILURE(child_cursor);
      child_cursors[i] = child_cursor.release();
    }
    FailureOrOwned<Cursor> cursor = BoundCoalesce(child_cursors);
    PROPAGATE_ON_FAILURE(cursor);
    // CoalesceCursor took ownership, so prevent child_cursors_delete
    // to delete them.
    child_cursors.clear();
    return Success(cursor.release());
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CoalesceOperation);
};

}  // namespace

FailureOrOwned<Cursor> BoundCoalesce(const vector<Cursor*>& children) {
  if (children.empty()) {
    return BoundGenerate(0);
  }
  if (children.size() == 1) {
    DCHECK(children[0] != NULL);
    return Success(children[0]);
  }

  CompoundMultiSourceProjector all_attributes_projector;
  vector<const TupleSchema*> child_schemas;
  for (int i = 0; i < children.size(); ++i) {
    Cursor* child = children[i];
    DCHECK(child != NULL);
    child_schemas.push_back(&child->schema());
    all_attributes_projector.add(i, ProjectAllAttributes());
  }
  FailureOrOwned<const BoundMultiSourceProjector> bound_projector =
      all_attributes_projector.Bind(child_schemas);
  PROPAGATE_ON_FAILURE(bound_projector);
  return Success(new CoalesceCursor(children, bound_projector.release()));
}

Operation* Coalesce(const vector<Operation*>& children) {
  return new CoalesceOperation(children);
}

}  // namespace supersonic
