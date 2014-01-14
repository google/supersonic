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

#include "supersonic/cursor/core/foreign_filter.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

namespace {

class ForeignFilterCursor : public BasicCursor {
 public:
  ForeignFilterCursor(
      const int filter_key_column,
      const int input_foreign_key_column,
      const BoundSingleSourceProjector* nonkey_input_projector,
      const BoundMultiSourceProjector* result_projector,
      Cursor* filter,
      Cursor* input)
      : BasicCursor(result_projector->result_schema()),
        filter_key_column_(filter_key_column),
        input_foreign_key_column_(input_foreign_key_column),
        nonkey_input_projector_(nonkey_input_projector),
        result_projector_(result_projector),
        input_copier_(nonkey_input_projector_.get(), false),
        filter_(filter),
        input_(input),
        input_indirector_(
            TupleSchema::Singleton("KeySelector", kRowidDatatype, NOT_NULLABLE),
            HeapBufferAllocator::Get()),
        filter_rowid_(
            TupleSchema::Singleton("KeySelector", kRowidDatatype, NOT_NULLABLE),
            HeapBufferAllocator::Get()),
        nonkey_input_block_(nonkey_input_projector_->result_schema(),
                            HeapBufferAllocator::Get()) {
    CHECK(input_indirector_.Reallocate(kDefaultRowCount));
    CHECK(filter_rowid_.Reallocate(kDefaultRowCount));
    CHECK(nonkey_input_block_.Reallocate(kDefaultRowCount));
    CHECK_EQ(kRowidDatatype,
             filter_.schema().attribute(filter_key_column).type());
    CHECK(!filter_.schema().attribute(filter_key_column).is_nullable());
    CHECK_EQ(kRowidDatatype,
             input_.schema().attribute(input_foreign_key_column).type());
    CHECK(!input_.schema().attribute(input_foreign_key_column).is_nullable());
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    max_row_count = std::min(max_row_count, input_indirector_.row_capacity());
    rowcount_t row_count = 0;
    const rowid_t* filter_key;
    const rowid_t* input_key;
    rowid_t* input_indirector =
        input_indirector_.mutable_column(0)->
            mutable_typed_data<kRowidDatatype>();
    rowid_t* filter_rowid =
        filter_rowid_.mutable_column(0)->mutable_typed_data<kRowidDatatype>();
    do {
      if (!input_.EagerNext()) {
        if (filter_.is_failure()) return filter_.result();
        if (input_.is_done()) filter_.Terminate();
        return input_.result();  // EOS or WAITING_ON_BARRIER.
      }
      if (!filter_.EagerNext()) {
        if (filter_.is_done()) {
          input_.Terminate();  // EOS or failure.
        } else {
          input_.truncate(0);  // WAITING_ON_BARRIER.
        }
        return filter_.result();
      }
      // Both input_ and filter_ have been successfully advanced. Extract the
      // keys to merge.
      filter_key = filter_.view().column(filter_key_column_).
          typed_data<kRowidDatatype>();
      input_key = input_.view().column(input_foreign_key_column_).
          typed_data<kRowidDatatype>();

      rowcount_t filter_offset = 0;
      rowcount_t input_offset = 0;
      while (row_count < max_row_count) {
        if (filter_key[filter_offset] < input_key[input_offset]) {
          ++filter_offset;  // TODO(user): progressive binary search.
          if (filter_offset == filter_.view().row_count()) break;
        } else if (filter_key[filter_offset] > input_key[input_offset]) {
          ++input_offset;  // TODO(user): progressive binary search.
          if (input_offset == input_.view().row_count()) break;
        } else {
          // Equal.
          input_indirector[row_count] = input_offset;
          filter_rowid[row_count] = filter_.current_row_index() + filter_offset;
          ++row_count;
          ++input_offset;  // But not ++filter_offset, as it is 1-to-many.
          if (input_offset == input_.view().row_count()) break;
        }
      }
      filter_.truncate(filter_offset);
      input_.truncate(input_offset);
    } while (row_count == 0);
    rowcount_t copied = input_copier_.Copy(row_count,
                                           input_.view(), input_indirector, 0,
                                           &nonkey_input_block_);
    DCHECK_EQ(row_count, copied);

    // Project expects an iterator range of View*. We use a plain array,
    // passing pointers to the first element and one-past-end.
    const View* intermediate_sources[] = {
      &filter_rowid_.view(),
      &nonkey_input_block_.view(),
    };
    result_projector_->Project(&intermediate_sources[0],
                               &intermediate_sources[2],
                               my_view());
    my_view()->set_row_count(row_count);
    return ResultView::Success(my_view());
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

  virtual void Interrupt() {
    input_.Interrupt();
    filter_.Interrupt();
  }

  virtual void ApplyToChildren(CursorTransformer* transformer) {
    filter_.ApplyToCursor(transformer);
    input_.ApplyToCursor(transformer);
  }

  virtual CursorId GetCursorId() const { return FOREIGN_FILTER; }

 private:
  const int filter_key_column_;
  const int input_foreign_key_column_;
  std::unique_ptr<const BoundSingleSourceProjector> nonkey_input_projector_;
  std::unique_ptr<const BoundMultiSourceProjector> result_projector_;
  SelectiveViewCopier input_copier_;
  CursorIterator filter_;
  CursorIterator input_;
  Block input_indirector_;
  Block filter_rowid_;        // Result of filtering - the new foreign key.
  Block nonkey_input_block_;  // Result of filtering - the non-key columns.
};

FailureOrVoid EnsureSingleColumnRowidTypeNotNull(const TupleSchema& schema) {
  if (schema.attribute_count() != 1) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_COUNT_MISMATCH,
        StrCat("Expected exactly 1 attribute for the key; found: ",
               schema.attribute_count())));
  }
  if (schema.attribute(0).type() != kRowidDatatype) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StringPrintf(
            "Column %s designated as a key has type %s; expected %s",
            schema.attribute(0).name().c_str(),
            GetTypeInfo(schema.attribute(0).type()).name().c_str(),
            TypeTraits<kRowidDatatype>::name())));
  }
  if (schema.attribute(0).is_nullable()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_IS_NULLABLE,
        StringPrintf(
            "Column %s designated as a key is nullable. Please "
            "remove nullability explicitly (by casting, or using NVL)",
            schema.attribute(0).name().c_str())));
  }
  return Success();
}

class ForeignFilterOperation : public BasicOperation {
 public:
  ForeignFilterOperation(const SingleSourceProjector* filter_key,
                         const SingleSourceProjector* foreign_key,
                         Operation* filter,
                         Operation* input)
      : BasicOperation(filter, input),
        filter_key_(filter_key),
        foreign_key_(foreign_key) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> filter = child_at(0)->CreateCursor();
    PROPAGATE_ON_FAILURE(filter);
    FailureOrOwned<Cursor> input = child_at(1)->CreateCursor();
    PROPAGATE_ON_FAILURE(input);
    FailureOrOwned<const BoundSingleSourceProjector> filter_key =
        filter_key_->Bind(filter->schema());
    PROPAGATE_ON_FAILURE(filter_key);
    PROPAGATE_ON_FAILURE(
        EnsureSingleColumnRowidTypeNotNull(filter_key->result_schema()));
    FailureOrOwned<const BoundSingleSourceProjector> foreign_key =
        foreign_key_->Bind(input->schema());
    PROPAGATE_ON_FAILURE(foreign_key);
    PROPAGATE_ON_FAILURE(
        EnsureSingleColumnRowidTypeNotNull(foreign_key->result_schema()));
    return Success(
        BoundForeignFilter(filter_key->source_attribute_position(0),
                           foreign_key->source_attribute_position(0),
                           filter.release(), input.release()));
  }

 private:
  std::unique_ptr<const SingleSourceProjector> filter_key_;
  std::unique_ptr<const SingleSourceProjector> foreign_key_;
  DISALLOW_COPY_AND_ASSIGN(ForeignFilterOperation);
};

}  // namespace


Operation* ForeignFilter(const SingleSourceProjector* filter_key,
                         const SingleSourceProjector* foreign_key,
                         Operation* filter,
                         Operation* input) {
  return new ForeignFilterOperation(filter_key, foreign_key, filter, input);
}

Cursor* BoundForeignFilter(const int filter_key_column,
                           const int input_foreign_key_column,
                           Cursor* filter,
                           Cursor* input) {
  std::unique_ptr<BoundSingleSourceProjector> input_projector(
      new BoundSingleSourceProjector(input->schema()));
  for (int i = 0; i < input->schema().attribute_count(); ++i) {
    if (i != input_foreign_key_column) input_projector->Add(i);
  }

  vector<const TupleSchema*> sources;
  TupleSchema key_schema(
      TupleSchema::Singleton("$parentFK$", kRowidDatatype, NOT_NULLABLE));
  sources.push_back(&key_schema);
  sources.push_back(&input_projector->result_schema());
  std::unique_ptr<BoundMultiSourceProjector> result_projector(
      new BoundMultiSourceProjector(sources));
  int input_attribute_index = 0;
  for (int i = 0; i < input->schema().attribute_count(); ++i) {
    if (i == input_foreign_key_column) {
      result_projector->AddAs(0, 0, input->schema().attribute(i).name());
    } else {
      result_projector->Add(1, input_attribute_index++);
    }
  }
  return new ForeignFilterCursor(filter_key_column, input_foreign_key_column,
                                 input_projector.release(),
                                 result_projector.release(),
                                 filter, input);
}

}  // namespace supersonic
