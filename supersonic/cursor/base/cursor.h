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
// A basic interface for streaming block processing.

#ifndef SUPERSONIC_CURSOR_BASE_CURSOR_H_
#define SUPERSONIC_CURSOR_BASE_CURSOR_H_

#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/exception/coowned_pointer.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class View;

// Result of Cursor::Next().
// TODO(user): consider refactoring to something like FailureOr<NextResult>.
class ResultView {
 public:
  // Needed to support implicit conversion from return Success(...).
  // TODO(user): revisit the ResultView design.
  ResultView(const common::FailurePropagator<Exception>& failure)  // NOLINT
      : result_(supersonic::Failure(failure.exception)),
        status_(failure.exception->return_code()) {}

  // A result that indicates END_OF_INPUT.
  static ResultView EOS() { return ResultView(NULL, END_OF_INPUT); }

  // A result that indicates BEFORE_INPUT.
  static ResultView BOS() { return ResultView(NULL, BEFORE_INPUT); }

  // A result that indicates WAITING_ON_BARRIER.
  static ResultView WaitingOnBarrier() {
    return ResultView(NULL, WAITING_ON_BARRIER);
  }

  // A result that indicates a success, with a valid value.
  static ResultView Success(const View* view) { return ResultView(view, OK); }

  // A result that indicates a failure, with a given exception.
  static ResultView Failure(Exception* exception) {
    return ResultView(exception);
  }

  bool has_data() const {
    mark_checked();
    return (status_ == OK);
  }
  bool is_done() const {
    mark_checked();
    return is_eos() || is_failure();
  }

  bool is_eos() const {
    mark_checked();
    return status_ == END_OF_INPUT;
  }
  bool is_bos() const {
    mark_checked();
    return status_ == BEFORE_INPUT;
  }
  bool is_failure() const {
    mark_checked();
    return result_.is_failure();
  }
  bool is_waiting_on_barrier() const {
    mark_checked();
    return status_ == WAITING_ON_BARRIER;
  }

  // Obtains the view. Can be called only if there was no exception.
  const View& view() const { return *result_.get(); }

  // Obtains the exception. Can be called only if there was one.
  const Exception& exception() const { return result_.exception(); }

  // Obtains and releases the exception, if any. Ownership is passed to the
  // caller.
  Exception* release_exception() { return result_.release_exception(); }

 protected:
  ResultView(const View* view, ReturnCode status)
      : result_(supersonic::Success(view)),
        status_(status) {}

  explicit ResultView(Exception* exception)
      : result_(supersonic::Failure(exception)),
        status_(exception->return_code()) {}

  // For debugging missing exception checks in debug mode.
  void mark_checked() const {
    result_.mark_checked();
  }

  FailureOr<const View*> result_;
  ReturnCode status_;
  // Copyable.
};

inline const View& SucceedOrDie(ResultView result_view) {
  CHECK(!result_view.is_failure()) << result_view.exception().PrintStackTrace();
  return result_view.view();
}

class CursorTransformer;

class Cursor {
 public:
  static const rowcount_t kDefaultRowCount = 1024;

  virtual ~Cursor() {}

  // Returns the schema associated with this cursor.
  virtual const TupleSchema& schema() const = 0;

  int column_count() const { return schema().attribute_count(); }

  // Tries to advance cursor to a next block.
  // The implementation returns a ResultView that encapsulates data (between
  // one and max_row_count rows), or an Exception.
  // If there is no further rows to return, the result's has_data() is false.
  //
  // You typically call this function from some other cursor's Next().
  virtual ResultView Next(rowcount_t max_row_count) = 0;

  // Notifies the cursor that its results are no longer required. The cursor
  // is expected to interrupt processing, and should do so at its earliest
  // convenience. The cursor should signal premature termination by returning
  // the INTERRUPTED error from a call to Next().
  //
  // More specifically, if there is a pending call to Next() that is blocked
  // (e.g. on IO or on expensive computation), the implementation is expected
  // to interrupt it. Otherwise, the implementation should guarantee that
  // subsequent calls to Next() won't become blocked.
  //
  // The implementation needs to ensure that the method behaves correctly
  // when called from a different thread than the one calling Next() (including
  // during a pending call to Next()).
  // The call to Interrupt() is asynchronous and non-blocking. In particular,
  // if that call initiates any I/O or network communication, it must be
  // asynchronous and best-effort.
  //
  // Because of the inherent asynchrony, there is a race condition between an
  // interruption request and the stream running to completion. Thus, the
  // caller should not rely on the stream being interrupted; it may as
  // well happen that the stream successfully runs till EOS.
  //
  // IMPLEMENTATION NOTES.
  // * Leaf cursors, dealing with I/O, may need to interrupt pending I/O calls,
  //   and store the interruption status in an atomic_flag.
  //   Subsequent calls to Next() should check the status of that flag,
  //   returning the INTERRUPTED error when the flag is set.
  // * For non-leaf cursors, the desired behavior is almost always
  //   obtained simply by forwarding interruption requests to children.
  //   The convenience base class BasicCursor already does that, so if your
  //   cursor inherits from it, you probably don't need to do anything to
  //   support interruption. Exception are blocking operations, i.e. cursors
  //   that perform expensive computations w/o reading data from children; e.g.
  //   Sort. These cursors may need to capture the interruption status in an
  //   atomic_flag, and check it periodically, interrupting computations as
  //   necessary.
  virtual void Interrupt() = 0;

  // Helper for debugging; prints out the structure of this cursor.
  virtual void AppendDebugDescription(string* target) const = 0;

  // TODO(user): this is a temporary function to safely add splitters to
  // the API. The idea is to fail-fast rather than spin if one uses a cursor
  // for which handling barriers has not been implemented yet.
  // TODO(user): we should add some generic test cases to operation_testing.h
  // to verify that a cursor properly handles WAITING_ON_BARRIER, and other
  // common signals from its children.
  virtual bool IsWaitingOnBarrierSupported() const { return false; }

  // The function applies a CursorTransformer to all of the current cursor's
  // children, which allows for cursor tree (DAG) traversal. Default
  // implementation undertakes no action and issues a warning. It is up
  // to inheriting classes to launch the transformer (using the Transform
  // method) on their children. Depending on the situation transformation may
  // not be called on all children (for instance with splitter reader
  // cursors, in order to avoid it being called multiple times on the same
  // cursor). See comments in implementations for more details.
  //
  // The function should be called before any operations (especially Next) take
  // place, otherwise undefined behaviour may happen.
  virtual void ApplyToChildren(CursorTransformer* transformer) {
    string description;
    AppendDebugDescription(&description);
    DLOG(WARNING) << "No Transform() implementation for "
                  << description
                  << ", defaulting to no action.";
  }

  // All cursors, whose ids we would like to check, should override this method
  // to return a viable enum value corresponding to their types. An unknown
  // cursor id is returned by default.
  virtual CursorId GetCursorId() const { return UNKNOWN_ID; }

 protected:
  // To allow instantiation in subclasses.
  Cursor() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Cursor);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_BASE_CURSOR_H_
