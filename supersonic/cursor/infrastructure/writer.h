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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_WRITER_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_WRITER_H_

#include "supersonic/utils/macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/infrastructure/iterators.h"

namespace supersonic {

class Cursor;
class TupleSchema;
class View;

// An abstraction of a 'data sink' that can be written to. Implementations
// may write data to tables, sockets, files, etc.
// TODO(user): formalize schema contract of the sink - can views have
// different schemas? If not, perhaps the sink should have a new method for
// initialization, so that it can know the schema even if there is zero rows
// or data written to it.
class Sink {
 public:
  virtual ~Sink() {}

  // Writes a new chunk of data to the sink. Returns the number of rows
  // successfully written. Exception may be returned in case of an
  // unrecoverable error.
  virtual FailureOr<rowcount_t> Write(const View& data) = 0;

  // Finalizes writing data to the Sink. After a sink is finalized, Write can't
  // be called anymore. Returns an exception in case on an error.
  // Finalize must always be called before the object is destroyed.
  virtual FailureOrVoid Finalize() = 0;

 protected:
  Sink() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Sink);
};

// Writes a cursor to sinks.
class Writer {
 public:
  // Takes ownership of the cursor.
  explicit Writer(Cursor* cursor) : iterator_(cursor) {}

  // Attempts to write up-to max_row_count rows from the cursor into the
  // specified sink. If either the cursor iteration (i.e. a call to Next()) or
  // sink->Write() results in an exception, write fails and the exception is
  // propagated. Otherwise, write succeeds and returns the number of rows
  // actually written (which may be less than max_row_count). The caller should
  // then use is_eos() to test whether the input has been entirely written.
  //
  // If this method returns without an exception, the caller may call Write or
  // WriteAll again, to attempt writing the remaining rows, if any. The calls
  // may use different sinks.
  FailureOr<rowcount_t> Write(Sink* sink, rowcount_t max_row_count);

  // Attempts to write all remaining rows from the cursor into the specified
  // sink, as if calling Write with no bounds. If either the cursor iteration
  // (i.e. a call to Next()) or sink->Write() results in an exception,
  // write fails and the exception is propagated. Otherwise, write succeeds
  // and returns the number of rows actually written. The caller should then
  // use is_eos() to test whether the input has been entirely written, which
  // is not guaranteed to always be the case.
  //
  // If this method returns without an exception, the caller may call Write or
  // WriteAll again, to attempt writing the remaining rows, if any. The calls
  // may use different sinks.
  FailureOr<rowcount_t> WriteAll(Sink* sink);

  // Exposes the underlying schema.
  const TupleSchema& schema() const { return iterator_.schema(); }

  // Returns true if the cursor has been iterated successfully till EOS; false
  // otherwise (i.e. if there is more rows, or exception has occurred). If
  // this method returns true, all subsequent calls to Write and WriteAll
  // do nothing.
  bool is_eos() const { return iterator_.is_eos(); }

  // Returns true if the cursor has been iterated successfully till
  // WAITING_ON_BARRIER; false otherwise (i.e. there is more rows,
  // or exception has occurred). You can unlock the barrier in the network
  // (i.e. by pulling another cursor or adding more input data) and then
  // successfully call Write or WriteAll.
  bool is_waiting_on_barrier() const {
    return iterator_.is_waiting_on_barrier();
  }

  // Interrupts processing; propagates interruption to the underlying cursor.
  // Semantics similar to Cursor::Interrupt() - i.e. the sink may see the
  // terminated stream and INTERRUPTED error, but it might also run to
  // completion due to a race between cancellation and stream processing.
  void Interrupt() { iterator_.Interrupt(); }

  void ApplyToIterator(CursorTransformer* transformer) {
    iterator_.ApplyToCursor(transformer);
  }

 private:
  CursorIterator iterator_;
  DISALLOW_COPY_AND_ASSIGN(Writer);
};

// A convenience method to write the entire content of the cursor into the
// specified sink, or fail trying. Takes ownership (and deletes) the cursor.
FailureOrVoid WriteCursor(Cursor* cursor, Sink* sink);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_WRITER_H_
