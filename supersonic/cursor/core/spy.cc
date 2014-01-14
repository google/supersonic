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
// Author: ptab@google.com (Piotr Tabor)

#include "supersonic/cursor/core/spy.h"

#include <iosfwd>
#include <iostream>
#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/timer.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/history_transformer.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/cursor/infrastructure/view_printer.h"

namespace supersonic {

namespace {

const int64 kNumNanosPerMilli = kNumMicrosPerMilli * 1000;

class SpyCursor : public BasicCursor {
 public:
  SpyCursor(const string& id, SpyListener* listener, Cursor* child)
      : BasicCursor(child->schema(), child),
        id_(id),
        listener_(listener) {}

  virtual ~SpyCursor() {}

  virtual ResultView Next(rowcount_t max_row_count) {
    listener_->BeforeNext(id_, max_row_count);
    timer_.Restart();
    ResultView result_view = child()->Next(max_row_count);
    timer_.Stop();
    listener_->AfterNext(id_, max_row_count, result_view, timer_.GetInNanos());
    return result_view;
  }

  virtual bool IsWaitingOnBarrierSupported() const { return true; }

 private:
  const string id_;
  SpyListener* listener_;
  WallTimer timer_;
  DISALLOW_COPY_AND_ASSIGN(SpyCursor);
};

class SpyOperation : public BasicOperation {
 public:
  SpyOperation(const string id, SpyListener* listener, Operation* child)
      : BasicOperation(child),
        id_(id),
        listener_(listener) {}

  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> bound_child(child()->CreateCursor());
    PROPAGATE_ON_FAILURE(bound_child);
    return Success(BoundSpy(id_, listener_, bound_child.release()));
  }

 private:
  const string id_;
  SpyListener* listener_;
  DISALLOW_COPY_AND_ASSIGN(SpyOperation);
};

class SpySink : public Sink {
 public:
  // Takes ownership of child.
  SpySink(const string id, SpyListener* listener, Sink* sink)
      : sink_(sink),
        id_(id),
        listener_(listener) {}

  virtual ~SpySink() {}

  virtual FailureOr<rowcount_t> Write(const View& data) {
    listener_->BeforeNext(id_, 0);
    timer_.Restart();
    FailureOr<rowcount_t> result = sink_->Write(data);
    timer_.Stop();
    listener_->AfterNext(id_, 0,
                         ResultView::Success(&data), timer_.GetInNanos());
    return result;
  }

  virtual FailureOrVoid Finalize() { return sink_->Finalize(); }

 private:
  std::unique_ptr<Sink> sink_;
  string id_;
  SpyListener* listener_;
  WallTimer timer_;
  DISALLOW_COPY_AND_ASSIGN(SpySink);
};

// An implementation of the CursorTransformerWithHistory which wraps
// the argument cursor into a spy cursor. Spy cursor's id is derived
// from the wrapped cursor's debug description. Cursors passed to Transform
// are stored in history.
//
// The ownership revoker wrapper is used in order to avoid double destruction
// of the stored cursors as the history objects take ownership of their entries.
class SpyCursorSimpleTransformer
    : public CursorTransformerWithVectorHistory<CursorOwnershipRevoker> {
 public:
  explicit SpyCursorSimpleTransformer(SpyListener* listener)
      : CursorTransformerWithVectorHistory<CursorOwnershipRevoker>(),
        listener_(listener) {}

  // Does not take ownership of the argument cursor, but the created
  // SpyCursor does. Should not be used once the transformed cursors have been
  // destroyed.
  //
  // Uses the spy listener stored on construction.
  virtual Cursor* Transform(Cursor* cursor) {
    string id;
    cursor->AppendDebugDescription(&id);
    run_history_->push_back(new CursorOwnershipRevoker(cursor));
    return new SpyCursor(id, listener_, cursor);
  }
 private:
  SpyListener* listener_;
};

}  // namespace

Operation* Spy(const string& id, SpyListener* listener, Operation* source) {
  return new SpyOperation(id, listener, source);
}

Sink* Spy(const string& id, SpyListener* listener, Sink* sink) {
  return new SpySink(id, listener, sink);
}

Cursor* BoundSpy(const string& id, SpyListener* listener, Cursor* child) {
  return new SpyCursor(id, listener, child);
}

CursorTransformerWithSimpleHistory* PrintingSpyTransformer() {
  return new SpyCursorSimpleTransformer(PrintingSpyListener());
}

// TODO(ptab): Needs pretty formating that will indent nested next's.
class PrintSpyListener : public SpyListener {
 public:
  PrintSpyListener() : view_printer_(true, true, 10) {}

  virtual ~PrintSpyListener() {}

  // Returns a singleton instance of the PrintSpyListener.
  static PrintSpyListener* Get() {
    static PrintSpyListener listener;
    return &listener;
  }

  virtual void BeforeNext(const string& id, rowcount_t max_row_count) {
    std::cout << "Calling Next(" << max_row_count << ") on " << id << ":\n";
  }

  virtual void AfterNext(const string& id,
                         rowcount_t max_row_count,
                         const ResultView& result_view,
                         int64 time_nanos) {
    double time_ms = time_nanos / static_cast<double>(kNumNanosPerMilli);
    std::cout << "Next(" << max_row_count << ") on " << id
              << " result in " << time_ms << " ms:";
    view_printer_.AppendResultViewToStream(result_view, &std::cout);
  }

 private:
  ViewPrinter view_printer_;
  DISALLOW_COPY_AND_ASSIGN(PrintSpyListener);
};

SpyListener* PrintingSpyListener() {
  return PrintSpyListener::Get();
}

Operation* SpyPrinter(const string& id, Operation* source) {
  return Spy(id, PrintSpyListener::Get(), source);
}

Cursor* BoundSpyPrinter(const string& id, Cursor* child) {
  return BoundSpy(id, PrintSpyListener::Get(), child);
}

}  // namespace supersonic
