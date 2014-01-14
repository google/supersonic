// Copyright 2012 Google Inc.  All Rights Reserved
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
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// Implementation of benchmarking spy cursors.

#include "supersonic/benchmark/infrastructure/benchmark_transformer.h"

#include <memory>

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/cursor/base/cursor.h"

namespace supersonic {

namespace {

// An implementation of the CursorTransformer which wraps the argument cursor
// into a spy cursor with a benchmark spy listener. Spy cursor's id is derived
// from the wrapped cursor's debug description. Provides a history composed
// of CursorWithBenchmarkListener objects.
class SpyCursorBenchmarkTransformer
: public CursorTransformerWithVectorHistory<CursorWithBenchmarkListener> {
 public:
  // Does not take ownership of cursor, but the created SpyCursor does. Also,
  // the transformer will store entries containing pointers to the created
  // cursors which should not be used, when the cursors have been destroyed.
  //
  // The transformer also creates a new BenchmarkListener which becomes
  // available to retrieve from the history. The entry inserted into the history
  // will take ownership of the listener, but not of the cursor. The transformer
  // takes ownership of the entries.
  virtual Cursor* Transform(Cursor* cursor) {
    string id;
    cursor->AppendDebugDescription(&id);
    std::unique_ptr<BenchmarkListener> listener(CreateBenchmarkListener());
    BenchmarkListener* ptr_to_listener = listener.get();
    run_history_->push_back(
        new CursorWithBenchmarkListener(cursor, listener.release()));
    return BoundSpy(id, ptr_to_listener, cursor);
  }
};

}  // namespace

CursorTransformerWithBenchmarkHistory* BenchmarkSpyTransformer() {
  return new SpyCursorBenchmarkTransformer();
}

}  // namespace supersonic
