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
// CursorTransformer interface extension for benchmarking performance.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_TRANSFORMER_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_TRANSFORMER_H_

#include <memory>

#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/infrastructure/history_transformer.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

class BenchmarkListener;

// A non-copiable class for storing pointers to cursors and listener objects
// which gather their operation statistics. The listeners are owned by
// the object, cursors on the other hand are not, as they are most likely
// already owned by either other cursors, or some other entity.
class CursorWithBenchmarkListener {
 public:
  CursorWithBenchmarkListener() : cursor_(NULL) {}

  // Takes ownership of the listener, does not take ownership of the cursor.
  CursorWithBenchmarkListener(Cursor* cursor, BenchmarkListener* listener)
      : listener_(listener),
        cursor_(cursor) {}

  // Simple cursor pointer getter - the object does not own the cursor itself.
  Cursor* cursor() const { return cursor_; }

  // Non-releasing getter for the benchmark listener.
  BenchmarkListener* listener() const { return listener_.get(); }

  // Releasing getter for the benchmark listener.
  BenchmarkListener* release_listener() { return listener_.release(); }

 private:
  std::unique_ptr<BenchmarkListener> listener_;
  Cursor* cursor_;

  DISALLOW_COPY_AND_ASSIGN(CursorWithBenchmarkListener);
};

typedef CursorTransformerWithVectorHistory<CursorWithBenchmarkListener>
CursorTransformerWithBenchmarkHistory;

// Spy wrapping benchmark cursor transformer.
CursorTransformerWithBenchmarkHistory* BenchmarkSpyTransformer();

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_TRANSFORMER_H_
