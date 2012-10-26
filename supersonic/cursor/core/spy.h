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
//
// Special operators/cursors that allow to spy (sniff) all the data
// (data itself, execution time or returned status code) sent between
// to cursors.

#ifndef SUPERSONIC_CURSOR_CORE_SPY_H_
#define SUPERSONIC_CURSOR_CORE_SPY_H_

#include <string>
using std::string;

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/infrastructure/history_transformer.h"

namespace supersonic {

class Cursor;
class Operation;
class ResultView;
class Sink;

// Spies on cursor: calls these methods before and after every call to
// child->next.
class SpyListener {
 public:
  virtual ~SpyListener() {}
  virtual void BeforeNext(const string& id, rowcount_t max_row_count) = 0;
  virtual void AfterNext(const string& id,
                         rowcount_t max_row_count,
                         const ResultView& result_view,
                         int64 time_cycles) = 0;
};

// Returns a spy listener which will print the measured information and view
// contents to the output.
SpyListener* PrintingSpyListener();

// Spy wrapping printing cursor transformer.
CursorTransformerWithSimpleHistory* PrintingSpyTransformer();

// This operation will call listener's BeforeNext method
// every time it is asked for more data. Then it delegates the call to
// the source and call AfterNext on the listener.
// Parameter "id" is used to distinguish different operators that can share
// the same listener. The 'id' is forwarded to listener with every call.
// Takes ownership of the source. Does not take ownership of the listener.
Operation* Spy(const string& id, SpyListener* listener, Operation* source);

// Spy around Sink. See comments about spy for operations above.
// Will call BeforeNext() before sink write, and AfterNext()
// after Sink write. For sinks the max_row_count parameter of the
// calls is set to 0.
Sink* Spy(const string& id, SpyListener* listener, Sink* sink);

// Takes ownership of the child. Does not take ownership of the listener.
Cursor* BoundSpy(const string& id, SpyListener* listener, Cursor* child);

// SpyPrinter is a cursor that dumps to the standard output all information
// acquired from given source. It presents the same set of data as source.
// Takes ownership of the source.
Operation* SpyPrinter(const string& id, Operation* source);

// Takes ownership of the source.
Cursor* BoundSpyPrinter(const string& id, Cursor* child);

#define SPY_CURSOR(cursor) \
  BoundSpyPrinter(#cursor, cursor)

#define SPY_OPERATION(operation) \
  SpyPrinter(#operation, operation)

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_SPY_H_
