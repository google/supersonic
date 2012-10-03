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
// Author: tkaftal@google.com (Tomasz Kaftal)
//
// File contains a class for cursor transformers which maintain a history of
// transformations.
#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_HISTORY_TRANSFORMER_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_HISTORY_TRANSFORMER_H_

#include "supersonic/cursor/base/cursor_transformer.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

// A specialised CursorTransformer-based interface which stores a history
// of entries for transformed cursors and can be used, for instance,
// to traverse a cursor tree.
//
// An implementation of this class may use its own type of entry and provide
// custom memory management for entries.
template<typename T>
class CursorTransformerWithHistory : public CursorTransformer {
 public:
  // Returns the number of entries stored in the transformer's memory. A
  // transformation entry is stored once for each call to Run().
  virtual size_t GetHistoryLength() const = 0;

  // Retrieves the entry at position.
  virtual T GetEntryAt(size_t position) const = 0;

  // Retrieves the first entry currently in memory.
  virtual T GetFirstEntry() const = 0;

  // Retrieves the last entry currently in memory.
  virtual T GetLastEntry() const = 0;

  // Cleans the history. Does not clean the history objects, so it should only
  // be called when all history entries have been retrieved and have owners.
  virtual void CleanHistory() = 0;
};

typedef CursorTransformerWithHistory<Cursor*>
CursorTransformerWithSimpleHistory;

// An abstract class providing implementation for history utilities using an STL
// vector. The T class has to define a copy constructor.
template<typename T>
class CursorTransformerWithVectorHistory
    : public CursorTransformerWithHistory<T> {
 public:
  virtual size_t GetHistoryLength() const {
    return run_history_.size();
  }

  virtual T GetEntryAt(size_t position) const {
    CHECK_LT(position, run_history_.size());
    return run_history_[position];
  }

  virtual T GetFirstEntry() const {
    return GetEntryAt(0);
  }

  virtual T GetLastEntry() const {
    CHECK_GT(run_history_.size(), 0);
    return run_history_.back();
  }

  virtual void CleanHistory() {
    run_history_.clear();
  }

 protected:
  std::vector<T> run_history_;
};

}  // namespace supersonic
#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_HISTORY_TRANSFORMER_H_
