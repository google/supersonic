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
// Mock cursor class for use in testing.

#ifndef SUPERSONIC_CURSOR_BASE_CURSOR_MOCK_H_
#define SUPERSONIC_CURSOR_BASE_CURSOR_MOCK_H_

#include "supersonic/cursor/base/cursor.h"
#include "supersonic/utils/macros.h"

#include "gmock/gmock.h"

namespace supersonic {

class MockCursor : public Cursor {
 public:
  MOCK_METHOD0(Interrupt, void());
  MOCK_CONST_METHOD0(schema, const TupleSchema&());
  MOCK_CONST_METHOD0(GetCursorId, CursorId());
  MOCK_METHOD1(Next, ResultView(rowcount_t max_row_count));
  MOCK_CONST_METHOD1(AppendDebugDescription, void(string* target));
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_BASE_CURSOR_MOCK_H_
