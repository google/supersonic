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
// A cursor transformer interface providing a method to transform input cursors
// into possibly different output cursors.

#ifndef SUPERSONIC_CURSOR_BASE_CURSOR_TRANSFORMER_H_
#define SUPERSONIC_CURSOR_BASE_CURSOR_TRANSFORMER_H_

#include "supersonic/utils/macros.h"

namespace supersonic {

class Cursor;

class CursorTransformer {
 public:
  virtual ~CursorTransformer() { }

  // Runs the transformer on the specified cursor. The caller takes ownership
  // of the returned cursor and relinquishes ownership of the argument.
  // The result cursor may or may not be the same as the argument.
  // See CursorTransformer implementations for more details.
  virtual Cursor* Transform(Cursor* cursor) ABSTRACT;

 protected:
  // To allow instantiation in subclasses.
  CursorTransformer() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(CursorTransformer);
};

}  // namespace supersonic
#endif  // SUPERSONIC_CURSOR_BASE_CURSOR_TRANSFORMER_H_
