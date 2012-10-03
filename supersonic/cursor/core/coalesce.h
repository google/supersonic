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
//
// Cursor that has all attributes of its child cursors.

#ifndef SUPERSONIC_CURSOR_CORE_COALESCE_H_
#define SUPERSONIC_CURSOR_CORE_COALESCE_H_

#include <vector>
using std::vector;

#include "supersonic/base/exception/result.h"

namespace supersonic {

class Cursor;

// Creates a coalesce cursor that has all attributes of all child cursors.
// Returns an Exception if the schemata of the child cursors contain
// duplicated attribute names. Does not check if the children vector contains
// only valid (non-NULL) pointers, and can fail at runtime if it doesn't.
// If successful, takes ownership of the child cursors.
FailureOrOwned<Cursor> BoundCoalesce(const vector<Cursor*>& children);

// TODO(user): Add the following functions:
//  BoundCoalesceUsingProjector(const BoundMultiSourceProjector*, ...)
//  Operation* Coalesce(const vector<Operation*>& children)
//  Operation* CoalesceUsingProjector(const BoundMultiSourceProjector*, ...)

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_COALESCE_H_
