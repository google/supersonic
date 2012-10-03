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
// Cursor that allows to restrict which columns from input cursor are returned
// and to rearange the order of columns.

#ifndef SUPERSONIC_CURSOR_CORE_PROJECT_H_
#define SUPERSONIC_CURSOR_CORE_PROJECT_H_

namespace supersonic {

class BoundSingleSourceProjector;
class Cursor;
class Operation;
class SingleSourceProjector;

// Creates project operation. Takes ownership of the projector and the child.
Operation* Project(const SingleSourceProjector* projector,
                   Operation* child);

// Creates project cursor that returns all rows from child cursor but with
// columns restricted to these specified by projector. Takes ownership of
// the projector and the child.
Cursor* BoundProject(const BoundSingleSourceProjector* projector,
                     Cursor* child);

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_PROJECT_H_
