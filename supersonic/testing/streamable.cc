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

#include "supersonic/testing/streamable.h"

#include <sstream>

namespace supersonic {

std::ostream& operator<<(std::ostream& s, const Streamable& streamable) {
  streamable.AppendToStream(&s);
  return s;
}

std::ostream& operator<<(std::ostream& s, const Streamable* streamable) {
  streamable->AppendToStream(&s);
  return s;
}

string StreamableToString(const Streamable& streamable) {
  std::ostringstream s;
  streamable.AppendToStream(&s);
  return s.str();
}

}  // namespace supersonic
