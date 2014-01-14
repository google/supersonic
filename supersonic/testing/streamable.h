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

#ifndef SUPERSONIC_TESTING_STREAMABLE_H_
#define SUPERSONIC_TESTING_STREAMABLE_H_

#include <iosfwd>
#include <ostream>
#include <string>
namespace supersonic {using std::string; }

namespace supersonic {

// Common interface for wrappers that produce verbose string representations
// of their complex underyling supersonic objects such as Cursors, Views, ...
// when used in gUnit assertions; eg.
//
// class ComparableView : public Streamable { ... };
//
// View a1_b2_, a1_a2_;
// EXPECT_EQ(ComparableView(a1_b2_), ComparableView(a1_a2_));
//
// produces:
//
// Value of: ComparableView(a1_b2_)
// Actual: View; rows: 2; schema: STRING "col0", INT64 "col1"
//   a, NULL
//   b, 2
//
// Expected: ComparableView(a1_a2_)
// Which is: View; rows: 2; schema: STRING "col0", INT64 "col1"
//   a, NULL
//   a, 2


class Streamable {
 public:
  virtual ~Streamable() {}
  virtual void AppendToStream(std::ostream* s) const = 0;
};

std::ostream& operator<<(std::ostream& s, const Streamable& streamable);
std::ostream& operator<<(std::ostream& s, const Streamable* streamable);

string StreamableToString(const Streamable& streamable);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_STREAMABLE_H_
