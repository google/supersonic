// Copyright 2010 Google Inc.  All Rights Reserved
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

#include "supersonic/base/memory/arena.h"

#include "gtest/gtest.h"

namespace supersonic {

class ArenaTest : public testing::Test {};

TEST_F(ArenaTest, ShouldHandleVisciousOverflows) {
  Arena arena(3, 3);
  StringPiece foo(arena.AddStringPieceContent("foo"), 3);
  // The length of '4' should outrule the max component size of 3.
  StringPiece barz(arena.AddStringPieceContent("barz"), 4);
  // Now, 3 items filling the next component completely.
  StringPiece a(arena.AddStringPieceContent("a"), 1);
  StringPiece b(arena.AddStringPieceContent("b"), 1);
  StringPiece c(arena.AddStringPieceContent("c"), 1);
  // Now, 3 items filling the next component and then overflowing.
  StringPiece d(arena.AddStringPieceContent("d"), 1);
  StringPiece e(arena.AddStringPieceContent("e"), 1);
  StringPiece bar(arena.AddStringPieceContent("bar"), 3);
  EXPECT_EQ("foo", foo);
  EXPECT_EQ("barz", barz);
  EXPECT_EQ("a", a);
  EXPECT_EQ("b", b);
  EXPECT_EQ("c", c);
  EXPECT_EQ("d", d);
  EXPECT_EQ("e", e);
  EXPECT_EQ("bar", bar);
}

TEST_F(ArenaTest, ShouldHandleResets) {
  Arena arena(10, 200);
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < i + 10; ++j) {
      arena.AddStringPieceContent("yada");
      arena.AddStringPieceContent("bar");
    }
    arena.Reset();
  }
  StringPiece foo(arena.AddStringPieceContent("foo"), 3);
  StringPiece barz(arena.AddStringPieceContent("barz"), 4);
  EXPECT_EQ("foo", foo);
  EXPECT_EQ("barz", barz);
}

TEST_F(ArenaTest, ShouldHandleResourceLimits) {
  MemoryLimit memory_limit(20);
  Arena arena(&memory_limit, 12, 50);
  EXPECT_TRUE(arena.AddStringPieceContent("foofoofoo"));  // 9 bytes.
  EXPECT_EQ(12, arena.memory_footprint());
  EXPECT_TRUE(arena.AddStringPieceContent("barbar"));  // Another 6 bytes.
  EXPECT_EQ(20, arena.memory_footprint());
  // Another 9 bytes - no go.
  EXPECT_FALSE(arena.AddStringPieceContent("catcatcat"));
}

}  // namespace supersonic
