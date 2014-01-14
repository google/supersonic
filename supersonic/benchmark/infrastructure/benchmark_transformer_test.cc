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

#include <memory>

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/benchmark/infrastructure/benchmark_transformer.h"
#include "supersonic/cursor/base/cursor_mock.h"
#include "supersonic/utils/pointer_vector.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace supersonic {

namespace {

using testing::ReturnRef;
using testing::StrictMock;
using testing::_;

using util::gtl::PointerVector;

// Template construction functions for cursor transformers with history.
template<typename T>
CursorTransformerWithVectorHistory<T>* CreateCursor();

template<>
CursorTransformerWithSimpleHistory* CreateCursor<CursorOwnershipRevoker>() {
  return PrintingSpyTransformer();
}

template<>
CursorTransformerWithBenchmarkHistory*
CreateCursor<CursorWithBenchmarkListener>() {
  return BenchmarkSpyTransformer();
}

// Template cursor pointer extraction from history entry.
template<typename T>
Cursor* ExtractCursor(T* entry);

// Extracts the original cursor from the ownership revoker.
template<>
Cursor* ExtractCursor<CursorOwnershipRevoker>(CursorOwnershipRevoker* entry) {
  return entry->original();
}

// Extracts cursor from the wrapper.
template<>
Cursor* ExtractCursor<CursorWithBenchmarkListener>(
    CursorWithBenchmarkListener* entry) {
  return entry->cursor();
}

template<typename T>
class CursorTransformerWithHistoryTest : public testing::Test {
 protected:
  CursorTransformerWithHistoryTest()
    : transformer_(CreateCursor<T>()) {}

  virtual MockCursor* Init(MockCursor* cursor) {
    // Spy cursors need schema for construction, passing a simple, blank
    // TupleSchema by default.
    ON_CALL(*cursor, schema())
        .WillByDefault(ReturnRef(schema_));

    EXPECT_CALL(*cursor, Interrupt()).Times(0);
    EXPECT_CALL(*cursor, Next(_)).Times(0);
    EXPECT_CALL(*cursor, AppendDebugDescription(_)).Times(1);
    EXPECT_CALL(*cursor, schema()).Times(1);
    return cursor;
  }

  // Uses a proper template function to extract the cursor from the history
  // entry.
  virtual Cursor* Extract(T* entry) {
    return ExtractCursor<T>(entry);
  }

  std::unique_ptr<CursorTransformerWithVectorHistory<T>> transformer_;
  TupleSchema schema_;
};

typedef testing::Types<CursorOwnershipRevoker, CursorWithBenchmarkListener>
EntryTypes;

TYPED_TEST_CASE(CursorTransformerWithHistoryTest, EntryTypes);

TYPED_TEST(CursorTransformerWithHistoryTest, CorrectHistory) {
  std::unique_ptr<Cursor> cursor1(this->Init(new StrictMock<MockCursor>));
  std::unique_ptr<Cursor> cursor2(this->Init(new StrictMock<MockCursor>));
  std::unique_ptr<Cursor> cursor3(this->Init(new StrictMock<MockCursor>));
  std::unique_ptr<Cursor> cursor4(this->Init(new StrictMock<MockCursor>));

  Cursor* cursor_p1 = cursor1.get();
  Cursor* cursor_p2 = cursor2.get();
  Cursor* cursor_p3 = cursor3.get();
  Cursor* cursor_p4 = cursor4.get();

  CursorTransformerWithVectorHistory<TypeParam>* transformer(
      this->transformer_.get());

  cursor1.reset(transformer->Transform(cursor1.release()));
  cursor2.reset(transformer->Transform(cursor2.release()));
  cursor3.reset(transformer->Transform(cursor3.release()));

  ASSERT_EQ(3, transformer->GetHistoryLength());
  EXPECT_EQ(cursor_p1, this->Extract(transformer->GetFirstEntry()));
  EXPECT_EQ(cursor_p1, this->Extract(transformer->GetEntryAt(0)));
  EXPECT_EQ(cursor_p2, this->Extract(transformer->GetEntryAt(1)));
  EXPECT_EQ(cursor_p3, this->Extract(transformer->GetEntryAt(2)));
  EXPECT_EQ(cursor_p3, this->Extract(transformer->GetLastEntry()));
  EXPECT_EQ(3, transformer->GetHistoryLength());

  transformer->CleanHistory();
  ASSERT_EQ(0, transformer->GetHistoryLength());

  cursor4.reset(transformer->Transform(cursor4.release()));
  ASSERT_EQ(1, transformer->GetHistoryLength());
  EXPECT_EQ(cursor_p4, this->Extract(transformer->GetFirstEntry()));
  EXPECT_EQ(cursor_p4, this->Extract(transformer->GetEntryAt(0)));
  EXPECT_EQ(cursor_p4, this->Extract(transformer->GetLastEntry()));

  transformer->CleanHistory();
  ASSERT_EQ(0, transformer->GetHistoryLength());
}

TYPED_TEST(CursorTransformerWithHistoryTest, ReleaseHistory) {
  std::unique_ptr<Cursor> cursor1(this->Init(new StrictMock<MockCursor>));
  std::unique_ptr<Cursor> cursor2(this->Init(new StrictMock<MockCursor>));
  std::unique_ptr<Cursor> cursor3(this->Init(new StrictMock<MockCursor>));

  Cursor* cursor_p1 = cursor1.get();
  Cursor* cursor_p2 = cursor2.get();
  Cursor* cursor_p3 = cursor3.get();

  CursorTransformerWithVectorHistory<TypeParam>* transformer(
      this->transformer_.get());

  cursor1.reset(transformer->Transform(cursor1.release()));
  cursor2.reset(transformer->Transform(cursor2.release()));
  cursor3.reset(transformer->Transform(cursor3.release()));

  ASSERT_EQ(3, transformer->GetHistoryLength());

  std::unique_ptr<PointerVector<TypeParam>> history(
      transformer->ReleaseHistory());
  ASSERT_EQ(cursor_p1, this->Extract((*history)[0].get()));
  ASSERT_EQ(cursor_p2, this->Extract((*history)[1].get()));
  ASSERT_EQ(cursor_p3, this->Extract((*history)[2].get()));

  ASSERT_EQ(0, transformer->GetHistoryLength());
}

}  // namespace

}  // namespace supersonic
