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

#include "supersonic/cursor/infrastructure/row_hash_set.h"

#include <unordered_set>
#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <vector>
using std::vector;

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/row.h"
#include "supersonic/utils/strings/numbers.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"
#include "supersonic/utils/hash/hash.h"

namespace supersonic {

namespace row_hash_set {

class RowHashSetTest : public testing::Test {
 public:
  void SetUp() {
    row_hash_set_block_schema_.add_attribute(
        Attribute("c1", INT64, NULLABLE));
    row_hash_set_block_schema_.add_attribute(
        Attribute("c2", STRING, NULLABLE));

    row_hash_set_.reset(new RowHashSet(row_hash_set_block_schema_,
                                       HeapBufferAllocator::Get()));
    row_multi_set_.reset(new RowHashMultiSet(row_hash_set_block_schema_,
                                             HeapBufferAllocator::Get()));
    row_hash_set_result_.reset(new FindResult(10000));
    row_multi_set_result_.reset(new FindMultiResult(10000));

    query_1_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .Build());

    query_11_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .AddRow(1, "one")
        .Build());

    query_1122_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .AddRow(1, "one")
        .AddRow(2, "two")
        .AddRow(2, "two")
        .Build());

    query_24680_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(2, "two")
        .AddRow(4, "four")
        .AddRow(6, "six")
        .AddRow(8, "eight")
        .AddRow(0, "zero")
        .Build());

    query_1234567890_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .AddRow(2, "two")
        .AddRow(3, "three")
        .AddRow(4, "four")
        .AddRow(5, "five")
        .AddRow(6, "six")
        .AddRow(7, "seven")
        .AddRow(8, "eight")
        .AddRow(9, "nine")
        .AddRow(0, "zero")
        .Build());

    query_1oNoNo1N1N_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .AddRow(__, "one")
        .AddRow(__, "one")
        .AddRow(1, __)
        .AddRow(1, __)
        .Build());

    query_1o2o1t2t_.reset(
        BlockBuilder<INT64, STRING>()
        .AddRow(1, "one")
        .AddRow(2, "one")
        .AddRow(1, "two")
        .AddRow(2, "two")
        .Build());

    BlockBuilder<INT64, STRING> query_1k_rows_builder;
    for (size_t i = 0; i < 1000; i++)
      query_1k_rows_builder.AddRow(i % 2, SimpleItoa(i % 2));
    query_1k_rows_.reset(query_1k_rows_builder.Build());
  }

  static bool RowIdSetHasElements(
      const vector<rowid_t>& elements, RowIdSetIterator* it) {
    std::unordered_multiset<rowid_t> elements_set(elements.begin(), elements.end());
    for (; !it->AtEnd(); it->Next()) {
      std::unordered_multiset<rowid_t>::iterator it_set = elements_set.find(it->Get());
      if (it_set != elements_set.end())
        elements_set.erase(it_set);
      else
        return false;
    }
    return elements_set.empty();
  }

  TupleSchema row_hash_set_block_schema_;

  std::unique_ptr<RowHashSet> row_hash_set_;
  std::unique_ptr<RowHashMultiSet> row_multi_set_;

  std::unique_ptr<FindResult> row_hash_set_result_;
  std::unique_ptr<FindMultiResult> row_multi_set_result_;

  const View& query_1() const          { return query_1_->view(); }
  const View& query_11() const         { return query_11_->view(); }
  const View& query_1122() const       { return query_1122_->view(); }
  const View& query_24680() const      { return query_24680_->view(); }
  const View& query_1234567890() const { return query_1234567890_->view(); }
  const View& query_1oNoNo1N1N() const { return query_1oNoNo1N1N_->view(); }
  const View& query_1o2o1t2t() const   { return query_1o2o1t2t_->view(); }
  const View& query_1k_rows() const    { return query_1k_rows_->view(); }

 private:
  std::unique_ptr<Block> query_1_;
  std::unique_ptr<Block> query_11_;
  std::unique_ptr<Block> query_1122_;
  std::unique_ptr<Block> query_24680_;
  std::unique_ptr<Block> query_1234567890_;
  std::unique_ptr<Block> query_1oNoNo1N1N_;
  std::unique_ptr<Block> query_1o2o1t2t_;
  std::unique_ptr<Block> query_1k_rows_;
};

TEST_F(RowHashSetTest,
       RowHashSetFindOneRowNegative) {
  row_hash_set_->Find(query_1(), row_hash_set_result_.get());
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_->size());
}

TEST_F(RowHashSetTest,
       RowHashSetInsertOneRow) {
  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 0));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertThenFindOneRow) {
  // Verify that before insertion, Find doesn't find the row.
  row_hash_set_->Find(query_1(), row_hash_set_result_.get());
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(0));

  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 0));

  row_hash_set_->Find(query_1(), row_hash_set_result_.get());
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertDuplicateKeyReturnsExistingRow) {
  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_NE(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 1));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertTwoIdenticalKeysOnlyInsertsOne) {
  EXPECT_EQ(query_11().row_count(),
            row_hash_set_->Insert(query_11(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_11(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_NE(Row(query_11(), 0), Row(row_hash_set_->indexed_view(), 1));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertManyRowsOnlyInsertsEachNewKeyOnce) {
  EXPECT_EQ(query_11().row_count(),
            row_hash_set_->Insert(query_11(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_->size());

  EXPECT_EQ(query_1122().row_count(),
            row_hash_set_->Insert(query_1122(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));
  EXPECT_EQ(2, row_hash_set_->size());

  EXPECT_EQ(Row(query_1122(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_hash_set_->indexed_view(), 1));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertWithoutResultVector) {
  EXPECT_EQ(query_11().row_count(), row_hash_set_->Insert(query_11()));
  EXPECT_EQ(1, row_hash_set_->size());

  EXPECT_EQ(query_1122().row_count(), row_hash_set_->Insert(query_1122()));
  EXPECT_EQ(2, row_hash_set_->size());

  EXPECT_EQ(Row(query_1122(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_hash_set_->indexed_view(), 1));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertFiveThenFindTenOnlyFindsFive) {
  EXPECT_EQ(query_24680().row_count(),
            row_hash_set_->Insert(query_24680(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_result_->Result(1));
  EXPECT_EQ(2, row_hash_set_result_->Result(2));
  EXPECT_EQ(3, row_hash_set_result_->Result(3));
  EXPECT_EQ(4, row_hash_set_result_->Result(4));
  EXPECT_EQ(5, row_hash_set_->size());
  EXPECT_EQ(Row(query_24680(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_24680(), 1), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_24680(), 2), Row(row_hash_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_24680(), 3), Row(row_hash_set_->indexed_view(), 3));
  EXPECT_EQ(Row(query_24680(), 4), Row(row_hash_set_->indexed_view(), 4));

  row_hash_set_->Find(query_1234567890(), row_hash_set_result_.get());
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(4));
  EXPECT_EQ(2, row_hash_set_result_->Result(5));
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(6));
  EXPECT_EQ(3, row_hash_set_result_->Result(7));
  EXPECT_EQ(kInvalidRowId, row_hash_set_result_->Result(8));
  EXPECT_EQ(4, row_hash_set_result_->Result(9));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertNullRows) {
  EXPECT_TRUE(
      row_hash_set_->Insert(query_1oNoNo1N1N(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(2, row_hash_set_result_->Result(3));
  EXPECT_EQ(2, row_hash_set_result_->Result(4));
  EXPECT_EQ(3, row_hash_set_->size());
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 1), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 2), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 3), Row(row_hash_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 4), Row(row_hash_set_->indexed_view(), 2));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertThenFindRowsWithNulls) {
  EXPECT_TRUE(
      row_hash_set_->Insert(query_1oNoNo1N1N(), row_hash_set_result_.get()));
  row_hash_set_->Find(query_1oNoNo1N1N(), row_hash_set_result_.get());
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(2, row_hash_set_result_->Result(3));
  EXPECT_EQ(2, row_hash_set_result_->Result(4));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertPartiallyUnequalRows) {
  EXPECT_TRUE(
      row_hash_set_->Insert(query_1o2o1t2t(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_result_->Result(1));
  EXPECT_EQ(2, row_hash_set_result_->Result(2));
  EXPECT_EQ(3, row_hash_set_result_->Result(3));

  EXPECT_EQ(4, row_hash_set_->size());
  EXPECT_EQ(Row(query_1o2o1t2t(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1o2o1t2t(), 1), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1o2o1t2t(), 2), Row(row_hash_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1o2o1t2t(), 3), Row(row_hash_set_->indexed_view(), 3));
}

TEST_F(RowHashSetTest,
       RowHashSetWithKeySelectorInsertPartiallyUnequalRows) {
  const BoundSingleSourceProjector* key_selector =
      SucceedOrDie(CompoundSingleSourceProjector()
                   .add(ProjectAttributeAt(1))
                   ->Bind(row_hash_set_block_schema_));
  RowHashSet row_hash_set(row_hash_set_block_schema_,
                          HeapBufferAllocator::Get(),
                          key_selector);
  EXPECT_TRUE(
      row_hash_set.Insert(query_1o2o1t2t(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));

  EXPECT_EQ(2, row_hash_set.size());
  EXPECT_EQ(Row(query_1o2o1t2t(), 0), Row(row_hash_set.indexed_view(), 0));
  EXPECT_EQ(Row(query_1o2o1t2t(), 2), Row(row_hash_set.indexed_view(), 1));

  View query_oott(TupleSchema::Singleton("c1", STRING, NOT_NULLABLE));
  key_selector->Project(query_1o2o1t2t(), &query_oott);
  query_oott.set_row_count(query_1o2o1t2t().row_count());
  row_hash_set.Find(query_oott, row_hash_set_result_.get());
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertBigQuery) {
  for (size_t j = 0; j < 10; j++) {
    EXPECT_TRUE(
        row_hash_set_->Insert(query_1k_rows(), row_hash_set_result_.get()));
    for (size_t i = 0; i < 1000; i++)
      EXPECT_EQ(i % 2, row_hash_set_result_->Result(i))
          << "j = " << j << " i = " << i;

    EXPECT_EQ(2, row_hash_set_->size());
    for (size_t i = 0; i < 1000; i++) {
      EXPECT_EQ(Row(query_1k_rows(), i),
                Row(row_hash_set_->indexed_view(), i % 2))
          << "j = " << j << " i = " << i;
    }
  }
}

TEST_F(RowHashSetTest,
       RowHashSetReset) {
  EXPECT_EQ(query_24680().row_count(),
            row_hash_set_->Insert(query_24680(), row_hash_set_result_.get()));
  EXPECT_EQ(5, row_hash_set_->size());
  row_hash_set_->Clear();
  EXPECT_EQ(0, row_hash_set_->size());

  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 0));
}

TEST_F(RowHashSetTest,
       RowHashSetInsertTwoIdenticalRowsFollowedByOtherTwoIdenticalRowsTwice) {
  EXPECT_EQ(query_1122().row_count(),
            row_hash_set_->Insert(query_1122(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));
  EXPECT_EQ(Row(query_1122(), 0), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1122(), 1), Row(row_hash_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1122(), 3), Row(row_hash_set_->indexed_view(), 1));
  EXPECT_EQ(2, row_hash_set_->size());

  EXPECT_EQ(query_1122().row_count(),
            row_hash_set_->Insert(query_1122(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(0, row_hash_set_result_->Result(1));
  EXPECT_EQ(1, row_hash_set_result_->Result(2));
  EXPECT_EQ(1, row_hash_set_result_->Result(3));
  EXPECT_EQ(2, row_hash_set_->size());
}

TEST_F(RowHashSetTest,
       RowHashMultiSetFindOneRowNegative) {
  row_multi_set_->Find(query_1(), row_multi_set_result_.get());
  EXPECT_TRUE(row_multi_set_result_->Result(0).AtEnd());
  EXPECT_EQ(0, row_multi_set_->size());
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertOneRow) {
  EXPECT_EQ(query_1().row_count(),
            row_multi_set_->Insert(query_1(), row_multi_set_result_.get()));
  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
  EXPECT_EQ(1, row_multi_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_multi_set_->indexed_view(), 0));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertThenFindOneRow) {
  // Verify that before insertion, Find doesn't find the row.
  row_multi_set_->Find(query_1(), row_multi_set_result_.get());
  EXPECT_TRUE(row_multi_set_result_->Result(0).AtEnd());

  EXPECT_EQ(query_1().row_count(),
            row_multi_set_->Insert(query_1(), row_multi_set_result_.get()));
  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
  EXPECT_EQ(1, row_multi_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_multi_set_->indexed_view(), 0));

  row_multi_set_->Find(query_1(), row_multi_set_result_.get());
  it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertTwoIdenticalKeysInsertsBoth) {
  EXPECT_EQ(query_11().row_count(),
            row_multi_set_->Insert(query_11(), row_multi_set_result_.get()));

  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0, 1), &it));

  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0, 1), &it));

  EXPECT_EQ(2, row_multi_set_->size());
  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 1));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertManyRowsInsertsAll) {
  EXPECT_EQ(query_11().row_count(),
            row_multi_set_->Insert(query_11(), row_multi_set_result_.get()));
  EXPECT_EQ(2, row_multi_set_->size());

  EXPECT_EQ(query_1122().row_count(),
            row_multi_set_->Insert(query_1122(), row_multi_set_result_.get()));

  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0, 1, 2, 3), &it));
  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0, 1, 2, 3), &it));

  it = row_multi_set_result_->Result(2);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(4, 5), &it));
  it = row_multi_set_result_->Result(3);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(4, 5), &it));

  EXPECT_EQ(6, row_multi_set_->size());
  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1122(), 0), Row(row_multi_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1122(), 0), Row(row_multi_set_->indexed_view(), 3));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_multi_set_->indexed_view(), 4));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_multi_set_->indexed_view(), 5));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertWithoutResultVector) {
  EXPECT_EQ(query_11().row_count(), row_multi_set_->Insert(query_11()));
  EXPECT_EQ(2, row_multi_set_->size());

  EXPECT_EQ(query_1122().row_count(), row_multi_set_->Insert(query_1122()));
  EXPECT_EQ(6, row_multi_set_->size());

  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_11(), 0), Row(row_multi_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1122(), 0), Row(row_multi_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1122(), 0), Row(row_multi_set_->indexed_view(), 3));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_multi_set_->indexed_view(), 4));
  EXPECT_EQ(Row(query_1122(), 2), Row(row_multi_set_->indexed_view(), 5));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertNullRows) {
  EXPECT_EQ(
      query_1oNoNo1N1N().row_count(),
      row_multi_set_->Insert(query_1oNoNo1N1N(), row_multi_set_result_.get()));
  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1, 2), &it));
  it = row_multi_set_result_->Result(2);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1, 2), &it));
  it = row_multi_set_result_->Result(3);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3, 4), &it));
  it = row_multi_set_result_->Result(4);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3, 4), &it));
  EXPECT_EQ(5, row_multi_set_->size());
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 0), Row(row_multi_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 1), Row(row_multi_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 2), Row(row_multi_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 3), Row(row_multi_set_->indexed_view(), 3));
  EXPECT_EQ(Row(query_1oNoNo1N1N(), 4), Row(row_multi_set_->indexed_view(), 4));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertThenFindRowsWithNulls) {
  EXPECT_TRUE(
      row_multi_set_->Insert(query_1oNoNo1N1N(), row_multi_set_result_.get()));
  row_multi_set_->Find(query_1oNoNo1N1N(), row_multi_set_result_.get());
  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1, 2), &it));
  it = row_multi_set_result_->Result(2);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1, 2), &it));
  it = row_multi_set_result_->Result(3);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3, 4), &it));
  it = row_multi_set_result_->Result(4);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3, 4), &it));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertPartiallyUnequalRows) {
  EXPECT_EQ(query_1o2o1t2t().row_count(),
            row_multi_set_->Insert(query_1o2o1t2t(), bool_ptr(NULL),
                                   row_multi_set_result_.get()));
  RowIdSetIterator it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0), &it));
  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1), &it));
  it = row_multi_set_result_->Result(2);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(2), &it));
  it = row_multi_set_result_->Result(3);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3), &it));

  EXPECT_EQ(4, row_multi_set_->size());
  EXPECT_EQ(Row(query_1o2o1t2t(), 0), Row(row_multi_set_->indexed_view(), 0));
  EXPECT_EQ(Row(query_1o2o1t2t(), 1), Row(row_multi_set_->indexed_view(), 1));
  EXPECT_EQ(Row(query_1o2o1t2t(), 2), Row(row_multi_set_->indexed_view(), 2));
  EXPECT_EQ(Row(query_1o2o1t2t(), 3), Row(row_multi_set_->indexed_view(), 3));

  EXPECT_EQ(query_1o2o1t2t().row_count(),
            row_multi_set_->Insert(query_1o2o1t2t(), bool_ptr(NULL),
                                   row_multi_set_result_.get()));
  it = row_multi_set_result_->Result(0);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(0, 4), &it));
  it = row_multi_set_result_->Result(1);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(1, 5), &it));
  it = row_multi_set_result_->Result(2);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(2, 6), &it));
  it = row_multi_set_result_->Result(3);
  EXPECT_TRUE(RowIdSetHasElements(util::gtl::Container(3, 7), &it));

  EXPECT_EQ(8, row_multi_set_->size());
  EXPECT_EQ(Row(query_1o2o1t2t(), 0), Row(row_multi_set_->indexed_view(), 4));
  EXPECT_EQ(Row(query_1o2o1t2t(), 1), Row(row_multi_set_->indexed_view(), 5));
  EXPECT_EQ(Row(query_1o2o1t2t(), 2), Row(row_multi_set_->indexed_view(), 6));
  EXPECT_EQ(Row(query_1o2o1t2t(), 3), Row(row_multi_set_->indexed_view(), 7));
}

TEST_F(RowHashSetTest,
       RowHashMultiSetInsertBigQuery) {
  for (int j = 1; j <= 10; j++) {
    EXPECT_EQ(query_1k_rows().row_count(),
              row_multi_set_->Insert(query_1k_rows(),
                                     row_multi_set_result_.get()));
    EXPECT_EQ(j * 1000, row_multi_set_->size());

    RowIdSetIterator it_even = row_multi_set_result_->Result(0);
    for (int i = 2; i < 1000; i += 2) {
      RowIdSetIterator it = row_multi_set_result_->Result(i);
      EXPECT_EQ(it_even.Get(), it.Get()) << "j = " << j << " i = " << i;
    }

    RowIdSetIterator it_odd = row_multi_set_result_->Result(1);
    for (int i = 3; i < 1000; i += 2) {
      RowIdSetIterator it = row_multi_set_result_->Result(i);
      EXPECT_EQ(it_odd.Get(), it.Get()) << "j = " << j << " i = " << i;
    }

    rowcount_t count;
    rowid_t current, previous;
    for (count = 0, previous = kInvalidRowId;
         !it_even.AtEnd();
         it_even.Next(), previous = current, count++) {
      current = it_even.Get();
      EXPECT_LE(0, current);
      EXPECT_GT(j * 1000, current);
      if (previous != kInvalidRowId)
        EXPECT_LT(previous, current);
      EXPECT_EQ(Row(query_1k_rows(), 0),
                Row(row_multi_set_->indexed_view(), current));
    }
    EXPECT_EQ(j * 500, count);

    for (count = 0, previous = kInvalidRowId;
         !it_odd.AtEnd();
         it_odd.Next(), previous = current, count++) {
      current = it_odd.Get();
      EXPECT_LE(0, current);
      EXPECT_GT(j * 1000, current);
      if (previous != kInvalidRowId)
        EXPECT_LT(previous, current);
      EXPECT_EQ(Row(query_1k_rows(), 1),
                Row(row_multi_set_->indexed_view(), current));
    }
    EXPECT_EQ(j * 500, count);
  }
}

TEST_F(RowHashSetTest, ReserveCapacity) {
  EXPECT_EQ(query_1().row_count(),
            row_hash_set_->Insert(query_1(), row_hash_set_result_.get()));
  EXPECT_EQ(0, row_hash_set_result_->Result(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_TRUE(row_hash_set_->ReserveRowCapacity(0));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_TRUE(row_hash_set_->ReserveRowCapacity(1));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_TRUE(row_hash_set_->ReserveRowCapacity(20));
  EXPECT_EQ(1, row_hash_set_->size());
  EXPECT_EQ(Row(query_1(), 0), Row(row_hash_set_->indexed_view(), 0));
}

TEST_F(RowHashSetTest, PartialSuccessUnderMemoryConstraints) {
  MemoryLimit limit;
  RowHashSet set(row_hash_set_block_schema_, &limit);
  set.ReserveRowCapacity(1);
  limit.SetQuota(limit.GetUsage() + 8);
  EXPECT_EQ(2, set.Insert(query_1122(), row_hash_set_result_.get()));
  EXPECT_EQ(1, set.size());
  EXPECT_EQ(Row(query_1122(), 0), Row(set.indexed_view(), 0));
  limit.SetQuota(std::numeric_limits<size_t>::max());
  set.ReserveRowCapacity(4);
  limit.SetQuota(limit.GetUsage() + 24);
  EXPECT_EQ(3, set.Insert(query_24680(), row_hash_set_result_.get()));
  EXPECT_EQ(Row(query_24680(), 0), Row(set.indexed_view(), 1));
  EXPECT_EQ(Row(query_24680(), 1), Row(set.indexed_view(), 2));
  EXPECT_EQ(Row(query_24680(), 2), Row(set.indexed_view(), 3));
}

TEST_F(RowHashSetTest, MultiPartialSuccessUnderMemoryConstraints) {
  MemoryLimit limit;
  RowHashMultiSet set(row_hash_set_block_schema_, &limit);
  set.ReserveRowCapacity(3);
  limit.SetQuota(limit.GetUsage() + 12);
  EXPECT_EQ(3, set.Insert(query_1122(), row_multi_set_result_.get()));
  EXPECT_EQ(3, set.size());
  EXPECT_EQ(Row(query_1122(), 0), Row(set.indexed_view(), 0));
  EXPECT_EQ(Row(query_1122(), 1), Row(set.indexed_view(), 1));
  EXPECT_EQ(Row(query_1122(), 2), Row(set.indexed_view(), 2));
  limit.SetQuota(std::numeric_limits<size_t>::max());
  set.ReserveRowCapacity(6);
  limit.SetQuota(limit.GetUsage() + 24);
  EXPECT_EQ(3, set.Insert(query_24680(), row_multi_set_result_.get()));
  EXPECT_EQ(Row(query_24680(), 0), Row(set.indexed_view(), 3));
  EXPECT_EQ(Row(query_24680(), 1), Row(set.indexed_view(), 4));
  EXPECT_EQ(Row(query_24680(), 2), Row(set.indexed_view(), 5));
}

// TODO(user): Selection vector tests.

}  // namespace row_hash_set

}  // namespace supersonic
