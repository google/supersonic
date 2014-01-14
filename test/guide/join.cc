// Copyright 2012 Google Inc. All Rights Reserved.
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
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
// The last tutorial file in this section covers one very important thing we
// haven't considered so far - performing joins. Finally, we will discuss
// a larger and more complex operation tree to see how a real-world example
// works in practice.

#include <map>
using std::map;
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <utility>
#include "supersonic/utils/std_namespace.h"

#include "gtest/gtest.h"

#include "supersonic/supersonic.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/utils/strings/stringpiece.h"

// Include some map utilities to use for result verification.
#include "supersonic/utils/map_util.h"

using supersonic::Attribute;
using supersonic::Block;
using supersonic::Cursor;
using supersonic::Operation;
using supersonic::FailureOr;
using supersonic::FailureOrOwned;
using supersonic::GetConstantExpressionValue;
using supersonic::TupleSchema;
using supersonic::Table;
using supersonic::TableRowWriter;
using supersonic::View;
using supersonic::ViewCopier;
using supersonic::HashJoinOperation;
using supersonic::HeapBufferAllocator;
using supersonic::JoinType;
using supersonic::ProjectNamedAttribute;
using supersonic::ProjectNamedAttributeAs;
using supersonic::rowid_t;
using supersonic::SingleSourceProjector;
using supersonic::MultiSourceProjector;
using supersonic::CompoundSingleSourceProjector;
using supersonic::CompoundMultiSourceProjector;
using supersonic::ResultView;
using supersonic::ScanView;
using supersonic::SucceedOrDie;

using supersonic::If;
using supersonic::IfNull;
using supersonic::Less;
using supersonic::CompoundExpression;
using supersonic::Expression;
using supersonic::Compute;
using supersonic::Generate;
using supersonic::ParseStringNulling;
using supersonic::ConstBool;
using supersonic::ConstString;
using supersonic::ConstInt32;
using supersonic::Null;

using supersonic::INNER;
using supersonic::UNIQUE;

using supersonic::INT32;
using supersonic::NOT_NULLABLE;
using supersonic::NULLABLE;
using supersonic::STRING;
using supersonic::DATE;
using supersonic::BOOL;

using supersonic::rowcount_t;

// EXAMPLE 1 :: Hash join
//
// This test attempts to explain an ubiquitous database operation that is
// joining. The hash join operation API and the functionalities it provides are
// explained. The test case also uses tables for in-memory materialisation and
// expressions to facilitate single row insertion into the table.
//
// We will also use sinks to pass computed data into tables.
class HashJoinTest : public testing::Test {
 protected:
  virtual void SetUp() {
    // We set up author and book schemata which will then be used to run a hash
    // join. To keep things simple we will only allow one author per book
    // (of course authors may have written multiple books, otherwise there
    // wouldn't be much point in joining).
    author_schema.add_attribute(Attribute("author_id", INT32, NOT_NULLABLE));
    author_schema.add_attribute(Attribute("name", STRING, NOT_NULLABLE));
    author_schema.add_attribute(Attribute("nobel", BOOL, NOT_NULLABLE));

    // The second schema will contain a column with the book's publishing
    // date. We haven't used temporal attributes yet, and it feels like it's
    // time to fix this. Supersonic provides two date-like data types:
    // DATE, which has one-day granularity and DATETIME allowing microsecond
    // precision. We will use the DATE field as we don't need that much
    // information.
    //
    // Internally DATE and DATETIME objects are stored as 32- and 64-bit
    // integers respectively.
    //
    // We will also work with null values this time. To declare a column
    // nullable, we use the appropriate argument to Attribute(), as shown
    // in two cases below.
    book_schema.add_attribute(Attribute("book_id", INT32, NOT_NULLABLE));
    book_schema.add_attribute(Attribute("author_id_ref", INT32, NULLABLE));
    book_schema.add_attribute(Attribute("title", STRING, NOT_NULLABLE));
    book_schema.add_attribute(Attribute("date_published",
                                        DATE,
                                        NULLABLE));

    // The two previous guide files used a very simple approach
    // towards data loading - we created the data manually in an array
    // and then let our input views load it by giving them pointers and
    // row counts. There is nothing wrong with that technically, but on
    // a higher level of abstraction we might not want to deal with
    // raw arrays.
    //
    // In this example we will explore tables, which are our every day
    // in-memory data materialisation units. Supersonic tables have a rich and
    // extensible API - they allow us to append whole views, rows one by one
    // and also manage memory usage by reserving extra capacity or shrinking
    // the size of used memory to the required minimum. One thing we might
    // find slighly annoying is that to append a new row we have to create
    // a memory spot for it and fill all column values one after the other.
    //
    // To alleviate the headache we could employ a TableRowWriter object which
    // would handle row addition in a much less verbose fashion. We will
    // do something different this time, though.
    //
    // In the first example in primer.cc we used Supersonic to do some simple
    // arithmetics. We will now revisit expressions and use them to insert
    // rows into our tables. We will use not only trivial "const value"
    // expressions, but also ones that do contain some logic, such as
    // parsing date strings.
    //
    // Our entities will contain ids and it would be really nice if we could
    // handle sequential value generation with as little hassle as possible.
    // Supersonic has an expression called Sequence and one might be tempted
    // to use it here, but it doesn't quite do what we want. A Sequence
    // expression would not remember its state between separate insertions
    // and it should be used for operations that deal with all rows we want
    // to enumerate at the same time. Hence it is not a counterpart for
    // *SQL database "auto increment" utilities. It is, however, extremely
    // helpful when used together with filtering - for instance to eliminate
    // every n-th row. Here we will have to manage the ids ourselves.
    //
    // First things first though - we have to create the tables.
    author_table.reset(new Table(author_schema,
                                 HeapBufferAllocator::Get()));
    book_table.reset(new Table(book_schema,
                               HeapBufferAllocator::Get()));

    // We will present 2 different mechanisms of populating the table with data:
    // 1. Using TableRowWriter - that better matches simple test scenarios.
    author_table_writer.reset(new TableRowWriter(author_table.get()));
    // 2. And writing directly to the table.

    // Entry counters to generate ids.
    author_count = 0;
    book_count = 0;
  }

  void PrepareJoin() {
    // Before we start implementing our operation a few notes on how
    // joins are realised should be made. Join is used to combine
    // data from different sources by matching selected attributes' values.
    // We sometimes refer to these sources as the left and right hand side
    // (or lhs and rhs to keep it brief).
    //
    // In Supersonic the right hand side is treated as the index - it should
    // be relatively small, as the data coming from lhs cursor
    // will be streamed and matched against the index, which is itself stored
    // in memory. In our case there should be significantly fewer authors than
    // books and so books will be the lhs and authors the rhs. Supersonic can
    // turn on certain optimisations for the index in some specific cases, but
    // we will get to that shortly.
    //
    // We now prepare single source projectors (key selectors) for both tables.
    std::unique_ptr<const SingleSourceProjector> book_selector(
        ProjectNamedAttribute("author_id_ref"));

    std::unique_ptr<const SingleSourceProjector> author_selector(
        ProjectNamedAttribute("author_id"));

    // We now need a multisource projector to describe precisely what kind of
    // result we want. To do so we use the CompoundMultiSourceProjector -
    // Supersonic will then bind it to our two schemata. We should now specify
    // for each schema which columns we want projected to the result.
    // It is perfectly legal for two schemata to have matching column names
    // and so, in order to avoid duplicates, we can specify prefixes which
    // should be slapped onto those columns - one for each schema, come
    // up with new names ourselves or simply decide to toss some of them away.
    std::unique_ptr<CompoundMultiSourceProjector> result_projector(
        new CompoundMultiSourceProjector());

    // The add() function used by the multi source projector, unlike its single
    // source counterpart, takes two arguments, the source index and a single
    // source projector. We must now specify those (compound) projectors.
    // It is also possible to use a shortcut ProjectAllAtributes utility
    // method, but it would have the ugly side effect of generating both
    // columns we join on, which is clearly superfluous. We might also want
    // to decide to omit other columns from our projection.
    std::unique_ptr<CompoundSingleSourceProjector> result_book_projector(
        new CompoundSingleSourceProjector());
    result_book_projector->add(ProjectNamedAttribute("title"));
    result_book_projector->add(ProjectNamedAttribute("date_published"));

    std::unique_ptr<CompoundSingleSourceProjector> result_author_projector(
        new CompoundSingleSourceProjector());
    result_author_projector->add(
        ProjectNamedAttributeAs("name", "author_name"));
    result_author_projector->add(ProjectNamedAttribute("nobel"));

    // Single source projectors are ready - let's add them to the final result
    // projector.
    result_projector->add(0, result_book_projector.release());
    result_projector->add(1, result_author_projector.release());

    // It seems that we could now finish off by attaching cursors containing
    // our left and right input schemata, but there are two more things we need
    // to consider.
    //
    // Firstly we have yet to decide what sort of join we want to perform.
    // Currently Supersonic only supports two kinds: inner and left outer join.
    // An inquisitive user/developer will have found that there exist
    // JoinType stubs for right outer and full outer join, but their support
    // has yet to be implemented. Here, we will use an inner join in order
    // to omit books with an unknown author.
    //
    // Secondly, Supersonic also asks us to say whether in the rhs schema data
    // all keys are unique. If we have this information beforehand we should
    // specify so, which will turn on some hash join optimisations. If we know
    // there will be duplicates, or we're simply not sure, we should go for
    // the NOT_UNIQUE option. In this case, with authors being our rhs index
    // table, we can give green light to optimisations.
    //
    // We are now ready to create the operation.
    std::unique_ptr<Operation> hash_join(
        new HashJoinOperation(/* join type */ INNER,
                              /* select left */ book_selector.release(),
                              /* select right */ author_selector.release(),
                              /* project result */ result_projector.release(),
                              /* unique keys on the right ? */ UNIQUE,
                              /* left data */ ScanView(book_table->view()),
                              /* right data */ ScanView(author_table->view())));
    result_cursor.reset(SucceedOrDie(hash_join->CreateCursor()));
  }

  // The add author method will create an author entry
  // with a given name and "received Nobel Prize?" field. We will
  // return the generated ids from the function call to be able
  // to tie books to the authors.
  int32 AddAuthor(const StringPiece& name, bool nobel) {
    int32 author_id = author_count++;
    // The order of fields is important and must match the schema of
    // author_table.
    author_table_writer
        ->AddRow().Int32(author_id).String(name).Bool(nobel).CheckSuccess();
    return author_id;
  }

  // We will here the direct write to book table. Here we also
  // add simple support for null values, a null date_published argument
  // and a negative author_id will both be treated as such.
  int32 AddBook(const StringPiece& title,
                const StringPiece& date_published,
                int32 author_id) {
    int32 book_id = book_count++;
    // In fact we don't need separate book_count as we can always read it from
    // book_table.row_count() directly.
    CHECK_EQ(book_id, book_table->row_count());

    rowid_t row_id = book_table->AddRow();

    // setting Attribute("book_id", INT32, NOT_NULLABLE).
    book_table->Set<INT32>(0, row_id, book_id);

    // setting Attribute("author_id_ref", INT32, NULLABLE).
    if (author_id >= 0) {
      book_table->Set<INT32>(1, row_id, author_id);
    } else {
      book_table->SetNull(1, row_id);
    }
    // setting Attribute("title", STRING, NOT_NULLABLE).
    // This makes a deep copy of the StringPiece.
    book_table->Set<STRING>(2, row_id, title);

    // setting Attribute("date_published", DATE, NULLABLE).

    // DATEs are internally represented as int32s, and - since we don't want our
    // test to depend on the exact representation - we're using Supersonic's
    // evaluation machinery to obtain the int32 that would be used to represent
    // the value and store it in the row.
    // Alternate way to do this would be to simply put the string into the
    // table, and then prepend the ParseStringNulling to our evaluation.

    // The ParseStringNulling utility allows us to convert a string expression
    // into a date object. A null entry will be created on null or invalid
    // input.
    //
    // Side note: DATETIME expressions actually have a ConstDateTime method
    // for creating objects directly from StringPieces, but a shortcut for
    // DATEs has not been implemented.
    std::unique_ptr<const Expression> date_or_null(
        ParseStringNulling(DATE, ConstString(date_published)));
    bool date_published_is_null = false;
    FailureOr<int32> data_published_as_int32 =
        GetConstantExpressionValue<DATE>(*date_or_null,
                                         &date_published_is_null);
    CHECK(data_published_as_int32.is_success())
        << data_published_as_int32.exception().ToString();

    if (!date_published_is_null) {
      book_table->Set<DATE>(3, row_id, data_published_as_int32.get());
    } else {
      book_table->SetNull(3, row_id);
    }
    return book_id;
  }

  // Maps of author names and book titles by ids (authors) and author reference
  // ids (books).
  typedef std::map<int32, StringPiece> author_name_map;
  typedef std::multimap<int32, StringPiece> book_title_map;

  // Utilities for storing pairs of (name, title).
  typedef std::pair<StringPiece, StringPiece> author_book_entry;
  typedef std::set<author_book_entry> author_book_set;

  void TestResults() {
    // We will now check if the results match out expectations. Firstly, we
    // have to poll for the rows and put them in a memory block.
    std::unique_ptr<Block> result_space(
        new Block(result_cursor->schema(), HeapBufferAllocator::Get()));

    ViewCopier copier(result_cursor->schema(), /* deep copy */ true);
    rowcount_t offset = 0;
    std::unique_ptr<ResultView> rv(new ResultView(result_cursor->Next(-1)));

    while (!rv->is_done()) {
      const View& view = rv->view();
      rowcount_t view_row_count = view.row_count();

      // Reallocate block for the new values - we don't know beforehand how many
      // of them there will be.
      result_space->Reallocate(offset + view_row_count);

      rowcount_t rows_copied = copier.Copy(view_row_count,
                                           view,
                                           offset,
                                           result_space.get());

      EXPECT_EQ(view_row_count, rows_copied);

      offset += rows_copied;
      rv.reset(new ResultView(result_cursor->Next(-1)));
    }

    const View& result_view(result_space->view());

    author_name_map author_names;
    book_title_map book_titles;

    // Populate book and author maps with entries taken out of tables.
    const rowcount_t author_count = author_table->row_count();
    const rowcount_t book_count = book_table->row_count();

    for (rowcount_t i = 0; i < author_count; ++i) {
      int32 id = author_table->view().column(0).typed_data<INT32>()[i];
      StringPiece name = author_table->view().column(1).typed_data<STRING>()[i];
      author_names[id] = name;
    }

    for (rowcount_t i = 0; i < book_count; ++i) {
      bool has_ref = !book_table->view().column(1).is_null()[i];
      if (has_ref) {
        int32 ref = book_table->view().column(1).typed_data<INT32>()[i];
        StringPiece title = book_table->view().column(2)
                                              .typed_data<STRING>()[i];
        book_titles.insert(std::make_pair(ref, title));
      }
    }

    // Now we use the populated maps to create a set of author-book pairs...
    author_book_set author_book_pairs;
    for (book_title_map::const_iterator it = book_titles.begin();
         it != book_titles.end();
         ++it) {
      int32 id = it->first;
      author_book_pairs.insert(author_book_entry(author_names[id],
                                                 it->second));
    }

    // ...and using the created set we test if the data in our result
    // view matches what we have been expecting. (For simplicity, we assume
    // that there are no situations where two authors with identical names
    // wrote books with identical titles).
    for (rowcount_t i = 0; i < result_view.row_count(); ++i) {
      StringPiece title = result_view.column(0).typed_data<STRING>()[i];
      StringPiece name = result_view.column(2).typed_data<STRING>()[i];

      author_book_entry entry(name, title);

      EXPECT_TRUE(ContainsKey(author_book_pairs, entry))
          << "Entry ("
          << entry.first
          << ", "
          << entry.second
          << " invalidly created by hash join!";
      // Mark that the entry is okay by removing it.
      author_book_pairs.erase(entry);
    }

    // Make sure that all the "good" entries have made it through the join
    // by asserting that the set is empty.
    EXPECT_TRUE(author_book_pairs.empty());
  }

  // Supersonic objects.
  std::unique_ptr<Cursor> result_cursor;

  TupleSchema author_schema;
  TupleSchema book_schema;

  std::unique_ptr<Table> author_table;
  std::unique_ptr<TableRowWriter> author_table_writer;
  std::unique_ptr<Table> book_table;

  // Sequence counters.
  int32 author_count;
  int32 book_count;
};

TEST_F(HashJoinTest, SmallHashJoinTest) {
  // DISCLAIMER: The values below should by no means be used as a reliable
  // information source, especially the publishing dates are not accurate,
  // although the years should match reality... :)
  int32 terry_id = AddAuthor("Terry Pratchett", false);
  int32 chuck_id = AddAuthor("Chuck Palahniuk", false);
  int32 ernest_id = AddAuthor("Ernest Hemingway", true);

  // Again, in a production environment one would use a simpler INT32 field
  // if they didn't care about full dates, but we are excused by demonstration
  // purposes.
  AddBook("The Reaper Man", "1991/01/01", terry_id);
  AddBook("Colour of Magic", "1983/01/01", terry_id);
  AddBook("Light Fantastic", "1986/01/01", terry_id);
  AddBook("Mort", NULL, terry_id);

  AddBook("Fight Club", "1996/01/01", chuck_id);
  AddBook("Survivor", NULL, chuck_id);
  AddBook("Choke", "2001/01/01", chuck_id);

  AddBook("The old man and the sea", NULL, ernest_id);
  AddBook("For whom the bell tolls", NULL, ernest_id);
  AddBook("A farewell to arms", "1929/01/01", ernest_id);

  AddBook("Carpet People", NULL, -1);
  AddBook("Producing open source software.", NULL, -1);
  AddBook("Quantum computation and quantum information.", NULL, -1);

  PrepareJoin();

  TestResults();
}
