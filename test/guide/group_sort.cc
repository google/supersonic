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
// This tutorial is the second in the guide and it picks up where primer.cc left
// off. For more information see the README file in this directory.
//
// The file contains two larger test cases which cover more complex
// aggregations and sorting. Basic memory management will also be touched
// upon including creating StringPiece objects using a memory arena.

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
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

using supersonic::TupleSchema;
using supersonic::View;
using supersonic::ResultView;
using supersonic::Attribute;
using supersonic::Arena;
using supersonic::AggregationSpecification;
using supersonic::Operation;
using supersonic::CompoundSingleSourceProjector;
using supersonic::SingleSourceProjector;
using supersonic::ProjectNamedAttribute;
using supersonic::ProjectNamedAttributeAs;
using supersonic::ProjectAttributeAt;
using supersonic::GroupAggregate;
using supersonic::Cursor;
using supersonic::SucceedOrDie;
using supersonic::Block;
using supersonic::HeapBufferAllocator;
using supersonic::ViewCopier;
using supersonic::NO_SELECTOR;

using supersonic::MAX;
using supersonic::MIN;

using supersonic::INT32;
using supersonic::NOT_NULLABLE;
using supersonic::STRING;
using supersonic::BOOL;
using supersonic::DOUBLE;

using supersonic::SortOrder;
using supersonic::Sort;
using supersonic::ASCENDING;

// EXAMPLE 1 :: Compound grouping and string piece use.
//
// In this test example we use Googletest's TestFixture mechanism to reuse
// Supersonic object creation for a couple of test cases.
//
// The GroupingTest will feature a simple Employee-style column schema with
// name, age, salary, department and contract type information stored for every
// individual. The goal will be to use a more complex aggregation in order
// to extract the minimum salary and maximum age for all departments, but
// separately for full-time and non-full-time employees. The test case also
// shows how to use variable length blobs - strings in this example - and
// allocate memory for them. Some simple memory management techniques are
// also pointed out.
class GroupingTest : public testing::Test {
 protected:
  // Setup routine run for every test case.
  virtual void SetUp() {
    // First off we prepare the tuple schema and an input view.
    schema.add_attribute(Attribute("name", STRING, NOT_NULLABLE));
    schema.add_attribute(Attribute("age", INT32, NOT_NULLABLE));
    schema.add_attribute(Attribute("salary", INT32, NOT_NULLABLE));
    schema.add_attribute(Attribute("department", STRING, NOT_NULLABLE));
    schema.add_attribute(Attribute("full_time", BOOL, NOT_NULLABLE));

    input_view.reset(new View(schema));

    // Supersonic's API does not deal directly with STL or C-style strings,
    // instead, a rather simple wrapper called StringPiece is exploited.
    // A StringPiece consists of a pointer to the buffer which contains
    // the actual string data and the data's length. More information
    // about StringPieces and how they should be used can be found in
    // supersonic/utils/strings/stringpiece.h . In order to provide an effective
    // way of manipulating data bits of variable length Supersonic introduces
    // a (memory) arena. An arena manages memory buffers, which are coherent
    // data blocks. It starts off with a single buffer of a size specified
    // in the Arena object's constructor. Once a buffer's memory has been
    // used up it will attempt to create another one. The next created
    // buffer is twice as big as its predecessor, provided that the specified
    // memory cap allows for the allocation. In the following example the arena
    // will kick off with a single 32-byte buffer and will not use buffers
    // larger than 128 bytes.
    //
    // It will sometimes happen in this tutorial that we will use examples
    // which are not entirely sensible for production cases in order to
    // provide an in-depth demonstration of how the library works. To achieve
    // good performance the maximum buffer size should not be lower than
    // the page size on the used architecture, which will help minimise
    // fragmentation.
    //
    // Here, however, performance is not an issue, on the other hand we do
    // want to get some coverage - setting a low maximum buffer size
    // will guarantee that the arena will have to actively manage buffers
    // even when dealing with small data sets.
    arena.reset(new Arena(32, 128));

    // Arenas use a default heap allocator, but it is possible to provide
    // a more complex one. Examples include allocators that impose memory
    // limits, guarantee thread-safety or collect memory statistics. For more
    // details check out supersonic/base/memory/memory.h .
  }

  // This helper method will prepare the aggregation much like we did in
  // the aggregation example in primer.cc .
  void PrepareAggregation() {
    std::unique_ptr<AggregationSpecification> specification(
        new AggregationSpecification());

    // Aggregation specification will now contain two end results.
    specification->AddAggregation(MIN, "salary", "min_salary");
    specification->AddAggregation(MAX, "age", "max_age");

    // We now use a compound source projector to group by two different columns.
    // This time we change the contract type's description to a more verbose one
    // and specify the department column's index.
    //
    // Referring to a column using its index may often be unnecessarily obscure;
    // in this case it would also be better-style to use ProjectNamedAttribute.
    // Still, we use it here merely to demonstrate the possibility.
    std::unique_ptr<CompoundSingleSourceProjector> projector(
        new CompoundSingleSourceProjector());

    projector->add(ProjectNamedAttributeAs("full_time", "Works full time?"));
    projector->add(ProjectAttributeAt(3)); // department column

    // Finally, we create the aggregation and store the resulting cursor
    // as a test fixture's field.
    std::unique_ptr<Operation> aggregation(GroupAggregate(
        projector.release(), specification.release(),
        /* use default aggregation options */ NULL, ScanView(*input_view)));

    result_cursor.reset(SucceedOrDie(aggregation->CreateCursor()));
  }

  // Simple method for loading data from memory.
  void LoadData(const StringPiece* names,
                const int32* ages,
                const int32* salaries,
                const StringPiece* depts,
                const bool* full_time,
                size_t row_count) {
    input_view->set_row_count(row_count);
    input_view->mutable_column(0)->Reset(names, NULL);
    input_view->mutable_column(1)->Reset(ages, NULL);
    input_view->mutable_column(2)->Reset(salaries, NULL);
    input_view->mutable_column(3)->Reset(depts, NULL);
    input_view->mutable_column(4)->Reset(full_time, NULL);
  }

  typedef std::pair<bool, StringPiece> key_type;

  // The following method will perform the aggregation programatically using
  // STL sets and maps and compare the results.
  void TestResults(const int32* ages,
                   const int32* salaries,
                   const StringPiece* depts,
                   const bool* full_times,
                   unsigned input_row_count) {
    std::map<key_type, int32> min_sal;
    std::map<key_type, int32> max_age;

    // Find the expected output row count.
    std::set<key_type> key_set;
    for (unsigned i = 0; i < input_row_count; ++i) {
      key_set.insert(key_type(full_times[i], depts[i]));
    }

    // The result row count may vary depending on the number
    // of department-contract value combinations represented in the input.
    unsigned row_count = key_set.size();

    // Scan input and populate aggregate results.
    for (unsigned i = 0; i < input_row_count; ++i) {
      key_type key(full_times[i], depts[i]);
      if (!ContainsKey(max_age, key)) {
        min_sal[key] = salaries[i];
        max_age[key] = ages[i];
      } else {
        min_sal[key] = std::min(min_sal[key], salaries[i]);
        max_age[key] = std::max(max_age[key], ages[i]);
      }
    }

    // Load all available rows to the result view. The default GroupOptions
    // object sets a high memory cap, hence we may quite safely assume that we
    // will not surpass it even with hundreds of thousands of rows. A safer
    // and more general approach to using cursors will be shown in the example
    // on sorting.
    ResultView result(result_cursor->Next(-1));
    ASSERT_TRUE(result.has_data());
    ASSERT_FALSE(result.is_eos());

    View result_view(result.view());

    // We expect four columns in the output schema, two keys onto which we have
    // projected input values and two aggregate results.
    EXPECT_EQ(4, result_view.column_count());

    EXPECT_EQ(row_count, result_view.row_count());

    // Test whether view results match those in the map.
    for (unsigned i = 0; i < row_count; ++i) {
      key_type key(result_view.column(0).typed_data<BOOL>()[i],
                   result_view.column(1).typed_data<STRING>()[i]);
      EXPECT_EQ(min_sal[key], result_view.column(2).typed_data<INT32>()[i]);
      EXPECT_EQ(max_age[key], result_view.column(3).typed_data<INT32>()[i]);
    }
  }

  // Supersonic objects.
  std::unique_ptr<Cursor> result_cursor;
  std::unique_ptr<Arena> arena;
  TupleSchema schema;
  std::unique_ptr<View> input_view;
};

TEST_F(GroupingTest, SmallGroupingTest) {
  // Creating the data set.
  const unsigned row_count = 5;
  string names_str[row_count] = {"John", "Darrel", "Greg", "Amanda", "Stacy"};
  int32 ages[row_count] = {20, 25, 32, 31, 33};
  int32 salaries[row_count] = {1800, 3300, 4800, 3500, 1900};
  string depts_str[row_count] = {"Accounting", "Sales", "Sales", "IT", "IT"};
  bool full_times[row_count] = {false, true, false, true, false};

  StringPiece names[row_count];
  StringPiece depts[row_count];

  // TODO(tkaftal): Would it make more sense to add names and departments
  // in separate loops, so that they could populate coherent memory
  // blocks in the arena?
  // If so, I'll drop a comment on this but maybe the code should be left as
  // it is now as it's more compact this way.
  //
  // We call the AddStringPieceContent() method to add a string piece to the
  // arena. We receive a handle to the string in return, to create a valid
  // string piece, we need to specify its length as well.
  for (unsigned i = 0; i < row_count; i++) {
    names[i] = StringPiece(arena->AddStringPieceContent(names_str[i]),
                           names_str[i].length());
    depts[i] = StringPiece(arena->AddStringPieceContent(depts_str[i]),
                           depts_str[i].length());
  }

  LoadData(names, ages, salaries, depts, full_times, row_count);
  PrepareAggregation();
  TestResults(ages, salaries, depts, full_times, row_count);
}

TEST_F(GroupingTest, LargeRandomGroupingTest) {
  // We will create a larger data set this time using sevaral
  // pooled employee and department names.
  const unsigned row_count = 10000;

  // We allocate on heap to not exceed max stack frame size (e.g. 16KB).
  std::unique_ptr<StringPiece[]> names(new StringPiece[row_count]);
  std::unique_ptr<int32[]> ages(new int32[row_count]);
  std::unique_ptr<int32[]> salaries(new int32[row_count]);
  std::unique_ptr<StringPiece[]> depts(new StringPiece[row_count]);
  std::unique_ptr<bool[]> full_times(new bool[row_count]);

  const unsigned kNamePoolSize = 7;
  const unsigned kDeptPoolSize = 15;
  string namePool[kNamePoolSize] = {
      "John",
      "James",
      "Alan",
      "Judy",
      "Anne",
      "Ray",
      "Grace"
  };
  string deptPool[kDeptPoolSize] = {
      "IT",
      "Sales",
      "Legal",
      "Services",
      "Advertising",
      "Research",
      "Operations",
      "Compliance",
      "Public Relations",
      "Human Resources",
      "Research",
      "Engineering",
      "Deployment",
      "Accounting",
      "Tech Support"
  };

  // Populate random data.
  for (unsigned i = 0; i < row_count; ++i) {
    unsigned namePoolRand = rand() % kNamePoolSize;
    unsigned deptPoolRand = rand() % kDeptPoolSize;

    names[i] = StringPiece(arena->AddStringPieceContent(namePool[namePoolRand]),
                           namePool[namePoolRand].length());

    depts[i] = StringPiece(arena->AddStringPieceContent(deptPool[deptPoolRand]),
                           deptPool[deptPoolRand].length());

    ages[i] = (rand() % 60) + 20;
    salaries[i] = (rand() % 1900) * 10 + 1000;
    full_times[i] = rand() % 2;
  }

  LoadData(names.get(), ages.get(), salaries.get(), depts.get(),
           full_times.get(), row_count);
  PrepareAggregation();
  TestResults(ages.get(), salaries.get(), depts.get(), full_times.get(),
              row_count);
}

// EXAMPLE 2 :: Single-column sort
//
// The following fixture creates a two-column schema of, say, student ids
// and their grades. Our goal will be to sort the tuples by the grade.
// In order to do so, we will create a simple sorting operation and test it
// on two data sets.
class SortingTest : public testing::Test {
 protected:
  virtual void SetUp() {
    // Simple two-column schema to use for sorting by the "grade" attribute.
    schema.add_attribute(Attribute("id", INT32, NOT_NULLABLE));
    schema.add_attribute(Attribute("grade", DOUBLE, NOT_NULLABLE));

    input_view.reset(new View(schema));
  }

  void PrepareSort() {
    // First, we'll need to specify the column by which the sorting should be
    // carried out. To do it we once again employ a single source projector.
    std::unique_ptr<const SingleSourceProjector> projector(
        ProjectNamedAttribute("grade"));

    // The created projector will have to be passed to a SortOrder object
    // together with the order type - either ascending or descending.
    // We could have also specified several projectors to run a multi-level
    // sort, in that case the order of attribute addition would be the same as
    // the order of sorting by the specified columns.
    std::unique_ptr<SortOrder> sort_order(new SortOrder());
    sort_order->add(projector.release(), ASCENDING);

    // We also have to specify a memory limit for sorting - if there
    // is more data than the bound allows for, it will be sorted using temporary
    // files containing partial results which will eventually be merged
    // to obtain a final sorted tuple sequence.
    //
    // The current implementation supports only "soft" memory limits, that is
    // the actual amount of used memory may be higher, but will remain within
    // the same order of magnitude. Throughout most of the computation the used
    // memory will not exceed two times the specified limit, but momentary use
    // may still be higher.
    const size_t mem_limit = 128;

    // We now create the sort operation passing the created sort order object,
    // the soft memory limit and the scan view input wrapper. It is also
    // possible to provide a result projector as the second argument to Sort()
    // in order to process the result columns. In this scenario we settle for
    // passing a NULL which will default to an identity projection.
    //
    // Note that the implementation of sorting is NOT stable! Therefore in order
    // to sort data by several columns one should use the aforementioned
    // approach with specifying multiple projectors rather than combining Sort()
    // operations.
    std::unique_ptr<Operation> sort(Sort(sort_order.release(),
                                         /* identity projection */ NULL,
                                         mem_limit, ScanView(*input_view)));

    result_cursor.reset(SucceedOrDie(sort->CreateCursor()));
  }

  void LoadData(const int32* ids,
                const double* grades,
                size_t row_count) {
    // Load up the data columns.
    input_view->set_row_count(row_count);
    input_view->mutable_column(0)->Reset(ids, NULL);
    input_view->mutable_column(1)->Reset(grades, NULL);
  }

  typedef std::pair<int32, double> entry;
  typedef map<entry, int32>::const_iterator entry_it;

  void TestResults(const int32* ids,
                   const double* grades,
                   size_t row_count) {
    // Load all available rows to the result view. Like we mentioned before
    // there is no guarantee of receiving a view of the same size that we
    // call Next() with. The sorting operation, for one, will only create views
    // with no more than 1024 rows, which emphasises the difference between
    // the view size and the actual underlying data size. To obtain the
    // desired data we will have to keep polling until we've got all rows.
    //
    // To simplify testing (and present another Supersonic feature at the same
    // time!) we will save the polling results into one preallocated memory
    // block. Since views are managed by the cursors that have created them,
    // we need to use a ViewCopier whenever we want to make a functional copy.
    //
    // We start by creating a target block. We need to specify a tuple schema
    // and a buffer allocator. As we know how many rows to expect beforehand,
    // we should preallocate the block's memory.
    std::unique_ptr<Block> result_space(
        new Block(schema, HeapBufferAllocator::Get()));
    result_space->Reallocate(row_count);

    // We now prepare a view copier to handle memory juggling for us.
    // What it needs to know are the input and output tuple schemas
    // (identical in this case), which rows should be selected (we can
    // use a NO_SELECTOR parameter to copy all rows without filtering)
    // and whether a deep copy should be made.
    ViewCopier copier(schema, /* deep copy */ true);

    // The copier is ready to get cracking. We can now launch a polling
    // loop and get our hands on the results.
    unsigned offset = 0;

    // Create a result view much like before.
    std::unique_ptr<ResultView> rv(new ResultView(result_cursor->Next(1024)));

    while (!rv->is_done()) {
      const View& view = rv->view();
      unsigned view_row_count = view.row_count();

      // Copy the data to its rightful place in our new result space.
      // We have to provide the row count, reference to the data
      // source view, offset within the target block and the block itself.
      // Optionally a list of input row ids may be provided, but we will
      // not use it in this example.
      //
      // Also note that the copier should not have to be responsible for
      // the target memory block, it is therefore incorrect to release
      // the result_space scoped pointer.
      unsigned rows_copied = copier.Copy(view_row_count,
                                         view,
                                         offset,
                                         result_space.get());

      // The returned number of copied rows will match the specified
      // count, unless there have been allocation errors.
      EXPECT_EQ(view_row_count, rows_copied);

      offset += rows_copied;
      rv.reset(new ResultView(result_cursor->Next(1024)));
    }

    const View& result_view(result_space->view());

    ASSERT_EQ(2, result_view.column_count());
    ASSERT_EQ(row_count, result_view.row_count());

    // Test if the result data is correctly sorted. Previous assertions
    // will fail if there are no results.
    for (unsigned i = 1; i < row_count; ++i) {
      EXPECT_TRUE(result_view.column(1).typed_data<DOUBLE>()[i] >=
          result_view.column(1).typed_data<DOUBLE>()[i - 1]);
    }

    // Check if the sorted entries are exactly the ones we
    // received as input.
    map<entry, int32> occurences;
    for (unsigned i = 0; i < row_count; ++i) {
      entry key(ids[i], grades[i]);
      if (!ContainsKey(occurences, key)) {
        occurences[key] = 0;
      }
      occurences[key]++;
    }

    for (unsigned i = 0; i < row_count; ++i) {
      entry key(result_view.column(0).typed_data<INT32>()[i],
                result_view.column(1).typed_data<DOUBLE>()[i]);
      EXPECT_TRUE(ContainsKey(occurences, key)) << "Invalid value ("
          << key.first
          << ", "
          << key.second
          << ") appeared during sorting!";
      occurences[key]--;
    }

    for (entry_it it = occurences.begin(); it != occurences.end(); ++it) {
      EXPECT_EQ(0, it->second) << "Value ("
          << it->first.first
          << ", "
          << it->first.second
          << ") invalidly "
          << (it->second > 0 ? "removed" : "created")
          << " during sorting!";
    }
  }

  // Supersonic objects.
  std::unique_ptr<Cursor> result_cursor;
  TupleSchema schema;
  std::unique_ptr<View> input_view;
};

TEST_F(SortingTest, SmallSortingTest) {
  // Data set for sorting.
  const unsigned row_count = 8;
  int32 ids[row_count] = {1, 2, 3, 4, 5, 6, 7, 8};
  double grades[row_count] = {4.5, 4.2, 3.5, 4.8, 4.2, 3.9, 3.2, 4.8};

  LoadData(ids, grades, row_count);
  PrepareSort();
  TestResults(ids, grades, row_count);
}

TEST_F(SortingTest, LargeSortingTest) {
  // Data set for sorting.
  const unsigned row_count = 100000;
  // We allocate on heap to not exceed max stack frame size (e.g. 16KB).
  std::unique_ptr<int32[]> ids(new int32[row_count]);
  std::unique_ptr<double[]> grades(new double[row_count]);

  for (int i = 0; i < row_count; ++i) {
    ids[i] = i + 1;
    grades[i] = (4.0 * static_cast<double>(rand()) / RAND_MAX) + 1.0;
  }

  LoadData(ids.get(), grades.get(), row_count);
  PrepareSort();
  TestResults(ids.get(), grades.get(), row_count);
}
