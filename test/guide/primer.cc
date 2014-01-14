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
// This folder contains a set of tutorial guides which demonstrate a few
// core Supersonic functionalities with this file being the first of them.
// If you've just installed Supersonic and can't wait to get started, you're
// in the right place!
//
// This is an introductory set of primer end-to-end tests which describe
// the basic functionalities of the Supersonic Query Execution Engine.
// Googletest framework is used, but in this particular test set
// the operations will be placed in plain C++ functions with due
// descriptions and the use of the testing framework will be minimal.
//
// Supersonic is a fast C++ library of data transformation utilities, whose
// main aim is to be used as a Query executor for a column-oriented database.
// It works on a single machine, mostly in a single thread (with some
// exceptions) and makes heavy use of cache-aware techniques to speed up
// the computation.
//
// This file will guide you through the basic use of the library.

// STL map will be useful for checking results.
#include <map>
using std::map;
#include <memory>

// Include Googletest - we'll need it later on.
#include "gtest/gtest.h"

// Include Supersonic. The public/supersonic.h is a utility include which
// encapsulates all functionalities of the engine. It is convenient to use
// for core functionality but additional includes may at times be required.
#include "supersonic/supersonic.h"

// Using-declarations for Supersonic names, pre-listed to make the test code
// more readable.
using supersonic::BoundExpressionTree;
using supersonic::Expression;
using supersonic::Plus;
using supersonic::AttributeAt;
using supersonic::TupleSchema;
using supersonic::Attribute;
using supersonic::DOUBLE;
using supersonic::INT32;
using supersonic::STRING;
using supersonic::NOT_NULLABLE;
using supersonic::FailureOrOwned;
using supersonic::HeapBufferAllocator;
using supersonic::View;
using supersonic::EvaluationResult;
using supersonic::AggregationSpecification;
using supersonic::SUM;
using supersonic::Operation;
using supersonic::ProjectNamedAttribute;
using supersonic::SingleSourceProjector;
using supersonic::GroupAggregate;
using supersonic::ScanView;
using supersonic::ResultView;
using supersonic::SucceedOrDie;
using supersonic::GroupAggregateOptions;
using supersonic::Cursor;

// EXAMPLE 1 :: Simple expression.
//
// Supersonic is generally not intended to perform straight expression
// evaluation and may deliver poor results compared to hand-coded C++ routines.
// At times, it may also work faster, due to some low-level optimisations.
// Still, the following example is not meant to provide a use case, it has been
// designed to show an example of the data processing scheme.
//
// The following function prepares a *bound expression* for calculation. A bound
// expression is an expression (integer addition, string concatenation, etc.;
// more info on expressions will follow) which has been bound to a column
// schema and assigned space pre-allocated for its output. By column schema,
// also referred to as "tuple schema", we mean a sequence of column types.
// We will create a simple two int32 addition expression.
//
// One note on memory usage should be made before we get started. These
// tutorials will make frequent use of scoped pointers, maybe even too frequent
// at times.
//
// The reason for this is twofold. Firstly they oftentimes do lead to a much
// cleaner and easier to maintain code, but that's something the reader is
// most likely aware of. More importantly for the purpose of learning how
// to use Supersonic, however, scoped_ptr's API allows us to specify clearly
// that the pointer is merely passed to a function or object "temporarily"
// or that we intend the receipient to take ownership of the pointee. This
// clear distinction will create an outline of how Supersonic handles object
// clean-ups.
BoundExpressionTree* PrepareBoundExpression() {
  std::unique_ptr<const Expression> addition(
      Plus(AttributeAt(0), AttributeAt(1)));
  // The Plus construction method is self-explanatory - it takes two
  // expressions and returns their sum. The AttributeAt function will provide
  // values from the input residing in the column at the given index. Note that
  // the indices are numbered starting from zero and do not necessarily have to
  // be consecutive numbers.
  //
  // We now describe the input's tuple schema by creating an empty one
  // and adding attributes (column descriptions). For each attribute the column
  // name, type and nullability have to be provided.
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));

  // Once we have both the expression and the schema we may bind them. In order
  // to do it we need an allocator which will take care of creating space
  // for results. A HeapBufferAllocator will take care of the job and allocate
  // heap memory using C routines. Unless you want to do some manual memory
  // management you should be happy using the HeapBufferAllocator as shown
  // below.
  FailureOrOwned<BoundExpressionTree> bound_addition =
      addition->Bind(schema, HeapBufferAllocator::Get(), 2048);

  // We have also provided a maximum result row count in a single block.
  // As the memory may be unavailable, the binding could fail. Supersonic does
  // not use plain C++ exceptions - it has its own mechanism for handling
  // undesirable operation outcomes. The FailureOrOwned object is a wrapper
  // which can tell us whether or not the operation has succeeded and provide
  // the result. Hopefully, in this simple scenario we will not run into memory
  // failures and we can access the result using release(). If there was
  // an error, .exception().message() would tell us more about it.
  EXPECT_TRUE(bound_addition.is_success()) <<
      bound_addition.exception().message();

  return bound_addition.release();
}

// We now have a useful method for preparing a bound expression; how about
// trying to actually compute something with it? Supersonic attempts to carry
// out computations directly on arrays of values - that's one of the reasons
// it is so quick. The following method will load two arrays of values into
// a *view* and then evaluate the given expression. A view is a reference to a
// multi-column, read-only block of data Supersonic uses as a container.
// The result of the addition will also be stored in a view, but we will return
// a plain pointer to raw data from this method.
//
// Further tutorials will cover memory management in more detail, but it should
// be noted here that using the returned data is safe as long as the
// BoundExpressionTree argument is alive as it takes care of all views it
// has evaluated.
//
// In general views are managed by their creators. As they are only references
// to data blocks, one should remember that copying a view does not create
// a new physical instance of the data itself.
const int32* AddColumns(int32* a,
                        int32* b,
                        size_t row_count,
                        BoundExpressionTree* bound_tree) {
  // We have to redefine the input schema - we could also reuse the one defined
  // in PrepareBoundExpression.
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));

  // Create the input container and define input size.
  View input_view(schema);
  input_view.set_row_count(row_count);

  // Insert the input columns into the view. The second argument of the Reset
  // method is an array of bools indicating which data positions should
  // be treated as null values. Passing a NULL as the array means there are
  // no null values.
  input_view.mutable_column(0)->Reset(a, NULL);
  input_view.mutable_column(1)->Reset(b, NULL);

  // To receive the final evaluation result we call the bound expression's
  // Evaluate method on the input view. The EvaluationResult type is defined
  // as FailureOrReference<const View>, which works in a similar fashion
  // FailureOrOwned does, but it wraps up a reference rather than a pointer.
  EvaluationResult result = bound_tree->Evaluate(input_view);

  // We should now test for errors and return the data stored in the result
  // view's only column. A few more checks are run to be absolutely sure we've
  // got what we wanted.
  EXPECT_TRUE(result.is_success()) << result.exception().message();

  // Success cannot be failure.
  EXPECT_FALSE(result.is_failure());

  // Get column number using view's column_count() method.
  EXPECT_EQ(1, result.get().column_count());

  // And similarly for rows - we should have just as many as we started with.
  EXPECT_EQ(row_count, result.get().row_count());

  return result.get().column(0).typed_data<INT32>();
}

// Having created the two functions which facilitate performing a simple
// addition, we now use Googletest to check the results.
TEST(PrimerExample1, ColumnAddTest) {
  // Declare two input arrays.
  int32 a[8] = {0, 1, 2, 3,  4, 5, 6,  7};
  int32 b[8] = {3, 4, 6, 8,  1, 2, 2,  9};

  // Expected output array.
  int32 c[8] = {3, 5, 8, 11, 5, 7, 8, 16};

  // Go!
  std::unique_ptr<BoundExpressionTree> expr(PrepareBoundExpression());
  const int32* result = AddColumns(a, b, 8, expr.get());

  // Test results.
  for (int i = 0; i < 8; i++) {
    ASSERT_EQ(c[i], result[i]) << "Error at position: " << i;
  }
}

// EXAMPLE 2 :: Grouping and aggregation.
//
// We will now move on to something still simple, but far more database-like
// - grouping and aggregation. In this example we will manipulate two column
// inputs, containing integer keys and double values. Our goal will be to sum
// the values referred to by the same keys. This time our worker method will
// return a Supersonic cursor.
//
// Before we start it might be worth mentioning operations. They can be thought
// of as higher level expressions which, unlike their simpler counterparts, do
// not necessarily have to operate on a single tuple. Common operations include
// grouping, sorting, joining and others. Notably, it is possible to promote
// an expression into an operation using Compute.
//
// In this example we will end up with two columns, whose row counts will
// depend on the input. Hence it will be more convenient to pass a *cursor*
// which will allow us to iterate over result views. The cursor will handle
// memory management for the views.
Cursor* GroupedSums(int32* keys, double* data, size_t row_number) {
  // Like before, we start off by creating a schema and an input view.
  TupleSchema schema;
  schema.add_attribute(Attribute("key", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("data", DOUBLE, NOT_NULLABLE));

  View input_view(schema);

  // The following line is important, row_count defaults to 0, so passing
  // data via Reset only will not suffice!
  input_view.set_row_count(row_number);
  input_view.mutable_column(0)->Reset(keys, NULL);
  input_view.mutable_column(1)->Reset(data, NULL);

  // It's time to describe the aggregation and we do so by using
  // an AggregationSpecification object to which we will append the desired
  // aggregation outcomes. In this simple case there will only be one
  // output, but the following tutorial tests will convey more interesting
  // aggregations. For each registered aggregation we need three things:
  // its type and the names of the input and output columns.
  std::unique_ptr<AggregationSpecification> specification(
      new AggregationSpecification());
  specification->AddAggregation(SUM, "data", "data_sums");

  // The specification will take care of the "right hand side" of grouping
  // operation, but we also need to know what to group by. This is where
  // column projectors come in handy. Once again we will only group by one
  // column this time, but it is possible to do it with several columns at the
  // same time using a CompoundSingleSourceProjector.
  std::unique_ptr<const SingleSourceProjector> key_projector(
      ProjectNamedAttribute("key"));

  // We've got the projector and specification taken care of, all we need now is
  // to create the aggregation and include the data source. Our data resides
  // in a view, but in general we might want to tap into another operation's
  // output. Therefore we need to create a trivial operation which will
  // simply read our view - this is done using ScanView.
  // The third argument is an aggregation option object which allows us to be
  // more specific about memory handling. Passing a NULL will cause Supersonic
  // to use a viable default.
  std::unique_ptr<Operation> aggregation(
      GroupAggregate(key_projector.release(), specification.release(), NULL,
                     ScanView(input_view)));

  // In order to have access to the aggregate results we create a cursor,
  // which will be our data iterator. The SucceedOrDie function will yank one
  // out of the FailureOrOwned<Cursor> wrapper if there has been no failure.
  std::unique_ptr<Cursor> bound_aggregation(
      SucceedOrDie(aggregation->CreateCursor()));

  return bound_aggregation.release();
}

TEST(PrimerExample2, GroupAggregateTest) {
  const unsigned size = 8;
  const unsigned key_count = 3;

  // Declare two input arrays.
  int32 a[size] =  {  1,   2,   3,   1,   2,   3,   1,   2};
  double b[size] = {1.5, 3.0, 3.0, 7.6, 5.5, 2.0, 1.6, 9.5};

  // Populate expected results.
  map<int32, double> result_map;
  result_map[1] = 0.0;
  result_map[2] = 0.0;
  result_map[3] = 0.0;

  for (unsigned i = 0; i < size; i++) {
    result_map[a[i]] += b[i];
  }

  // Here's the aggregation's result cursor.
  std::unique_ptr<Cursor> result_cursor(GroupedSums(a, b, size));

  // The Next() function conveys an iterator's typical behaviour. We specify
  // how many rows the resulting view should consist of, or pass -1 to fetch
  // as many as possible. It should be underlined that what we specify is merely
  // a maximum row count and there are no guarantees that we will get them all
  // at once - they may well be split up into separate views. We will explore
  // an example that does so in the next tutorial.
  ResultView result(result_cursor->Next(-1));

  // Make sure we have data to play with.
  ASSERT_TRUE(result.has_data());
  ASSERT_FALSE(result.is_eos());

  View result_view(result.view());

  // Check for data size. Despite the previous comment on cursor behaviour,
  // it should be safe to expect all rows in the single created cursor.
  // The size of this example is very small, hence the chances of them spilling
  // over to another view are next to none.
  EXPECT_EQ(2, result_view.column_count());
  EXPECT_EQ(key_count, result_view.row_count());

  // Check if column names are what we expect them to be.
  EXPECT_EQ("key", result_view.column(0).attribute().name());
  EXPECT_EQ("data_sums", result_view.column(1).attribute().name());

  // Check if aggregation results match.
  const int32* keys = result_view.column(0).typed_data<INT32>();
  const double* sums = result_view.column(1).typed_data<DOUBLE>();

  for (unsigned i = 0; i < key_count; i++) {
    EXPECT_EQ(result_map[keys[i]], sums[i]);
  }
}
