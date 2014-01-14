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

#ifndef SUPERSONIC_TESTING_OPERATION_TESTING_H_
#define SUPERSONIC_TESTING_OPERATION_TESTING_H_

#include <stddef.h>

#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/proto_matcher.h"
#include "gtest/gtest.h"
#include "supersonic/utils/random.h"

namespace supersonic {

#define ASSERT_PROTO_EQ(expected, actual)                                      \
  ASSERT_THAT(actual, testing::EqualsProto(expected))

class Block;
class InputWrapperOperation;
class TupleSchema;
class View;
class Cursor;

// A fixture for testing operations. It tests an operation for a combination
// of view sizes (both these requested from its parent, and these returned
// by its children).
//
// For single-argument operations, the usage pattern is:
//
// OperationTest test;
// test.SetInput(TestDataBuilder<...>().AddRow(...)....Build());
// test.SetExpectedResult(TestDataBuilder<...>().AddRow(...)....Build());
// test.Execute(CreateOperation(test.input(), parameters);
//
// The inputs and the expected result may have exceptions; use
// 'ReturnException(...)' instead of 'AddRow' when building the test data.
//
// For multi-argument operations (e.g. union), the usage pattern is:
//
// OperationTest test;
// test.AddInput(TestDataBuilder<...>().AddRow(...)....Build());
// test.AddInput(TestDataBuilder<...>().AddRow(...)....Build());
// ... (add the appropriate number of inputs)
// test.SetExpectedResult(TestDataBuilder<...>().AddRow(...)....Build());
// test.Execute(CreateOperation(test.input_at(0), test.input_at(1), ...,
//                              parameters);
//
// CreateOperation() is a function that returns a new instance of the tested
// operation. The ownership is passed to the test fixture.
//
// This class should reduce the need to test cursors on large input data
// (e.g. loop-generated inputs), as it runs the tests for max_row_count capped
// at small values (0, 1, 5, ...) in addition to large ones (up to maxint).
class OperationTest {
 public:
  OperationTest();
  ~OperationTest();

  // Adds a new input for the tested operation. For single-child operations
  // (a common case), you call this exactly once.
  void AddInput(Operation* input);

  // A convenience setter for operations that have exactly one input.
  void SetInput(Operation* input) {
    CHECK(inputs_.empty());
    AddInput(input);
  }

  // Sets the max_row_count for operation inputs. See Execute. If no arguments
  // are provided, resets to defaults.
  void SetInputViewSizes(
      size_t A = 0, size_t B = 0, size_t C = 0, size_t D = 0,
      size_t E = 0, size_t F = 0, size_t G = 0, size_t H = 0,
      size_t I = 0, size_t J = 0, size_t K = 0, size_t L = 0);

  // Sets the max_row_count for the operation result. See Execute. If no
  // arguments are provided, resets to defaults.
  void SetResultViewSizes(
      size_t A = 0, size_t B = 0, size_t C = 0, size_t D = 0,
      size_t E = 0, size_t F = 0, size_t G = 0, size_t H = 0,
      size_t I = 0, size_t J = 0, size_t K = 0, size_t L = 0);

  // Sets the expected result of the tested operation.
  void SetExpectedResult(Operation* expected) {
    expected_.reset(expected);
  }

  // Use it (instead of SetExpectedResult) when the operation is expected to
  // fail during bind.
  void SetExpectedBindFailure(ReturnCode error) {
    expected_bind_result_ = error;
  }

  // If true, will ignore row order when comparing obtained results to
  // expected.
  void SetIgnoreRowOrder(bool ignore_row_order) {
    ignore_row_order_ = ignore_row_order;
  }

  // If true, will not run tests that verify proper handling of the
  // WAITING_ON_BARRIER signal.
  void SkipBarrierHandlingChecks(bool skip_barrier_checks) {
    skip_barrier_checks_ = skip_barrier_checks;
  }

  // Sets the baseline buffer allocator that the test operation will use.
  // If NULL, replaced a default (heap allocator). In Execute(), we pass this
  // allocator to the tested operation, by calling
  // SetBufferAllocator(..., true). The ownership of the allocator
  // remains with the caller.
  void SetBufferAllocator(BufferAllocator* buffer_allocator) {
    buffer_allocator_ = (buffer_allocator == NULL)
        ? HeapBufferAllocator::Get()
        : buffer_allocator;
  }

  // For operations that take exactly one input.
  // Grabs the (wrapped) input, so that it can be passed to the tested
  // operation. Ownership is transferred to the caller. Must be called exactly
  // once.
  Operation* input() {
    CHECK_EQ(1, inputs_.size())
        << "Multiple inputs given; please disambiguate by calling "
        << "input_at(position) instead";
    return input_at(0);
  }

  // For operations that take multiple inputs (e.g. join, union).
  // Grabs the specified (wrapped) input, so that it can be passed to the tested
  // operation. Ownership is transferred to the caller. Must be called exactly
  // once for each input added.
  Operation* input_at(size_t position);

  // Runs the test on the specified operation. Performs the operation on the
  // input(s), and compares against the expected output. Runs a test for every
  // pair of (input view size, result view size), where the former bounds the
  // row count that the inputs return to the tested operation, and the latter
  // bounds the row count that is requested of the tested operation. The sizes
  // to use for the test can be set by SetInputViewSizes and SetResultViewSizes.
  // If not set, defaults are used (including some small and some large
  // numbers).
  void Execute(Operation* tested_operation);

 private:
  void ExecuteOnce(Operation* tested_operation,
                   size_t input_max_row_count,
                   size_t output_max_row_count,
                   double barrier_probability);

  friend class TestInput;
  vector<InputWrapperOperation*> inputs_;
  // Records which of the inputs have been acquired by the tested operation
  // (which delegates ownership). We expect the tested operation to claim
  // each input, exactly once.
  vector<bool> inputs_claimed_;
  // Expected result.
  std::unique_ptr<Operation> expected_;
  // Expected bind error, if any, or OK if no error expected.
  ReturnCode expected_bind_result_;
  // Whether row order should be ignored when comparing expected to actual.
  bool ignore_row_order_;
  // Whether to test if the operation propagates WAITING_ON_BARRIER correctly.
  bool skip_barrier_checks_;
  // Will be used as a baseline allocator for the tested operation. Defaults
  // to HeapBufferAllocator::Get().
  BufferAllocator* buffer_allocator_;
  // Used in Execute, to carry the information to the input operations
  // embedded in the user-created operation.
  vector<size_t> input_view_sizes_;
  vector<size_t> result_view_sizes_;
  MTRandom random_;
  DISALLOW_COPY_AND_ASSIGN(OperationTest);
};

// Creates a cursor that delegates to the supplied cursor, but caps
// max_row_count at the specified value.
Cursor* CreateViewLimiter(size_t capped_max_row_count, Cursor* delegate);

// An operation that will create cursors with the specified content,
// and optionally failing with the provided exception.
class TestData : public BasicOperation {
 public:

  // Takes ownership of the block.
  explicit TestData(Block* block) : table_(block) {}

  // Takes ownership of the block and the exception (if any).
  TestData(Block* block, const Exception* exception)
      : table_(block),
        exception_(exception) {}

  virtual ~TestData() {}

  virtual FailureOrOwned<Cursor> CreateCursor() const;

  const View& view() const { return table_.view(); }
  const TupleSchema& schema() const { return table_.schema(); }

 private:
  Table table_;
  std::unique_ptr<const Exception> exception_;
  DISALLOW_COPY_AND_ASSIGN(TestData);
};

// Not templated content of TestDataBuilder. Refer there for more information.
class AbstractTestDataBuilder {
 public:
  virtual ~AbstractTestDataBuilder() {}

  // Builds an operation that will create cursors over the stream.
  // Ownership of the result is passed to the caller.
  virtual TestData* Build() const = 0;

  Operation* OverwriteNamesAndBuild(const vector<string>& names) const;

  // Builds a cursor over the stream.
  // Ownership of the result is passed to the caller.
  Cursor* BuildCursor() const;

  // Builds a cursor over the stream.
  // Overrides column names with the given names.
  // Ownership of the result is passed to the caller.
  Cursor* BuildCursor(const vector<string>& names) const;
};

// TestDataBuilder that allows to create operations and cursors that return
// predefined output, and may return exceptions.
// Example usage:
//    scoped_ptr<Operation> test_data(
//      TestDataBuilder<STRING, INT32>()
//      .AddRow("first", 12)
//      .AddRow("second", __)
//      .Build());
// Creates an operation with two columns STRING and INT32, STRING column is not
// nullable (because all string values passed to AddRow are not null). INT32
// column is nullable, (because value in second row is NULL, represented by
// global constant __).
template <int A = UNDEF, int B = UNDEF, int C = UNDEF, int D = UNDEF,
          int E = UNDEF, int F = UNDEF, int G = UNDEF, int H = UNDEF,
          int I = UNDEF, int J = UNDEF, int K = UNDEF, int L = UNDEF,
          int M = UNDEF, int N = UNDEF, int O = UNDEF, int P = UNDEF,
          int R = UNDEF, int S = UNDEF, int T = UNDEF, int U = UNDEF>
class TestDataBuilder : public AbstractTestDataBuilder {
 public:
  typedef TestDataBuilder<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P,
                          R, S, T, U> This;
  TestDataBuilder() : block_builder_() {}

  explicit TestDataBuilder(const TupleSchema& schema)
      : block_builder_(schema) {}

  // Takes ownership of the factory.
  This& ReturnException(const ReturnCode error_code) {
    exception_.reset(new Exception(error_code, ""));
    return *this;
  }

  // Adds a row. Caller can use built-in types instead of ValueRef objects.
  // Number of arguments passed to AddRow must equal number of parameters passed
  // to CursorBuilder template. Global constant __ can be used for each value
  // that is null.
  This& AddRow(ValueRef<A> a = ValueRef<UNDEF>(),
               ValueRef<B> b = ValueRef<UNDEF>(),
               ValueRef<C> c = ValueRef<UNDEF>(),
               ValueRef<D> d = ValueRef<UNDEF>(),
               ValueRef<E> e = ValueRef<UNDEF>(),
               ValueRef<F> f = ValueRef<UNDEF>(),
               ValueRef<G> g = ValueRef<UNDEF>(),
               ValueRef<H> h = ValueRef<UNDEF>(),
               ValueRef<I> i = ValueRef<UNDEF>(),
               ValueRef<J> j = ValueRef<UNDEF>(),
               ValueRef<K> k = ValueRef<UNDEF>(),
               ValueRef<L> l = ValueRef<UNDEF>(),
               ValueRef<M> m = ValueRef<UNDEF>(),
               ValueRef<N> n = ValueRef<UNDEF>(),
               ValueRef<O> o = ValueRef<UNDEF>(),
               ValueRef<P> p = ValueRef<UNDEF>(),
               ValueRef<R> r = ValueRef<UNDEF>(),
               ValueRef<S> s = ValueRef<UNDEF>(),
               ValueRef<T> t = ValueRef<UNDEF>(),
               ValueRef<U> u = ValueRef<UNDEF>()) {
    block_builder_.AddRow(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, r, s,
                          t, u);
    return *this;
  }

  virtual TestData* Build() const {
    return exception_.get() == NULL
        ? new TestData(block_builder_.Build())
        : new TestData(block_builder_.Build(), exception_->Clone());
  }

  // Force a column to be nullable, even though no element of that column is
  // NULL.
  This& ForceNullable(int column_number) {
    CHECK_GE(column_number, 0);
    CHECK_LT(column_number, 20);
    block_builder_.ForceNullable(column_number);
    return *this;
  }

 private:
  BlockBuilder<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, R, S, T, U>
      block_builder_;
  // Shared w/ built TestData instances.
  std::unique_ptr<const Exception> exception_;

  DISALLOW_COPY_AND_ASSIGN(TestDataBuilder);
};

// A shorthand function that calls project to rename all the columns in the
// cursor.
// Takes ownership of input cursor.
Cursor* RenameAttributesInCursorAs(const vector<string>& new_names,
                                   Cursor* input);

// Same with above, but for operations.
Operation* RenameAttributesInOperationAs(const vector<string>& new_names,
                                         Operation* input);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_OPERATION_TESTING_H_
