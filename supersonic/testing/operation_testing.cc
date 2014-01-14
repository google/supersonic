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

#include "supersonic/testing/operation_testing.h"

#include <cstddef>
#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <memory>

#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/core/ownership_taker.h"
#include "supersonic/cursor/core/project.h"
#include "supersonic/cursor/core/scan_view.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/testing/comparators.h"
#include "gtest/gtest.h"
#include "supersonic/utils/random.h"

DEFINE_int32(random_seed, 0, "Seed used for RNG during testing.");

namespace supersonic {

namespace {

Cursor* Sort(Cursor* input) {
  SortOrder sort_order;
  sort_order.add(ProjectAllAttributes(), ASCENDING);
  std::unique_ptr<const BoundSortOrder> bound_sort_order(
      SucceedOrDie(sort_order.Bind(input->schema())));
  std::unique_ptr<const SingleSourceProjector> result_projector(
      ProjectAllAttributes());
  std::unique_ptr<const BoundSingleSourceProjector> bound_result_projector(
      SucceedOrDie(result_projector->Bind(input->schema())));
  return SucceedOrDie(BoundSort(
      bound_sort_order.release(),
      bound_result_projector.release(),
      1 << 19,  // 0.5 MB
      "",
      HeapBufferAllocator::Get(),
      input));
}

// Decorator that limits max_row_count requested of the delegate.
class ViewLimiter : public BasicDecoratorCursor {
 public:
  ViewLimiter(size_t capped_max_row_count, Cursor* delegate)
      : BasicDecoratorCursor(delegate),
        capped_max_row_count_(capped_max_row_count) {
    CHECK_GT(capped_max_row_count, 0);
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    DCHECK_GT(max_row_count, 0);
    if (max_row_count > capped_max_row_count_) {
      max_row_count = capped_max_row_count_;
    }
    return delegate()->Next(max_row_count);
  }

  virtual void Interrupt() { delegate()->Interrupt(); }

  virtual CursorId GetCursorId() const { return VIEW_LIMITER; }

 private:
  size_t capped_max_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ViewLimiter);
};

// Decorator that injects random WAITING_ON_BARRIER signals into the stream.
class BarrierInjector : public BasicDecoratorCursor {
 public:
  BarrierInjector(RandomBase* random,
                  double barrier_probability,
                  Cursor* delegate)
      : BasicDecoratorCursor(delegate),
        random_(random),
        barrier_probability_(barrier_probability) {
    CHECK_NOTNULL(random);
  }

  virtual ResultView Next(rowcount_t max_row_count) {
    if (random_->RandDouble() <= barrier_probability_) {
      return ResultView::WaitingOnBarrier();
    } else {
      return delegate()->Next(max_row_count);
    }
  }

  virtual CursorId GetCursorId() const { return BARRIER_INJECTOR; }

 private:
  RandomBase* const random_;
  double barrier_probability_;
  DISALLOW_COPY_AND_ASSIGN(BarrierInjector);
};

// Decorator that consumes any WAITING_ON_BARRIER signals seen in the stream.
// (It is intended for use with BarrierInjector; i.e. it assumes that retrying
// does eventually unblock the stream).
class BarrierSwallower : public BasicDecoratorCursor {
 public:
  BarrierSwallower(int retry_limit,
                   Cursor* input)
      : BasicDecoratorCursor(input),
        retry_limit_(retry_limit) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    for (int retries = 0; retries < retry_limit_; ++retries) {
      ResultView result = delegate()->Next(max_row_count);
      if (!result.is_waiting_on_barrier()) return result;
    }
    LOG(FATAL)
        << "Too many successive WaitingOnBarriers (" << retry_limit_
        << "); giving up";
  }

  virtual CursorId GetCursorId() const { return BARRIER_SWALLOWER; }

 private:
  // Cirtuit-breaker, to crash (rather than loop infinitely) if it turns out
  // that WAITING_ON_BARRIER is persistent, i.e. apparently not caused by
  // random barriers injected by a downstream BarrierInjector.
  const int retry_limit_;
  DISALLOW_COPY_AND_ASSIGN(BarrierSwallower);
};

class Counter {
 public:
  Counter() : value_(0) {}
  int value() const { return value_; }
  void Increment() { ++value_; }
 private:
  int value_;
};

// Used to verify if the tested operation propagates interruption down the tree.
// Decorator that catches interruption requests and increments the associated
// counter. The counter is also incremented if the cursor detects EOS or
// failure (in this case, a future interruption request wouldn't matter,
// we thus can't fault the caller for not propagating it).
class InterruptionCounter : public BasicDecoratorCursor {
 public:
  InterruptionCounter(Counter* counter, Cursor* input)
      : BasicDecoratorCursor(input),
        marked_(false),
        counter_(counter) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    ResultView result = delegate()->Next(max_row_count);
    if (result.is_done()) mark();
    return result;
  }

  virtual void Interrupt() {
    mark();
    delegate()->Interrupt();
  }

  virtual CursorId GetCursorId() const { return INTERRUPTION_COUNTER; }

 private:
  void mark() {
    if (marked_) return;
    counter_->Increment();
    marked_ = true;
  }

  bool marked_;
  Counter* counter_;
  DISALLOW_COPY_AND_ASSIGN(InterruptionCounter);
};

class TestCursor : public BasicDecoratorCursor {
 public:
  // The view must outlive the cursor. Exception can be NULL. If it is
  // specified, the ownership of it remains with the caller (and it must
  // outlive the cursor).
  TestCursor(const View& view, Exception* exception)
      : BasicDecoratorCursor(BoundScanView(view)),
        exception_(exception),
        done_(false) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    CHECK_GT(max_row_count, 0);
    CHECK(!done_) << "Another Next() called after EOS or exception";
    ResultView result = delegate()->Next(max_row_count);
    if (result.has_data()) {
      // Check if the cursor doesn't violate the contract.
      DCHECK_LE(result.view().row_count(), max_row_count);
      DCHECK_GT(result.view().row_count(), 0);
      return result;
    }
    if (result.is_failure()) {
      EXPECT_EQ(INTERRUPTED, result.exception().return_code());
      return result;
    }
    CHECK(result.is_eos());
    done_ = true;
    if (exception_.get() == NULL) {
      return ResultView::EOS();
    } else {
      return ResultView::Failure(exception_.release());
    }
  }

  virtual CursorId GetCursorId() const { return TEST_DECORATOR; }

 private:
  std::unique_ptr<Exception> exception_;
  bool done_;
  DISALLOW_COPY_AND_ASSIGN(TestCursor);
};

// Decorates cursor with some features that make debugging easier:
// - the data is deep-copied from the original block, so mistaken shallow
//   copies are easier to detect
// - block_.ResetArenas() is called on every Next()
class DeepCopyingCursor : public BasicDecoratorCursor {
 public:
  DeepCopyingCursor(Cursor* cursor, BufferAllocator* allocator)
      : BasicDecoratorCursor(cursor),
        block_(delegate()->schema(), allocator),
        view_(block_.schema()),
        deep_copier_(block_.schema(), true) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    block_.ResetArenas();

    ResultView result = delegate()->Next(max_row_count);
    if (result.has_data()) {
      rowcount_t result_rowcount = result.view().row_count();
      if (block_.row_capacity() < result_rowcount) {
        if (!block_.Reallocate(result_rowcount)) {
          THROW(new Exception(
              ERROR_MEMORY_EXCEEDED,
              "Cannot allocate memory in DeepCopyingCursor (1)."));
        }
      }
      rowcount_t copy_result =
          deep_copier_.Copy(result_rowcount, result.view(), 0, &block_);
      if (copy_result < result_rowcount) {
        THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            "Cannot allocate memory in DeepCopyingCursor (2)."));
      }
      view_.ResetFromSubRange(block_.view(), 0, result_rowcount);
      return ResultView::Success(&view_);
    } else {
      return result;
    }
  }

  virtual CursorId GetCursorId() const { return DEEP_COPYING; }

 private:
  Block block_;
  View view_;
  ViewCopier deep_copier_;
};

// Default value for retries on 'waiting on barrier' before we assume that
// something went wrong and we're in an infinite loop. (It's used with
// BarrierInjector, which generates barrier signals with some probability.
// With probabiity < 0.999 and 1e5 repetitions, the likelihood that it will
// not generate any non-barrier is 4e-44. We give it another order of magnitude
// to be on the safe side ;)
const int kMaxBarrierRetries = 1e6;

}  // namespace

Cursor* CreateViewLimiter(size_t capped_max_row_count, Cursor* delegate) {
  return new ViewLimiter(capped_max_row_count, delegate);
}

// Wraps an operation
class InputWrapperOperation : public BasicOperation {
 public:
  InputWrapperOperation(Operation* child, RandomBase* random)
      : child_(child),
        capped_max_row_count_(std::numeric_limits<size_t>::max()),
        random_(random),
        barrier_probability_(0),
        interruption_counter_(NULL) {}
  virtual ~InputWrapperOperation() {}
  virtual FailureOrOwned<Cursor> CreateCursor() const {
    FailureOrOwned<Cursor> child = child_->CreateCursor();
    PROPAGATE_ON_FAILURE(child);
    std::unique_ptr<Cursor> cursor(child.release());
    if (barrier_probability_ > 0) {
      cursor.reset(new BarrierInjector(random_, barrier_probability_,
                                       cursor.release()));
    }
    if (interruption_counter_ != NULL) {
      cursor.reset(new InterruptionCounter(interruption_counter_,
                                           cursor.release()));
    }
    cursor.reset(new DeepCopyingCursor(cursor.release(), buffer_allocator()));
    return Success(new ViewLimiter(capped_max_row_count_, cursor.release()));
  }

  void set_capped_max_row_count(size_t capped_max_row_count) {
    capped_max_row_count_ = capped_max_row_count;
  }
  void set_barrier_probability(double barrier_probability) {
    barrier_probability_ = barrier_probability;
  }
  void set_interruption_counter(Counter* counter) {
    interruption_counter_ = counter;
  }

 private:
  std::unique_ptr<Operation> child_;
  size_t capped_max_row_count_;
  RandomBase* const random_;
  double barrier_probability_;

  // If not NULL, the operation will wrap cursors with a InterruptionCounter,
  // passing this object. This way, the counter will count interruption
  // notifications from all inputs created by the operation once the
  // variable is set.
  Counter* interruption_counter_;
  DISALLOW_COPY_AND_ASSIGN(InputWrapperOperation);
};

// Test with some bigger size first, so we get more info when the test fails on
// the first run (with 1 we would see output rows only up to the first
// unexpected row).
static size_t default_view_sizes[] = {
    20, 1, 2, 5, 10001, std::numeric_limits<size_t>::max()
};

OperationTest::OperationTest()
    : inputs_(),
      expected_bind_result_(OK),
      ignore_row_order_(false),
      skip_barrier_checks_(false),
      buffer_allocator_(HeapBufferAllocator::Get()),
      input_view_sizes_(default_view_sizes,
                        default_view_sizes + arraysize(default_view_sizes)),
      result_view_sizes_(default_view_sizes,
                         default_view_sizes + arraysize(default_view_sizes)),
      random_(FLAGS_random_seed) {}

OperationTest::~OperationTest() {
  for (int i = 0; i < inputs_.size(); ++i) {
    if (!inputs_claimed_[i]) delete inputs_[i];
  }
}

void OperationTest::AddInput(Operation* input) {
  inputs_.push_back(new InputWrapperOperation(input, &random_));
  inputs_claimed_.push_back(false);
}

Operation* OperationTest::input_at(size_t position) {
  CHECK_LT(position, inputs_.size()) << "There aren't that many inputs";
  CHECK(!inputs_claimed_[position])
      << "The input " << position << " has already been used up.";
  inputs_claimed_[position] = true;
  return inputs_[position];
}

static void SetSizes(const size_t* sizes,
                     size_t length,
                     vector<size_t>* target) {
  target->clear();
  for (int i = 0; i < length; ++i) {
    if (sizes[i] == 0) return;
    target->push_back(sizes[i]);
  }
}

void OperationTest::SetInputViewSizes(
    size_t A, size_t B, size_t C, size_t D, size_t E, size_t F,
    size_t G, size_t H, size_t I, size_t J, size_t K, size_t L) {
  size_t sizes[] = { A, B, C, D, E, F, G, H, I, J, K, L };
  SetSizes(sizes, arraysize(sizes), &input_view_sizes_);
}

void OperationTest::SetResultViewSizes(
    size_t A, size_t B, size_t C, size_t D, size_t E, size_t F,
    size_t G, size_t H, size_t I, size_t J, size_t K, size_t L) {
  size_t sizes[] = { A, B, C, D, E, F, G, H, I, J, K, L };
  SetSizes(sizes, arraysize(sizes), &result_view_sizes_);
}

// Tests the specified operation while capping the views that its input
// cursors feed to it at input_max_row_count, and capping the max_row_count
// passed to it by its parent at output_max_row_count.
void OperationTest::ExecuteOnce(Operation* tested_operation,
                                size_t input_max_row_count,
                                size_t output_max_row_count,
                                double barrier_probability) {
  VLOG(1)
      << "Testing operation '" << tested_operation->DebugDescription()
      << "' with input_max_row_count = " << input_max_row_count << ", "
      << "output_max_row_count = " << output_max_row_count << ", "
      << "barrier_probability = "  << barrier_probability;

  // We're assuming that the tested operation uses our input() as its child.
  for (int i = 0; i < inputs_.size(); ++i) {
    inputs_[i]->set_capped_max_row_count(input_max_row_count);
    inputs_[i]->set_barrier_probability(barrier_probability);
  }
  FailureOrOwned<Cursor> tested_cursor(tested_operation->CreateCursor());
  ASSERT_TRUE(tested_cursor.is_success())
      << tested_cursor.exception().PrintStackTrace();
  Cursor* tested = new BarrierSwallower(
      kMaxBarrierRetries,
      new ViewLimiter(output_max_row_count,
                      tested_cursor.release()));

  FailureOrOwned<Cursor> expected_cursor = expected_->CreateCursor();
  ASSERT_TRUE(expected_cursor.is_success())
      << tested_cursor.exception().PrintStackTrace();
  Cursor* expected = expected_cursor.release();

  if (ignore_row_order_) {
    tested = Sort(tested);
    expected = Sort(expected);
  }

  ASSERT_CURSORS_EQUAL(expected, tested)
      << "When iterating operation ''" << tested_operation->DebugDescription()
      << "' with input_max_row_count = " << input_max_row_count
      << " and output_max_row_count = " << output_max_row_count
      << " and barrier_probability = " << barrier_probability;
}

void OperationTest::Execute(Operation* tested_operation) {
  MemoryLimit tracker(std::numeric_limits<size_t>::max(), true,
                      buffer_allocator_);
  std::unique_ptr<Operation> deleter(tested_operation);
  tested_operation->SetBufferAllocator(&tracker, true);
  for (int i = 0; i < inputs_.size(); ++i) {
    CHECK(inputs_claimed_[i])
        << "The input " << i << " has not been used by the tested operation.";
  }
  if (expected_bind_result_ != OK) {
    FailureOrOwned<Cursor> tested_cursor(tested_operation->CreateCursor());
    ASSERT_TRUE(tested_cursor.is_failure())
        << "Cursor creation succeeded, but was expected to fail w/ "
        << ReturnCode_Name(expected_bind_result_);
    ASSERT_EQ(expected_bind_result_, tested_cursor.exception().return_code())
        << "Cursor creation failed (as expected), but with an unexpected "
        << "return code ("
        << ReturnCode_Name(tested_cursor.exception().return_code())
        << " instead of "
        << ReturnCode_Name(expected_bind_result_) << ")";
    return;
  }

  for (size_t i = 0; i < input_view_sizes_.size(); ++i) {
    for (size_t j = 0; j < result_view_sizes_.size(); ++j) {
      ExecuteOnce(tested_operation,
                  input_view_sizes_[i],
                  result_view_sizes_[j],
                  0.0);
      // TODO(user): It would be useful to know if the test fails the same way
      // regardless of view sizes, or whether failure is dependent of view size.
      if (testing::Test::HasFatalFailure()) return;
    }
  }

  // Memory release checks.
  CHECK_EQ(0, tracker.GetUsage());
  {
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
  }
  ASSERT_EQ(0, tracker.GetUsage())
      << "Cursor constructor leaked " << tracker.GetUsage() << " bytes";

  {
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
    test->Next(1);
  }
  ASSERT_EQ(0, tracker.GetUsage())
      << "Cursor constructor followed by Next(1) leaked "
      << tracker.GetUsage() << " bytes";

  {
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
    test->Next(Cursor::kDefaultRowCount);
  }
  ASSERT_EQ(0, tracker.GetUsage())
      << "Cursor constructor followed by Next(kDefaultRowCount) leaked "
      << tracker.GetUsage() << " bytes";

  {
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
    if (test->Next(Cursor::kDefaultRowCount).has_data()) {
      test->Next(Cursor::kDefaultRowCount);
    }
  }
  ASSERT_EQ(0, tracker.GetUsage())
      << "Cursor constructor followed by 2 x Next(kDefaultRowCount) leaked "
      << tracker.GetUsage() << " bytes";

  {
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
    test->Next(std::numeric_limits<rowcount_t>::max());
  }
  ASSERT_EQ(0, tracker.GetUsage())
      << "Cursor constructor followed by Next(maxint) leaked "
      << tracker.GetUsage() << " bytes";

  if (!skip_barrier_checks_) {
    // Testing propagation of WAITING_ON_BARRIER.
    std::unique_ptr<Cursor> test(
        SucceedOrDie(tested_operation->CreateCursor()));
    ASSERT_TRUE(test->IsWaitingOnBarrierSupported());
    ExecuteOnce(tested_operation, 1, 1, 0.1);
    if (testing::Test::HasFatalFailure()) return;
    ExecuteOnce(tested_operation, 1, 1, 0.6);
    if (testing::Test::HasFatalFailure()) return;
    ExecuteOnce(tested_operation, 1, 1, 0.99);
    if (testing::Test::HasFatalFailure()) return;
    ExecuteOnce(tested_operation, 2, 2, 0.5);
    if (testing::Test::HasFatalFailure()) return;
  } else {
    LOG(WARNING) << "Skipping barrier checks";
  }

  // Test propagation of interruptions.
  Counter interruption_counter;
  for (int i = 0; i < inputs_.size(); ++i) {
    inputs_[i]->set_interruption_counter(&interruption_counter);
    inputs_[i]->set_capped_max_row_count(1);
  }
  std::unique_ptr<Cursor> test(new BarrierSwallower(
      kMaxBarrierRetries, SucceedOrDie(tested_operation->CreateCursor())));
  for (int i = 0; i < inputs_.size(); ++i) {
    inputs_[i]->set_interruption_counter(NULL);
  }

  std::unique_ptr<Cursor> expected(SucceedOrDie(expected_->CreateCursor()));
  test->Interrupt();
  // Should propagate to all children.
  EXPECT_EQ(inputs_.size(), interruption_counter.value());
}

FailureOrOwned<Cursor> TestData::CreateCursor() const {
  return Success(new TestCursor(
      table_.view(),
      exception_.get() != NULL ? exception_->Clone() : NULL));
}

// --------------- AbstractTestDataBuilder -------------------------------------

Operation* AbstractTestDataBuilder::OverwriteNamesAndBuild(
    const vector<string>& names) const {
  return Project(ProjectRename(names, ProjectAllAttributes()),
                 Build());
}

// Builds a cursor over the stream.
// Ownership of the result is passed to the caller.
Cursor* AbstractTestDataBuilder::BuildCursor() const {
  return SucceedOrDie(TurnIntoCursor(Build()));
}

// Builds a cursor over the stream.
// Overrides column names with the given names.
// Ownership of the result is passed to the caller.
Cursor* AbstractTestDataBuilder::BuildCursor(const vector<string>& names)
    const {
  return SucceedOrDie(TurnIntoCursor(OverwriteNamesAndBuild(names)));
}

Cursor* RenameAttributesInCursorAs(const vector<string>& new_names,
                                   Cursor* input) {
  CHECK_EQ(new_names.size(), input->schema().attribute_count());
  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(input->schema()));
  for (size_t i = 0; i < new_names.size(); ++i) {
    projector->AddAs(i, new_names[i]);
  }
  return BoundProject(projector.release(), input);
}

Operation* RenameAttributesInOperationAs(const vector<string>& new_names,
                                         Operation* input) {
  return Project(ProjectRename(new_names, ProjectAllAttributes()), input);
}

}  // namespace supersonic
