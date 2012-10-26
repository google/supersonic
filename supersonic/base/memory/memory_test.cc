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

#include "supersonic/base/memory/memory.h"

#include <cstring>                      // for NULL, memset

#include "supersonic/utils/scoped_ptr.h"            // for scoped_ptr
#include "supersonic/base/memory/memory_mocks.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"  // for EXPECT_EQ, TEST, etc

namespace supersonic {

TEST(BufferAllocatorTest, TrivialAllocatorAllocates) {
  scoped_ptr<Buffer> buffer(HeapBufferAllocator::Get()->Allocate(1000));
  EXPECT_EQ(1000, buffer->size());
  memset(buffer->data(), 34, buffer->size());
}

// TODO(onufry): add tests covering the case where HeapBufferAllocator fails to
// allocate due to OOM.

TEST(BufferAllocatorTest, ClearingAllocatorClears) {
  ClearingBufferAllocator allocator(HeapBufferAllocator::Get());
  scoped_ptr<Buffer> buffer(allocator.Allocate(10));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(0, static_cast<char*>(buffer->data())[i]);
    static_cast<char*>(buffer->data())[i] = i + 100;
  }

  ASSERT_TRUE(allocator.Reallocate(20, buffer.get()));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(i + 100, static_cast<char*>(buffer->data())[i]);
  }
  for (int i = 10; i < 20; ++i) {
    EXPECT_EQ(0, static_cast<char*>(buffer->data())[i]);
  }
}

TEST(BufferAllocatorTest, SharedQuotaAllocatorShouldAllocate) {
  StaticQuota<false> quota(1000);
  scoped_ptr<BufferAllocator> allocator(new MediatingBufferAllocator(
      HeapBufferAllocator::Get(), &quota));
  {
    EXPECT_EQ(0, quota.GetUsage());
    EXPECT_EQ(1000, allocator->Available());
    scoped_ptr<Buffer> buffer1(allocator->BestEffortAllocate(450, 100));
    ASSERT_EQ(450, buffer1->size());
    EXPECT_EQ(450, quota.GetUsage());
    EXPECT_EQ(550, allocator->Available());
    {
      scoped_ptr<Buffer> buffer2(allocator->BestEffortAllocate(350, 100));
      ASSERT_EQ(350, buffer2->size());
      EXPECT_EQ(800, quota.GetUsage());
      EXPECT_EQ(200, allocator->Available());
      scoped_ptr<Buffer> buffer3(allocator->BestEffortAllocate(500, 100));
      ASSERT_EQ(200, buffer3->size());
      EXPECT_EQ(1000, quota.GetUsage());
      EXPECT_EQ(0, allocator->Available());
      scoped_ptr<Buffer> buffer4(allocator->BestEffortAllocate(500, 100));
      ASSERT_EQ(NULL, buffer4.get());
      EXPECT_EQ(0, allocator->Available());
    }
    // Now, the last 3 allocations should be undone.
    EXPECT_EQ(450, quota.GetUsage());
    EXPECT_EQ(550, allocator->Available());
  }
  // Now, all the allocations should be undone.
  EXPECT_EQ(0, quota.GetUsage());
  EXPECT_EQ(1000, allocator->Available());
  scoped_ptr<Buffer> buffer5(allocator->BestEffortAllocate(1200, 800));
  EXPECT_EQ(1000, buffer5->size());
  EXPECT_EQ(1000, quota.GetUsage());
  EXPECT_EQ(0, allocator->Available());
  // Now, resize the quota.
  quota.SetQuota(1200);
  EXPECT_EQ(200, allocator->Available());
  scoped_ptr<Buffer> buffer6(allocator->BestEffortAllocate(1200, 100));
  EXPECT_EQ(200, buffer6->size());
  EXPECT_EQ(1200, quota.GetUsage());
  EXPECT_EQ(0, allocator->Available());
}

// This test tests a corner case - when the underlying allocator grants less
// memory, than would be permitted by the quota, but more than minimal. This
// actually combines two quotas in a pretty nonsensical fashion, but still
// should be handled.
TEST(BufferAllocatorTest, QuotaAllocatorWorksWithMemoryLimit) {
  StaticQuota<false> quota(100);
  scoped_ptr<BufferAllocator> allocator(new MemoryLimit(50));
  scoped_ptr<BufferAllocator> outer_allocator(new MediatingBufferAllocator(
      allocator.get(), &quota));

  EXPECT_EQ(0, quota.GetUsage());
  scoped_ptr<Buffer> buffer(outer_allocator->BestEffortAllocate(80, 20));
  // We expect the internal quota to be respected.
  EXPECT_LT(quota.GetUsage(), 51);
  EXPECT_GT(quota.GetUsage(), 19);
}

TEST(BufferAllocatorTest, MaxSizeTAllocateRequestFailsGracefully) {
  // A soft/unenforced quota.
  StaticQuota<false> quota(1000, false);
  scoped_ptr<BufferAllocator> allocator(new MediatingBufferAllocator(
      HeapBufferAllocator::Get(), &quota));
  EXPECT_EQ(1000, allocator->Available());
  scoped_ptr<Buffer> buffer1(allocator->BestEffortAllocate(400, 100));
  EXPECT_EQ(400, buffer1->size());
  EXPECT_EQ(600, allocator->Available());
  const size_t max_size_t = numeric_limits<size_t>::max();
  scoped_ptr<Buffer> buffer2(allocator->Allocate(max_size_t));
  EXPECT_TRUE(buffer2.get() == NULL);
  EXPECT_EQ(600, allocator->Available());
}

TEST(BufferAllocatorTest, SoftQuotaAllocateSucceedsEvenWhenAvailableIs0) {
  // A soft/unenforced quota.
  StaticQuota<false> quota(1000, false);
  scoped_ptr<BufferAllocator> allocator(new MediatingBufferAllocator(
      HeapBufferAllocator::Get(), &quota));
  EXPECT_EQ(1000, allocator->Available());
  scoped_ptr<Buffer> buffer1(allocator->BestEffortAllocate(400, 100));
  EXPECT_EQ(400, buffer1->size());
  EXPECT_EQ(600, allocator->Available());
  scoped_ptr<Buffer> buffer2(allocator->BestEffortAllocate(400, 100));
  EXPECT_EQ(400, buffer2->size());
  EXPECT_EQ(200, allocator->Available());
  EXPECT_EQ(800, quota.GetUsage());
  {
    scoped_ptr<Buffer> buffer3(allocator->BestEffortAllocate(400, 100));
    EXPECT_EQ(200, buffer3->size());
    EXPECT_EQ(0, allocator->Available());
    EXPECT_EQ(1000, quota.GetUsage());
    // Available() is 0, but this is a soft/unenforced quota, so we should be
    // able to allocate anyway.
    scoped_ptr<Buffer> buffer4(allocator->BestEffortAllocate(400, 100));
    EXPECT_EQ(100, buffer4->size());
    EXPECT_EQ(0, allocator->Available());
    EXPECT_EQ(1100, quota.GetUsage());
  }
  EXPECT_EQ(200, allocator->Available());
  EXPECT_EQ(800, quota.GetUsage());
}

TEST(BufferAllocatorTest, CompoundSoftHardQuota) {
  StaticQuota<false> hard_quota(1000, true);  // enforced
  StaticQuota<false> soft_quota(500, false);  // not enforced
  scoped_ptr<BufferAllocator> hard_allocator(new MediatingBufferAllocator(
      HeapBufferAllocator::Get(), &hard_quota));
  scoped_ptr<BufferAllocator> compound_allocator(new MediatingBufferAllocator(
      hard_allocator.get(), &soft_quota));
  EXPECT_EQ(500, compound_allocator->Available());
  scoped_ptr<Buffer> buffer1(compound_allocator->BestEffortAllocate(800, 400));
  EXPECT_EQ(500, buffer1->size());
  EXPECT_EQ(0, compound_allocator->Available());
  scoped_ptr<Buffer> buffer2(compound_allocator->BestEffortAllocate(800, 400));
  EXPECT_EQ(400, buffer2->size());
  EXPECT_EQ(0, compound_allocator->Available());
  scoped_ptr<Buffer> buffer3(compound_allocator->BestEffortAllocate(800, 400));
  EXPECT_EQ(NULL, buffer3.get());
}

TEST(BufferAllocatorTest, AvailableInMemoryLimit) {
  MemoryLimit limit(1000);
  EXPECT_EQ(1000, limit.Available());
  scoped_ptr<Buffer> buffer(limit.Allocate(300));
  EXPECT_EQ(700, limit.Available());
}

TEST(BufferAllocatorTest, ReallocShouldAdjustQuota) {
  MemoryLimit limit1(1000);
  MemoryLimit limit2(5000, &limit1);
  scoped_ptr<Buffer> buffer(limit2.Allocate(300));
  EXPECT_EQ(300, limit2.GetUsage());
  EXPECT_EQ(700, limit2.Available());
  ASSERT_TRUE(limit2.Reallocate(500, buffer.get()));
  EXPECT_EQ(500, limit2.GetUsage());
  EXPECT_EQ(500, limit2.Available());
  // We expect the realloc below to fail, as the mediator is conservative -
  // assuming that realloc might degenerate to create-copy-free, so reserving
  // the entire 'requested' initially.
  ASSERT_FALSE(limit2.BestEffortReallocate(700, 600, buffer.get()));
  EXPECT_EQ(500, limit2.GetUsage());
  EXPECT_EQ(500, limit2.Available());
}

TEST(BufferAllocatorTest, AllocatingEmptyAlwaysSucceeds) {
  MemoryLimit limit(0);
  scoped_ptr<Buffer> buffer1(limit.BestEffortAllocate(300, 10));
  EXPECT_TRUE(buffer1.get() == NULL);
  scoped_ptr<Buffer> buffer2(limit.BestEffortAllocate(300, 0));
  ASSERT_TRUE(buffer2.get() != NULL);
  EXPECT_EQ(0, buffer2->size());
  scoped_ptr<Buffer> buffer3(limit.Allocate(0));
  ASSERT_TRUE(buffer3.get() != NULL);
  EXPECT_EQ(0, buffer3->size());
}

TEST(BufferAllocatorTest, ReallocatingEmptyAlwaysSucceeds) {
  MemoryLimit limit(300);
  scoped_ptr<Buffer> buffer1(limit.BestEffortAllocate(400, 300));
  ASSERT_TRUE(buffer1.get() != NULL);
  EXPECT_EQ(300, buffer1->size());
  EXPECT_TRUE(limit.BestEffortReallocate(100, 0, buffer1.get()) != NULL);
  EXPECT_EQ(0, buffer1->size());
  EXPECT_EQ(300, limit.GetQuota());
  EXPECT_TRUE(limit.Reallocate(0, buffer1.get()) != NULL);
  EXPECT_EQ(0, buffer1->size());
  EXPECT_EQ(300, limit.GetQuota());
}

TEST(BufferAllocatorTest, SoftQuotaBypassing) {
  MemoryLimit hard_quota(1000);
  MemoryLimit soft_quota(100, false, &hard_quota);
  SoftQuotaBypassingBufferAllocator lifted_quota(&soft_quota, 200);

  scoped_ptr<Buffer> buffer1(lifted_quota.BestEffortAllocate(50, 20));
  ASSERT_TRUE(buffer1.get() != NULL);
  EXPECT_EQ(50, buffer1->size());
  scoped_ptr<Buffer> buffer2(lifted_quota.BestEffortAllocate(100, 20));
  ASSERT_TRUE(buffer2.get() != NULL);
  EXPECT_EQ(100, buffer2->size());
  scoped_ptr<Buffer> buffer3(lifted_quota.BestEffortAllocate(100, 20));
  ASSERT_TRUE(buffer3.get() != NULL);
  EXPECT_EQ(50, buffer3->size());
}

TEST(BufferAllocatorTest, SoftQuotaBypassingWithTightHardQuota) {
  MemoryLimit hard_quota(100);
  SoftQuotaBypassingBufferAllocator lifted_quota(&hard_quota, 200);

  scoped_ptr<Buffer> buffer1(lifted_quota.BestEffortAllocate(50, 20));
  ASSERT_TRUE(buffer1.get() != NULL);
  EXPECT_EQ(50, buffer1->size());
  scoped_ptr<Buffer> buffer2(lifted_quota.BestEffortAllocate(100, 20));
  ASSERT_TRUE(buffer2.get() != NULL);
  EXPECT_EQ(50, buffer2->size());
  scoped_ptr<Buffer> buffer3(lifted_quota.BestEffortAllocate(100, 20));
  EXPECT_TRUE(buffer3.get() == NULL);
}

TEST(BufferAllocatorTest, GuaranteeMemoryTest) {
  MemoryLimit upper_limit(100);
  GuaranteeMemory less_memory(50, &upper_limit);
  EXPECT_EQ(50, less_memory.Available());
  {
    scoped_ptr<Buffer> buffer1(less_memory.Allocate(30));
    ASSERT_TRUE(buffer1.get() != NULL);
    EXPECT_EQ(30, buffer1->size());
    EXPECT_EQ(20, less_memory.Available());
    ASSERT_TRUE(less_memory.Allocate(30) == NULL);
    EXPECT_EQ(20, less_memory.Available());
    ASSERT_TRUE(less_memory.BestEffortAllocate(30, 10) == NULL);
    scoped_ptr<Buffer> buffer2(less_memory.Allocate(20));
    EXPECT_EQ(20, buffer2->size());
    EXPECT_EQ(0, less_memory.Available());
  }
  EXPECT_EQ(50, less_memory.Available());
  GuaranteeMemory more_memory(150, &upper_limit);
  EXPECT_EQ(150, more_memory.Available());
  {
    scoped_ptr<Buffer> buffer1(more_memory.Allocate(70));
    ASSERT_TRUE(buffer1.get() != NULL);
    EXPECT_EQ(70, buffer1->size());
    EXPECT_EQ(80, more_memory.Available());
    ASSERT_TRUE(more_memory.BestEffortAllocate(40, 10) == NULL);
    EXPECT_EQ(80, more_memory.Available());
    ASSERT_TRUE(more_memory.Allocate(40) == NULL);
    EXPECT_EQ(80, more_memory.Available());
    scoped_ptr<Buffer> buffer2(more_memory.Allocate(30));
    ASSERT_TRUE(buffer2.get() != NULL);
    EXPECT_EQ(30, buffer2->size());
    EXPECT_EQ(50, more_memory.Available());
  }
  EXPECT_EQ(150, more_memory.Available());
}

TEST(BufferAllocatorTest,
     MemoryStatisticsCollectingBufferAllocatorCallsCollector) {
  MemoryStatisticsCollectorMock* stats_collector_mock(
      new MemoryStatisticsCollectorMock);
  MemoryLimit hard_limit(300);
  MemoryLimit soft_quota(100, false, &hard_limit);
  MemoryStatisticsCollectingBufferAllocator stats_allocator(
      &soft_quota, stats_collector_mock);
  {
    ::testing::InSequence dummy;
    EXPECT_CALL(*stats_collector_mock, AllocatedMemoryBytes(50));
    EXPECT_CALL(*stats_collector_mock, AllocatedMemoryBytes(100));
    EXPECT_CALL(*stats_collector_mock, AllocatedMemoryBytes(50));
    EXPECT_CALL(*stats_collector_mock, RefusedMemoryBytes(100));
    EXPECT_CALL(*stats_collector_mock, FreedMemoryBytes(80));
    EXPECT_CALL(*stats_collector_mock, FreedMemoryBytes(100));
    EXPECT_CALL(*stats_collector_mock, FreedMemoryBytes(20));
    EXPECT_CALL(*stats_collector_mock, Die());
  }
  scoped_ptr<Buffer> buffer1(stats_allocator.BestEffortAllocate(50, 50));
  ASSERT_TRUE(buffer1.get() != NULL);
  scoped_ptr<Buffer> buffer2(stats_allocator.BestEffortAllocate(150, 100));
  ASSERT_TRUE(buffer2.get() != NULL);
  EXPECT_TRUE(stats_allocator.BestEffortReallocate(200, 100, buffer1.get()));
  EXPECT_FALSE(stats_allocator.BestEffortReallocate(200, 200, buffer1.get()));
  EXPECT_TRUE(stats_allocator.BestEffortReallocate(20, 20, buffer1.get()));
}

class DeletionLogger {
 public:
  // Does not take ownership of log.
  DeletionLogger(int id, vector<int>* log)
      : id_(id), log_(log) {}
  // Destructor appends object's "id" to the end of the log vector.
  ~DeletionLogger() {
    log_->push_back(id_);
  }
 private:
  int id_;
  vector<int>* log_;
};

TEST(BufferAllocatorTest, OwningAllocatorDeletesOwnedObjectsInReverseOrder) {
  vector<int> destruction_log;
  {
    OwningBufferAllocator<DeletionLogger> owning_allocator(
        HeapBufferAllocator::Get());
    owning_allocator.Add(new DeletionLogger(1, &destruction_log));
    owning_allocator.Add(new DeletionLogger(2, &destruction_log));
    owning_allocator.Add(new DeletionLogger(3, &destruction_log));
    ASSERT_EQ(0, destruction_log.size());
  }
  ASSERT_EQ(3, destruction_log.size());
  EXPECT_EQ(3, destruction_log[0]);
  EXPECT_EQ(2, destruction_log[1]);
  EXPECT_EQ(1, destruction_log[2]);
}

}  // namespace supersonic
