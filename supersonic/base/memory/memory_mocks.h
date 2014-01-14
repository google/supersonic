// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// Contains various mocks for usage in testing of memory allocation bugs.

#ifndef SUPERSONIC_BASE_MEMORY_MEMORY_MOCKS_H_
#define SUPERSONIC_BASE_MEMORY_MEMORY_MOCKS_H_

#include <list>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/base/memory/memory.h"
#include "gmock/gmock.h"

namespace supersonic {
// This class is a buffer allocator which will grant memory according to
// a predefined user description.
class MockBufferAllocator : public BufferAllocator {
 public:
  enum Behaviour { GRANT_MAX, GRANT_MIN, DO_NOT_GRANT, FAIL_ON_CHECK };

  explicit MockBufferAllocator(Behaviour default_behaviour)
      : default_behaviour_(default_behaviour),
        internal_allocator_(HeapBufferAllocator::Get()) {}

  MockBufferAllocator(BufferAllocator* allocator,
                      Behaviour default_behaviour)
      : default_behaviour_(default_behaviour),
        internal_allocator_(allocator) {}

  virtual ~MockBufferAllocator() {}


  void set_default_behaviour(Behaviour behaviour) {
    default_behaviour_ = behaviour;
  }

  void add_predescribed_sizes(const list<size_t>& added) {
    predescribed_sizes_granted_.insert(
        predescribed_sizes_granted_.end(), added.begin(), added.end());
  }

  void clear_predescriped_sizes() {
    predescribed_sizes_granted_.clear();
  }

  bool predescribed_sizes_empty() {
    return predescribed_sizes_granted_.empty();
  }

 private:
  virtual Buffer* AllocateInternal(size_t requested,
                                   size_t minimal,
                                   BufferAllocator* originator);

  virtual bool ReallocateInternal(size_t requested,
                                  size_t minimal,
                                  Buffer* buffer,
                                  BufferAllocator* originator);

  virtual void FreeInternal(Buffer* buffer);

  // Helper function. Grants bytes memory into the returned buffer (via the
  // internal_allocator_), CHECKs for success.
  Buffer* Grant(size_t bytes, BufferAllocator* originator);

  // Helper function. Reallocates bytes memory into the returned buffer (via the
  // internal_allocator_), CHECKs for success.
  bool ReGrant(size_t bytes, Buffer* buffer, BufferAllocator* allocator);

  // Describes the behaviour of the allocator for the next calls. The first call
  // will receive memory equal to the first item on the list or the maximal
  // requested value, whichever is lower, unless the value from the list is
  // lower than the minimal value, in which case allocation fails.
  //
  // If you want to ensure that the request succeeds, put a
  // static_cast<size_t>(-1) in.
  //
  // Each item on the list is used for exactly one call to the the allocator,
  // and then removed from the list. If the list is empty, the default behaviour
  // is used.
  list<size_t> predescribed_sizes_granted_;
  // Defines the behaviour of the allocator if the predescribed_sizes_granted_
  // list is empty. Note that calls with a zero minimal value always succeed, as
  // per the contract of the BufferAllocator;
  Behaviour default_behaviour_;
  // The allocator to which all calls get delegated.
  BufferAllocator* internal_allocator_;

  DISALLOW_COPY_AND_ASSIGN(MockBufferAllocator);
};

class MemoryStatisticsCollectorMock
    : public MemoryStatisticsCollectorInterface {
 public:
  MOCK_METHOD1(AllocatedMemoryBytes, void(size_t bytes));
  MOCK_METHOD1(RefusedMemoryBytes, void(size_t bytes));
  MOCK_METHOD1(FreedMemoryBytes, void(size_t bytes));
  MOCK_METHOD0(Die, void());
  virtual ~MemoryStatisticsCollectorMock() { Die(); }
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_MEMORY_MEMORY_MOCKS_H_
