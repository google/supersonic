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

#include "supersonic/base/memory/memory_mocks.h"

namespace supersonic {

Buffer* MockBufferAllocator::Grant(const size_t bytes,
                                   BufferAllocator* const originator) {
  return CHECK_NOTNULL(DelegateAllocate(internal_allocator_,
                                        bytes,
                                        bytes,
                                        originator));
}

Buffer* MockBufferAllocator::AllocateInternal(
    const size_t requested,
    const size_t minimal,
    BufferAllocator* const originator) {
  if (predescribed_sizes_granted_.empty()) {
    switch (default_behaviour_) {
      case GRANT_MAX: return Grant(requested, originator);
      case GRANT_MIN: return Grant(minimal, originator);
      case DO_NOT_GRANT:
        if (minimal == 0) return Grant(0, originator);
        return NULL;
      case FAIL_ON_CHECK:
        if (minimal == 0) return Grant(0, originator);
        LOG(FATAL);
    }
    // Will never be called.
    LOG(FATAL);
    return nullptr;
  } else {
    // We have at least one item in the list.
    int granted = *predescribed_sizes_granted_.begin();
    predescribed_sizes_granted_.pop_front();
    if (granted < minimal) return nullptr;
    if (requested < granted) granted = requested;
    return Grant(granted, originator);
  }
}

bool MockBufferAllocator::ReGrant(const size_t bytes,
                                  Buffer* const buffer,
                                  BufferAllocator* const originator) {
  CHECK(DelegateReallocate(internal_allocator_,
                           bytes,
                           bytes,
                           buffer,
                           originator));
  return true;
}

bool MockBufferAllocator::ReallocateInternal(
    const size_t requested,
    const size_t minimal,
    Buffer* const buffer,
    BufferAllocator* const originator) {
  if (predescribed_sizes_granted_.empty()) {
    switch (default_behaviour_) {
      case GRANT_MAX: return ReGrant(requested, buffer, originator);
      case GRANT_MIN: return ReGrant(minimal, buffer, originator);
      case DO_NOT_GRANT:
        if (minimal == 0) return ReGrant(0, buffer, originator);
        return false;
      case FAIL_ON_CHECK:
        if (minimal == 0) return ReGrant(0, buffer, originator);
        LOG(FATAL);
    }
    // Will never be called.
    LOG(FATAL);
    return false;
  } else {
    // We have at least one item in the list.
    size_t granted = *predescribed_sizes_granted_.begin();
    predescribed_sizes_granted_.pop_front();
    if (granted < minimal) return false;
    if (requested < granted) granted = requested;
    return ReGrant(granted, buffer, originator);
  }
}

void MockBufferAllocator::FreeInternal(Buffer* const buffer) {
  DelegateFree(internal_allocator_, buffer);
}

}  // namespace supersonic
