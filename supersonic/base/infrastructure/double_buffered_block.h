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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// A class supplying two alternating memory blocks for an expression.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_DOUBLE_BUFFERED_BLOCK_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_DOUBLE_BUFFERED_BLOCK_H_

#include "supersonic/utils/macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

// Intended to be used within a single expression (i.e., each expression has
// its own memory supply).
class BufferAllocator;
class TupleSchema;

class DoubleBufferedBlock {
 public:
  DoubleBufferedBlock(BufferAllocator* const allocator,
                      const TupleSchema& schema)
      : give_first_block_(true),
        block_1_(schema, allocator),
        block_2_(schema, allocator) {}

  // Returns the current working block.
  Block* get_block();
  // Switches to a new working block and returns it. Invalidates the
  // second-to-last working block.
  Block* switch_block();
  // Reallocate both blocks to a different size.
  bool Reallocate(rowcount_t row_capacity);

 private:
  bool give_first_block_;
  Block block_1_;
  Block block_2_;
  DISALLOW_COPY_AND_ASSIGN(DoubleBufferedBlock);
};
}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_DOUBLE_BUFFERED_BLOCK_H_
