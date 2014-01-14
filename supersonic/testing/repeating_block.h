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

#ifndef SUPERSONIC_TESTING_REPEATING_BLOCK_H_
#define SUPERSONIC_TESTING_REPEATING_BLOCK_H_

#include <stddef.h>

#include <memory>

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"

namespace supersonic {

class Cursor;

// Create a block of given size filled cyclically with rows from the source.
Block* ReplicateBlock(const Block& source, rowcount_t row_count,
                      BufferAllocator* allocator);

// Takes ownership of the original block.
class RepeatingBlockOperation : public BasicOperation {
 public:
  RepeatingBlockOperation(Block* block, rowcount_t total_num_rows);

  FailureOrOwned<Cursor> CreateCursor() const;

 private:
  // Creates new block consisting of repeated rows from the original block.
  // Returns NULL if block can't be created.
  Block* CreateResizedBlock(Block* original_block, rowcount_t min_num_rows);

  std::unique_ptr<Block> resized_block_;
  rowcount_t total_num_rows_;
  DISALLOW_COPY_AND_ASSIGN(RepeatingBlockOperation);
};

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_REPEATING_BLOCK_H_
