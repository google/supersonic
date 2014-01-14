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

#ifndef SUPERSONIC_CURSOR_CORE_HASH_JOIN_H_
#define SUPERSONIC_CURSOR_CORE_HASH_JOIN_H_

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BoundSingleSourceProjector;
class MultiSourceProjector;
class SingleSourceProjector;
class Cursor;
class LookupIndexBuilder;
class Operation;

class HashJoinOperation : public BasicOperation {
 public:
  // Currently join_type can be INNER or LEFT_OUTER. In the latter case, output
  // columns corresponding to the rhs input will be nullable.
  // lhs_ rhs_key_selector indicate columns in respectively left- and right-
  // hand side input to be used as key. Their number and types must match.
  // result_projector describes the projection built into hash join. It is
  // logically defined on the combined schema of both inputs and indicates
  // columns to be included in the output.
  // rhs_key_uniqueness indicates if in the rhs input all keys are unique,
  // which enables an optimized implementation of hash join.
  // Takes ownership of all projectors.
  HashJoinOperation(
      JoinType join_type,
      const SingleSourceProjector* lhs_key_selector,
      const SingleSourceProjector* rhs_key_selector,
      const MultiSourceProjector* result_projector,
      KeyUniqueness rhs_key_uniqueness,
      Operation* lhs_child, Operation* rhs_child);

  virtual FailureOrOwned<Cursor> CreateCursor() const;

 private:
  template <KeyUniqueness rhs_key_uniqueness>
  FailureOrOwned<LookupIndexBuilder> CreateHashIndexMaterializer(
      JoinType join_type,
      const BoundSingleSourceProjector* bound_rhs_key_selector,
      Cursor* rhs_cursor) const;

  const JoinType join_type_;
  std::unique_ptr<const SingleSourceProjector> lhs_key_selector_;
  std::unique_ptr<const SingleSourceProjector> rhs_key_selector_;
  std::unique_ptr<const MultiSourceProjector> result_projector_;
  const KeyUniqueness rhs_key_uniqueness_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_HASH_JOIN_H_
