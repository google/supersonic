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
// Author: ptab@google.com (Piotr Tabor)

#include "supersonic/expression/ext/hashing/hashing_bound_expressions.h"

#include <stddef.h>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/ext/hashing/hashing_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

namespace {

UnaryExpressionFactory* FingerprintFactory(DataType type) {
  const OperatorId op = OPERATOR_FINGERPRINT;
  switch (type) {
    case INT32: return new SpecializedUnaryFactory<op, INT32, UINT64>();
    case UINT32: return new SpecializedUnaryFactory<op, UINT32, UINT64>();
    case INT64: return new SpecializedUnaryFactory<op, INT64, UINT64>();
    case UINT64: return new SpecializedUnaryFactory<op, UINT64, UINT64>();
    case DATE: return new SpecializedUnaryFactory<op, DATE, UINT64>();
    case DATETIME: return new SpecializedUnaryFactory<op, DATETIME, UINT64>();
    case STRING: return new SpecializedUnaryFactory<op, STRING, UINT64>();
    case BOOL: return new SpecializedUnaryFactory<op, BOOL, UINT64>();
    case BINARY: return new SpecializedUnaryFactory<op, BINARY, UINT64>();
    case FLOAT: return NULL;
    case DOUBLE: return NULL;
    case ENUM: return new SpecializedUnaryFactory<op, ENUM, UINT64>();
    case DATA_TYPE: return NULL;
  }
  LOG(FATAL);  // This will never be exectuted.
}

BinaryExpressionFactory* HashFactory(DataType type) {
  const OperatorId op = OPERATOR_HASH;
  switch (type) {
    case INT32:
      return new SpecializedBinaryFactory<op, INT32, UINT64, UINT64>();
    case UINT32:
      return new SpecializedBinaryFactory<op, UINT32, UINT64, UINT64>();
    case INT64:
      return new SpecializedBinaryFactory<op, INT64, UINT64, UINT64>();
    case UINT64:
      return new SpecializedBinaryFactory<op, UINT64, UINT64, UINT64>();
    case FLOAT:
      return new SpecializedBinaryFactory<op, FLOAT, UINT64, UINT64>();
    case DOUBLE:
      return new SpecializedBinaryFactory<op, DOUBLE, UINT64, UINT64>();
    case DATE:
      return new SpecializedBinaryFactory<op, DATE, UINT64, UINT64>();
    case DATETIME:
      return new SpecializedBinaryFactory<op, DATETIME, UINT64, UINT64>();
    case BOOL:
      return new SpecializedBinaryFactory<op, BOOL, UINT64, UINT64>();
    case ENUM:
      return new SpecializedBinaryFactory<op, ENUM, UINT64, UINT64>();
    case STRING:
      return new SpecializedBinaryFactory<op, STRING, UINT64, UINT64>();
    case BINARY:
      return new SpecializedBinaryFactory<op, BINARY, UINT64, UINT64>();
    case DATA_TYPE: return NULL;
  }
  LOG(FATAL);  // This will never be exectuted.
}

}  // namespace

FailureOrOwned<BoundExpression> BoundFingerprint(BoundExpression* child,
                                                 BufferAllocator* allocator,
                                                 rowcount_t row_capacity) {
  UnaryExpressionFactory* factory =
      FingerprintFactory(GetExpressionType(child));
  return RunUnaryFactory(factory, allocator, row_capacity, child,
                         "FINGERPRINT");
}

FailureOrOwned<BoundExpression> BoundHash(BoundExpression* child,
                                          BoundExpression* seed,
                                          BufferAllocator* allocator,
                                          rowcount_t row_capacity) {
  BinaryExpressionFactory* factory = HashFactory(GetExpressionType(child));
  return RunBinaryFactory(factory, allocator, row_capacity, child, seed,
                          "HASH");
}

}  // namespace supersonic
