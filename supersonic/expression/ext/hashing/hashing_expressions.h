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

#ifndef SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_EXPRESSIONS_H_

namespace supersonic {

// Returns a UINT64 fingerprint of a string, date, datetime, boolean, binary or
// integer, as in util/hash/hash.h.
// The weird naming is to avoid a name collision with the Fingerprint function
// defined in util/hash/hash.h.
class Expression;

const Expression* SupersonicFingerprint(const Expression* e);

// Returns a UINT64 hash of any DataType, taking a UINT64 seed, as in
// util/hash/hash.h. Note there is no implicit cast to UINT64 for the seed.
// This weird naming scheme is to avoid a collision with our internal hash.
// TODO(onufry): refactor to use the name "Hash" here, and change the internal
// name.
const Expression* SupersonicHash(const Expression* e,
                                 const Expression* seed);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_EXT_HASHING_HASHING_EXPRESSIONS_H_
