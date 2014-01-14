// Copyright 2012 Google Inc. All Rights Reserved.
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
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// The file contains a simple matching utility for protocol buffers.

#ifndef SUPERSONIC_OPENSOURCE_AUXILIARY_PROTO_MATCHER_H_
#define SUPERSONIC_OPENSOURCE_AUXILIARY_PROTO_MATCHER_H_

#include "gmock/gmock.h"

namespace testing {

MATCHER_P(EqualsProto, message, "") {
  // TODO(tkaftal): This is a very simple matcher which works as serialization
  // is deterministic in practice. Still, this is not something that should be
  // relied on.
  std::string expected_serialized;
  std::string actual_serialized;

  message.SerializeToString(&expected_serialized);
  arg.SerializeToString(&actual_serialized);
  return expected_serialized == actual_serialized;
}

}  // namespace testing

#endif  // SUPERSONIC_OPENSOURCE_AUXILIARY_PROTO_MATCHER_H_
