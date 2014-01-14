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
// Author: Tomasz Kaftal (tomasz.kaftal@gmail.com)
//
// The header contains one inline function, SupersonicInit(),
// which initialises atomicops for x86 architecture and the Google logging
// module.
#ifndef SUPERSONIC_OPENSOURCE_AUXILIARY_INIT_H_
#define SUPERSONIC_OPENSOURCE_AUXILIARY_INIT_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"

namespace supersonic {

// The function will initialise Supersonic and  the utilities it makes use of.
// It will strip the specified command line flags from argv and modify argc.
inline void SupersonicInit(int* argc, char*** argv) {
  google::ParseCommandLineFlags(argc, argv, true);
  google::InitGoogleLogging(*argv[0]);
}
}  // namespace supersonic
#endif  // SUPERSONIC_OPENSOURCE_AUXILIARY_INIT_H_
