// Copyright 2012 Google Inc.  All Rights Reserved
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
// The file contains the implementation of an ownership revoker which
// passes all function calls to its delegate, but does not take ownership
// of the underlying object.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_OWNERSHIP_REVOKER_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_OWNERSHIP_REVOKER_H_

#include "supersonic/utils/macros.h"

namespace supersonic {

// Generic ownership revoking class. The main use case is to avoid double
// ownership taking of the original element. To do so, we use the revoker
// as a wrapper over the original and pass it on. The revoker does not take
// ownership of the original, hence the latter will not be freed, when
// the revoker is destroyed.
template<typename T>
class OwnershipRevoker {
 public:
  // Does not take ownership of the original.
  explicit OwnershipRevoker(T* original)
      : original_(original) {}

  // Simple original cursor getter. No ownership transfer takes place, as the
  // class does not have ownership of the original.
  T* original() { return original_; }

 private:
  T* original_;

  DISALLOW_COPY_AND_ASSIGN(OwnershipRevoker);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_OWNERSHIP_REVOKER_H_
