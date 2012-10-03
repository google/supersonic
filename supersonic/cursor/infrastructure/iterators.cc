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

#include "supersonic/cursor/infrastructure/iterators.h"

// TODO(user): remove after a successful launch.
DEFINE_bool(supersonic_release_cursors_aggressively, true,
            "When true, cursor iterators delete underlying cursors as soon as "
            "they reach EOS or hit a failure");

namespace supersonic {
namespace internal {

CursorProxy::CursorProxy(Cursor* cursor)
    : cursor_(cursor),
      is_waiting_on_barrier_supported_(
          cursor_->IsWaitingOnBarrierSupported()),
      cursor_status_(ResultView::BOS()) {}

void CursorProxy::Terminate() {
  if (cursor_.get() == NULL) return;
  if (!cursor_status_.is_done()) {
    cursor_status_ = ResultView::Failure(
        new Exception(ERROR_UNKNOWN_ERROR, "Terminated prematurely"));
  }
  cursor_->AppendDebugDescription(&terminal_debug_description_);
  cursor_.reset();
}

void CursorProxy::AppendDebugDescription(string* target) const {
  if (cursor_.get() != NULL) {
    cursor_->AppendDebugDescription(target);
  } else {
    target->append(terminal_debug_description_);
  }
}

}  // namespace internal
}  // namespace supersonic
