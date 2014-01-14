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

#include "supersonic/cursor/infrastructure/writer.h"

#include <limits>
#include "supersonic/utils/std_namespace.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

class Cursor;

FailureOr<rowcount_t> Writer::Write(Sink* sink,
                                    const rowcount_t max_row_count) {
  rowcount_t total_written = 0;
  rowcount_t remaining = max_row_count;
  while (remaining > 0) {
    if (!iterator_.Next(remaining, false)) break;
    FailureOr<rowcount_t> write_result = sink->Write(iterator_.view());
    if (write_result.is_failure()) {
      // To be tidy, we're rolling back the data that failed to write.
      // (Perhaps one day we'll want to recover from such failures and retry).
      iterator_.truncate(0);
      PROPAGATE_ON_FAILURE(write_result);
    }
    rowcount_t written = write_result.get();
    total_written += written;
    if (written < iterator_.view().row_count()) {
      // Failed to write all rows; return with partial success.
      CHECK(iterator_.truncate(written));
      break;
    }
    DCHECK_LE(written, remaining);
    remaining -= written;
  }
  if (iterator_.is_failure()) {
    // We can't release here exception because iterator need to keep it,
    // to answer subsequent calls to Write(...).
    THROW(iterator_.exception().Clone());
  } else {
    return Success(total_written);
  }
}

FailureOr<rowcount_t> Writer::WriteAll(Sink* sink) {
  return Write(sink, std::numeric_limits<rowcount_t>::max());
}

FailureOrVoid WriteCursor(Cursor* cursor, Sink* sink) {
  Writer writer(cursor);
  FailureOr<rowcount_t> result = writer.WriteAll(sink);
  PROPAGATE_ON_FAILURE(result);
  if (writer.is_waiting_on_barrier()) {
    THROW(new Exception(ERROR_UNKNOWN_ERROR,
                        "Writing stumbled on a barrier."));
  }
  if (!writer.is_eos()) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        StrCat("Writing stopped after ", result.get(),
                               " rows.")));
  }
  return Success();
}

}  // namespace supersonic
