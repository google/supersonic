// Copyright 2010 Google Inc. All Rights Reserved.

#include "supersonic/utils/macros.h"
#include "supersonic/utils/mutex.h"

namespace concurrent {
namespace detail {

Mutex seq_cst_mutex(base::LINKER_INITIALIZED);

}  // namespace detail
}  // namespace concurrent
