// Copyright 2008 Google Inc.  All rights reserved.

#include "supersonic/utils/strings/substitute.h"

#include <algorithm>
#include "supersonic/utils/std_namespace.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/strings/ascii_ctype.h"
#include "supersonic/utils/strings/escaping.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/stl_util.h"

namespace strings {
namespace substitute_internal {

void SubstituteAndAppendArray(
    string* output, StringPiece format,
    const StringPiece* args_array, size_t num_args) {
  // Determine total size needed.
  int size = 0;
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (i+1 >= format.size()) {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return;
      } else if (ascii_isdigit(format[i+1])) {
        int index = format[i+1] - '0';
        if (index >= num_args) {
          LOG(DFATAL)
            << "strings::Substitute format string invalid: asked for \"$"
            << index << "\", but only " << num_args
            << " args were given.  Full format string was: \""
            << CEscape(format) << "\".";
          return;
        }
        size += args_array[index].size();
        ++i;  // Skip next char.
      } else if (format[i+1] == '$') {
        ++size;
        ++i;  // Skip next char.
      } else {
        LOG(DFATAL) << "Invalid strings::Substitute() format string: \""
                    << CEscape(format) << "\".";
        return;
      }
    } else {
      ++size;
    }
  }

  if (size == 0) return;

  // Build the string.
  int original_size = output->size();
  STLStringResizeUninitialized(output, original_size + size);
  char* target = string_as_array(output) + original_size;
  for (int i = 0; i < format.size(); i++) {
    if (format[i] == '$') {
      if (ascii_isdigit(format[i+1])) {
        const StringPiece src = args_array[format[i+1] - '0'];
        target = std::copy(src.begin(), src.end(), target);
        ++i;  // Skip next char.
      } else if (format[i+1] == '$') {
        *target++ = '$';
        ++i;  // Skip next char.
      }
    } else {
      *target++ = format[i];
    }
  }

  DCHECK_EQ(target - output->data(), output->size());
}

Arg::Arg(const void* value) {
  COMPILE_ASSERT(sizeof(scratch_) >= sizeof(value) * 2 + 2,
                 fix_sizeof_scratch_);
  if (value == NULL) {
    piece_ = "NULL";
  } else {
    char* ptr = scratch_ + sizeof(scratch_);
    uintptr_t num = reinterpret_cast<uintptr_t>(value);
    static const char kHexDigits[] = "0123456789abcdef";
    do {
      *--ptr = kHexDigits[num & 0xf];
      num >>= 4;
    } while (num != 0);
    *--ptr = 'x';
    *--ptr = '0';
    piece_.set(ptr, scratch_ + sizeof(scratch_) - ptr);
  }
}

}  // namespace substitute_internal
}  // namespace strings
