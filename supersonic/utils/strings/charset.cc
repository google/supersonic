// Copyright 2008 Google Inc. All Rights Reserved.

#include "supersonic/utils/strings/charset.h"

#include <string.h>
#include "supersonic/utils/strings/stringpiece.h"

namespace strings {

CharSet::CharSet() {
  memset(bits_, 0, sizeof(bits_));
}

CharSet::CharSet(const char* characters) {
  memset(bits_, 0, sizeof(bits_));
  for (; *characters != '\0'; ++characters) {
    Add(*characters);
  }
}

CharSet::CharSet(StringPiece characters) {
  memset(bits_, 0, sizeof(bits_));
  for (size_t i = 0; i < characters.length(); ++i) {
    Add(characters[i]);
  }
}

}  // namespace strings
