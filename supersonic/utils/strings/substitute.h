// Copyright 2008 Google Inc.  All rights reserved.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Substitutes variables into a format string.
//
// This file defines functions for substituting variables into strings using a
// format string with positional notation and a set of arguments.
//
//   (1) strings::Substitute()
//   (2) strings::SubstituteAndAppend()
//
// The format string uses positional identifiers indicated by a dollar sign ($)
// and a single digit rather than printf-style format specifiers. These
// positional ids indicate which of the following substitution arguments to use
// at that location within the format string.
//
// Arguments following the format string may be alphanumeric types, such as
// strings, StringPiece, ints, floats, bool, etc (see below for a list of
// supported types). These types will be converted to strings in the natural
// way.
//
// There is no way to specify *how* to format a value, beyond the default
// conversion to string. For example, you cannot format an integer in hex.
//
// Example 1:
//   string s = strings::Substitute("$1 purchased $0 $2. Thanks $1!",
//                                  5, "Bob", "Apples");
//   EXPECT_EQ("Bob purchased 5 Apples. Thanks Bob!", s);
//
// Example 2:
//   string s = "Hi. ";
//   strings::SubstituteAndAppend(&s, "My name is $0 and I am $1 years old.",
//                                "Bob", 5);
//   EXPECT_EQ("Hi. My name is Bob and I am 5 years old.", s);
//
// Differences from StringPrintf():
//   * The format string does not identify the types of arguments. Instead, the
//     arguments are implicitly converted to strings. See below for a list of
//     accepted types.
//   * Substitutions in the format string are identified by a '$' followed by a
//     single digit. You can use arguments out-of-order and use the same
//     argument multiple times.
//   * A '$$' sequence in the format string means output a literal '$'
//     character.
//   * strings::Substitute() is significantly faster than StringPrintf(). For
//     very large strings, it may be orders of magnitude faster.
//
// Supported types:
//   * StringPiece, string, const char* (NULL is equivalent to "")
//   * int32, int64, uint32, uint64
//   * float, double
//   * bool (Printed as "true" or "false")
//   * pointer types other than char* (Printed as "0x<lower case hex string>",
//     except that NULL is printed as "NULL")
//
// If not enough arguments are supplied, a LOG(DFATAL) will be issued and the
// empty string will be returned. If too many arguments are supplied, unused
// arguments will be ignored.

#include <string.h>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/basictypes.h"
#include "supersonic/utils/strings/numbers.h"
#include "supersonic/utils/strings/stringpiece.h"


#ifndef STRINGS_SUBSTITUTE_H_
#define STRINGS_SUBSTITUTE_H_

namespace strings {
namespace substitute_internal {

// ========================================
// NOTE:
//   Do not depend on these internal details.
//   They will change without notice.
// ========================================

// The argument type for strings::Substitute() and
// strings::SubstituteAndAppend(). It handles the implicit conversion of various
// types to a string. This is very similar to AlphaNum in strings/strcat.h.
// TODO(user): Investigate replacing this class with AlphaNum.
// This class has implicit constructors.
// Style guide exception granted:
// http://goto/style-guide-exception-20978288
class Arg {
 public:
  // Explicitly overload const char* so the compiler doesn't cast to bool.
  Arg(const char* value)  // NOLINT(runtime/explicit)
      : piece_(value) {}
  Arg(const string& value)  // NOLINT(runtime/explicit)
      : piece_(value) {}
  Arg(StringPiece value)  // NOLINT(runtime/explicit)
      : piece_(value) {}

  // Primitives
  //
  // No overloads for signed and unsigned char because if people are explicitly
  // declaring their chars as signed or unsigned then they are probably actually
  // using them as 8-bit integers and would probably prefer an integer
  // representation.  But, we don't really know.  So, we make the caller decide
  // what to do.
  Arg(char value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, 1) { scratch_[0] = value; }
  Arg(short value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastInt32ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(unsigned short value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastUInt32ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(int value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastInt32ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(unsigned int value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastUInt32ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(long value)  // NOLINT(runtime/explicit)
      : piece_(scratch_,
               (sizeof(value) == 4 ? FastInt32ToBufferLeft(value, scratch_)
                                   : FastInt64ToBufferLeft(value, scratch_)) -
                   scratch_) {}
  Arg(unsigned long value)  // NOLINT(runtime/explicit)
      : piece_(scratch_,
               (sizeof(value) == 4 ? FastUInt32ToBufferLeft(value, scratch_)
                                   : FastUInt64ToBufferLeft(value, scratch_)) -
                   scratch_) {}
  Arg(long long value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastInt64ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(unsigned long long value)  // NOLINT(runtime/explicit)
      : piece_(scratch_, FastUInt64ToBufferLeft(value, scratch_) - scratch_) {}
  Arg(float value)  // NOLINT(runtime/explicit)
      : piece_(FloatToBuffer(value, scratch_)) {}
  Arg(double value)  // NOLINT(runtime/explicit)
      : piece_(DoubleToBuffer(value, scratch_)) {}
  Arg(bool value)  // NOLINT(runtime/explicit)
      : piece_(value ? "true" : "false") {}
  // void* values, with the exception of char*, are printed as
  // StringPrintf with format "%p" would ("0x<hex value>"), with the
  // exception of NULL, which is printed as "NULL".
  Arg(const void* value);  // NOLINT(runtime/explicit)

  StringPiece piece() const { return piece_; }

 private:
  StringPiece piece_;
  char scratch_[kFastToBufferSize];

  DISALLOW_COPY_AND_ASSIGN(Arg);
};

// Internal helper function. Not okay to call this from outside.
// Interface will change without notice.
void SubstituteAndAppendArray(
    string* output, StringPiece format,
    const StringPiece* args_array, size_t num_args);

}  // namespace substitute_internal

//
// PUBLIC API
//

// Substitutes variables into a given format string and appends to "output".
// See file comments above for usage.
inline void SubstituteAndAppend(
    string* output, StringPiece format) {
  substitute_internal::SubstituteAndAppendArray(output, format, NULL, 0);
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0) {
  const StringPiece args[] = { a0.piece() };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1) {

  const StringPiece args[] = { a0.piece(), a1.piece() };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2) {
  const StringPiece args[] = { a0.piece(), a1.piece(), a2.piece() };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece(),
    a5.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece(),
    a5.piece(), a6.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece(),
    a5.piece(), a6.piece(), a7.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7,
    const substitute_internal::Arg& a8) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece(),
    a5.piece(), a6.piece(), a7.piece(), a8.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

inline void SubstituteAndAppend(
    string* output, StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7,
    const substitute_internal::Arg& a8, const substitute_internal::Arg& a9) {
  const StringPiece args[] = {
    a0.piece(), a1.piece(), a2.piece(), a3.piece(), a4.piece(),
    a5.piece(), a6.piece(), a7.piece(), a8.piece(), a9.piece()
  };
  substitute_internal::SubstituteAndAppendArray(output, format,
                                                args, arraysize(args));
}

// Substitutes variables into a given format string.
// See file comments above for usage.
inline string Substitute(StringPiece format) {
  string result;
  SubstituteAndAppend(&result, format);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0) {
  string result;
  SubstituteAndAppend(&result, format, a0);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4, a5);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4, a5, a6);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4, a5, a6, a7);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7,
    const substitute_internal::Arg& a8) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4, a5, a6, a7, a8);
  return result;
}

inline string Substitute(
    StringPiece format,
    const substitute_internal::Arg& a0, const substitute_internal::Arg& a1,
    const substitute_internal::Arg& a2, const substitute_internal::Arg& a3,
    const substitute_internal::Arg& a4, const substitute_internal::Arg& a5,
    const substitute_internal::Arg& a6, const substitute_internal::Arg& a7,
    const substitute_internal::Arg& a8, const substitute_internal::Arg& a9) {
  string result;
  SubstituteAndAppend(&result, format, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9);
  return result;
}

}  // namespace strings

#endif  // STRINGS_SUBSTITUTE_H_
