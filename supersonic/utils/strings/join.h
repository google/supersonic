// Copyright 2008 and onwards Google, Inc.
//
// Functions for joining strings.  Functions have been migrated to this file
// from strutil.h.
//
#ifndef STRINGS_JOIN_H_
#define STRINGS_JOIN_H_

#include <stdio.h>
#include <string.h>
#include <ext/hash_map>
using __gnu_cxx::hash;
using __gnu_cxx::hash_map;  // Not used in this file.
#include <ext/hash_set>
using __gnu_cxx::hash;
using __gnu_cxx::hash_set;  // Not used in this file.
#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <map>
using std::map;
using std::multimap;
#include <set>
using std::multiset;
using std::set;
#include <string>
using std::string;
#include <utility>
using std::make_pair;
using std::pair;
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/utils/strings/numbers.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/hash/hash.h"

// The AlphaNum type is designed for internal use by Join, though
// I suppose that any routine accepting either a string or a number could
// accept it.  The basic idea is that by accepting a "const AlphaNum &" as an
// argument to your function, your callers will automagically convert bools,
// integers, and floating point values to strings for you.
//
// Conversion from 8-bit values is not accepted because if it were, then an
// attempt to pass ':' instead of ":" might result in a 58 ending up in your
// result.
//
// Bools convert to "0" or "1".
//
// Floating point values are converted to a string which, if
// passed to strtod(), would produce the exact same original double
// (except in case of NaN; all NaNs are considered the same value).
// We try to keep the string short but it's not guaranteed to be as
// short as possible.
//
// This class has implicit constructors.
// Style guide exception granted:
// http://goto/style-guide-exception-20978288

struct AlphaNum {
  StringPiece piece;
  char digits[kFastToBufferSize];

  // No bool ctor -- bools convert to an integral type.
  // A bool ctor would also convert incoming pointers (bletch).

  AlphaNum(int32 i32)  // NOLINT(runtime/explicit)
      : piece(digits, FastInt32ToBufferLeft(i32, digits) - &digits[0]) {}
  AlphaNum(uint32 u32)  // NOLINT(runtime/explicit)
      : piece(digits, FastUInt32ToBufferLeft(u32, digits) - &digits[0]) {}
  AlphaNum(int64 i64)  // NOLINT(runtime/explicit)
      : piece(digits, FastInt64ToBufferLeft(i64, digits) - &digits[0]) {}
  AlphaNum(uint64 u64)  // NOLINT(runtime/explicit)
      : piece(digits, FastUInt64ToBufferLeft(u64, digits) - &digits[0]) {}

#ifdef _LP64
  AlphaNum(long x)  // NOLINT(runtime/explicit)
    : piece(digits, FastInt64ToBufferLeft(x, digits) - &digits[0]) {}
  AlphaNum(unsigned long x)  // NOLINT(runtime/explicit)
    : piece(digits, FastUInt64ToBufferLeft(x, digits) - &digits[0]) {}
#else
  AlphaNum(long x)  // NOLINT(runtime/explicit)
    : piece(digits, FastInt32ToBufferLeft(x, digits) - &digits[0]) {}
  AlphaNum(unsigned long x)  // NOLINT(runtime/explicit)
    : piece(digits, FastUInt32ToBufferLeft(x, digits) - &digits[0]) {}
#endif

  AlphaNum(float f)  // NOLINT(runtime/explicit)
    : piece(digits, strlen(FloatToBuffer(f, digits))) {}
  AlphaNum(double f)  // NOLINT(runtime/explicit)
    : piece(digits, strlen(DoubleToBuffer(f, digits))) {}

  AlphaNum(const char *c_str) : piece(c_str) {}  // NOLINT(runtime/explicit)
  AlphaNum(const StringPiece &pc) : piece(pc) {}  // NOLINT(runtime/explicit)
  AlphaNum(const string &s) : piece(s) {}  // NOLINT(runtime/explicit)

  StringPiece::size_type size() const { return piece.size(); }
  const char *data() const { return piece.data(); }

 private:
  // Use ":" not ':'
  AlphaNum(char c);  // NOLINT(runtime/explicit)
};

extern AlphaNum gEmptyAlphaNum;

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or numbers, with no delimiter.  This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, StringPieces, strings, bool values,
//    and numeric values.
//
//    Don't use this for user-visible strings.  The localization process
//    works poorly on strings built up out of fragments.
//
//    For clarity and performance, don't use StrCat when appending to a
//    string.  In particular, avoid using any of these (anti-)patterns:
//      str.append(StrCat(...)
//      str += StrCat(...)
//      str = StrCat(str, ...)
//    where the last is the worse, with the potential to change a loop
//    from a linear time operation with O(1) dynamic allocations into a
//    quadratic time operation with O(n) dynamic allocations.  StrAppend
//    is a better choice than any of the above, subject to the restriction
//    of StrAppend(&str, a, b, c, ...) that none of the a, b, c, ... may
//    be a reference into str.
// ----------------------------------------------------------------------

string StrCat(const AlphaNum &a);
string StrCat(const AlphaNum &a, const AlphaNum &b);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
              const AlphaNum &g);
string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
              const AlphaNum &g, const AlphaNum &h);

namespace strings {
namespace internal {

// Do not call directly - this is not part of the public API.
string StrCatNineOrMore(const AlphaNum *a1, ...);

}  // namespace internal
}  // namespace strings

// Support 9 or more arguments
inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u,
                     const AlphaNum &v) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, &v, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u,
                     const AlphaNum &v, const AlphaNum &w) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, &v, &w, null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u,
                     const AlphaNum &v, const AlphaNum &w, const AlphaNum &x) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, &v, &w, &x,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u,
                     const AlphaNum &v, const AlphaNum &w, const AlphaNum &x,
                     const AlphaNum &y) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, &v, &w, &x, &y,
                                             null_alphanum);
}

inline string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
                     const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
                     const AlphaNum &g, const AlphaNum &h, const AlphaNum &i,
                     const AlphaNum &j, const AlphaNum &k, const AlphaNum &l,
                     const AlphaNum &m, const AlphaNum &n, const AlphaNum &o,
                     const AlphaNum &p, const AlphaNum &q, const AlphaNum &r,
                     const AlphaNum &s, const AlphaNum &t, const AlphaNum &u,
                     const AlphaNum &v, const AlphaNum &w, const AlphaNum &x,
                     const AlphaNum &y, const AlphaNum &z) {
  const AlphaNum* null_alphanum = NULL;
  return strings::internal::StrCatNineOrMore(&a, &b, &c, &d, &e, &f, &g, &h, &i,
                                             &j, &k, &l, &m, &n, &o, &p, &q, &r,
                                             &s, &t, &u, &v, &w, &x, &y, &z,
                                             null_alphanum);
}

// ----------------------------------------------------------------------
// StrAppend()
//    Same as above, but adds the output to the given string.
//    WARNING: For speed, StrAppend does not try to check each of its input
//    arguments to be sure that they are not a subset of the string being
//    appended to.  That is, while this will work:
//
//    string s = "foo";
//    s += s;
//
//    This will not (necessarily) work:
//
//    string s = "foo";
//    StrAppend(&s, s);
//
//    Note: while StrCat supports appending up to 12 arguments, StrAppend
//    is currently limited to 9.  That's rarely an issue except when
//    automatically transforming StrCat to StrAppend, and can easily be
//    worked around as consecutive calls to StrAppend are quite efficient.
// ----------------------------------------------------------------------

void StrAppend(string *dest,      const AlphaNum &a);
void StrAppend(string *dest,      const AlphaNum &a, const AlphaNum &b);
void StrAppend(string *dest,      const AlphaNum &a, const AlphaNum &b,
               const AlphaNum &c);
void StrAppend(string *dest,      const AlphaNum &a, const AlphaNum &b,
               const AlphaNum &c, const AlphaNum &d);

// Support up to 9 params by using a default empty AlphaNum.
void StrAppend(string *dest,      const AlphaNum &a, const AlphaNum &b,
               const AlphaNum &c, const AlphaNum &d, const AlphaNum &e,
               const AlphaNum &f = gEmptyAlphaNum,
               const AlphaNum &g = gEmptyAlphaNum,
               const AlphaNum &h = gEmptyAlphaNum,
               const AlphaNum &i = gEmptyAlphaNum);

// ----------------------------------------------------------------------
// JoinUsing()
//    This concatenates a vector of strings "components" into a new char[]
//    buffer, using the C-string "delim" as a separator between components.
//
//    This is essentially the same as JoinUsingToBuffer except
//    the return result is dynamically allocated using "new char[]".
//    It is the caller's responsibility to "delete []" the char* that is
//    returned.
//
//    If result_length_p is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsing(const vector<const char*>& components,
                const char* delim,
                int*  result_length_p);

// ----------------------------------------------------------------------
// JoinUsingToBuffer()
//    This concatenates a vector of strings "components" into a given char[]
//    buffer, using the C-string "delim" as a separator between components.
//    User supplies the result buffer with specified buffer size.
//    The result is also returned for convenience.
//
//    If result_length_p is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsingToBuffer(const vector<const char*>& components,
                        const char* delim,
                        int result_buffer_size,
                        char* result_buffer,
                        int*  result_length_p);

// ----------------------------------------------------------------------
// JoinStrings(), JoinStringsIterator(), JoinStringsInArray()
//
//    JoinStrings concatenates a container of strings into a C++ string,
//    using the string "delim" as a separator between components.
//    "components" can be any sequence container whose values are C++ strings
//    or StringPieces. More precisely, "components" must support STL container
//    iteration; i.e. it must have begin() and end() methods with appropriate
//    semantics, which return forward iterators whose value type is
//    string or StringPiece. Repeated string fields of protocol messages
//    satisfy these requirements.
//
//    JoinStringsIterator is the same as JoinStrings, except that the input
//    strings are specified with a pair of iterators. The requirements on
//    the iterators are the same as the requirements on components.begin()
//    and components.end() for JoinStrings.
//
//    JoinStringsInArray is the same as JoinStrings, but operates on
//    an array of C++ strings or string pointers.
//
//    There are two flavors of each function, one flavor returns the
//    concatenated string, another takes a pointer to the target string. In
//    the latter case the target string is cleared and overwritten.
// ----------------------------------------------------------------------
template <class CONTAINER>
void JoinStrings(const CONTAINER& components,
                 const StringPiece& delim,
                 string* result);
template <class CONTAINER>
string JoinStrings(const CONTAINER& components,
                   const StringPiece& delim);

template <class ITERATOR>
void JoinStringsIterator(const ITERATOR& start,
                         const ITERATOR& end,
                         const StringPiece& delim,
                         string* result);
template <class ITERATOR>
string JoinStringsIterator(const ITERATOR& start,
                           const ITERATOR& end,
                           const StringPiece& delim);

template<typename ITERATOR>
void JoinKeysAndValuesIterator(const ITERATOR& start,
                               const ITERATOR& end,
                               const StringPiece& intra_delim,
                               const StringPiece& inter_delim,
                               string *result) {
  result->clear();
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter == start) {
      StrAppend(result, iter->first, intra_delim, iter->second);
    } else {
      StrAppend(result, inter_delim, iter->first, intra_delim, iter->second);
    }
  }
}

template <typename ITERATOR>
string JoinKeysAndValuesIterator(const ITERATOR& start,
                                 const ITERATOR& end,
                                 const StringPiece& intra_delim,
                                 const StringPiece& inter_delim) {
  string result;
  JoinKeysAndValuesIterator(start, end, intra_delim, inter_delim, &result);
  return result;
}

void JoinStringsInArray(string const* const* components,
                        int num_components,
                        const char* delim,
                        string* result);
void JoinStringsInArray(string const* components,
                        int num_components,
                        const char* delim,
                        string* result);
string JoinStringsInArray(string const* const* components,
                          int num_components,
                          const char* delim);
string JoinStringsInArray(string const* components,
                          int num_components,
                          const char* delim);

// ----------------------------------------------------------------------
// Definitions of above JoinStrings* methods
// ----------------------------------------------------------------------
template <class CONTAINER>
inline void JoinStrings(const CONTAINER& components,
                        const StringPiece& delim,
                        string* result) {
  JoinStringsIterator(components.begin(), components.end(), delim, result);
}

template <class CONTAINER>
inline string JoinStrings(const CONTAINER& components,
                          const StringPiece& delim) {
  string result;
  JoinStrings(components, delim, &result);
  return result;
}

template <class ITERATOR>
void JoinStringsIterator(const ITERATOR& start,
                         const ITERATOR& end,
                         const StringPiece& delim,
                         string* result) {
  result->clear();

  // Precompute resulting length so we can reserve() memory in one shot.
  if (start != end) {
    int length = delim.size()*(distance(start, end)-1);
    for (ITERATOR iter = start; iter != end; ++iter) {
      length += iter->size();
    }
    result->reserve(length);
  }

  // Now combine everything.
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter != start) {
      result->append(delim.data(), delim.size());
    }
    result->append(iter->data(), iter->size());
  }
}

template <class ITERATOR>
inline string JoinStringsIterator(const ITERATOR& start,
                                  const ITERATOR& end,
                                  const StringPiece& delim) {
  string result;
  JoinStringsIterator(start, end, delim, &result);
  return result;
}

inline string JoinStringsInArray(string const* const* components,
                                 int num_components,
                                 const char* delim) {
  string result;
  JoinStringsInArray(components, num_components, delim, &result);
  return result;
}

inline string JoinStringsInArray(string const* components,
                                 int num_components,
                                 const char* delim) {
  string result;
  JoinStringsInArray(components, num_components, delim, &result);
  return result;
}

// ----------------------------------------------------------------------
// JoinMapKeysAndValues()
// JoinHashMapKeysAndValues()
// JoinVectorKeysAndValues()
//    This merges the keys and values of a string -> string map or pair
//    of strings vector, with one delim (intra_delim) between each key
//    and its associated value and another delim (inter_delim) between
//    each key/value pair.  The result is returned in a string (passed
//    as the last argument).
// ----------------------------------------------------------------------

void JoinMapKeysAndValues(const map<string, string>& components,
                          const StringPiece& intra_delim,
                          const StringPiece& inter_delim,
                          string* result);
void JoinVectorKeysAndValues(const vector< pair<string, string> >& components,
                             const StringPiece& intra_delim,
                             const StringPiece& inter_delim,
                             string* result);

// DEPRECATED(jyrki): use JoinKeysAndValuesIterator directly.
template<typename T>
void JoinHashMapKeysAndValues(const T& container,
                              const StringPiece& intra_delim,
                              const StringPiece& inter_delim,
                              string* result) {
  JoinKeysAndValuesIterator(container.begin(), container.end(),
                            intra_delim, inter_delim,
                            result);
}

// ----------------------------------------------------------------------
// JoinCSVLineWithDelimiter()
//    This function is the inverse of SplitCSVLineWithDelimiter() in that the
//    string returned by JoinCSVLineWithDelimiter() can be passed to
//    SplitCSVLineWithDelimiter() to get the original string vector back.
//    Quotes and escapes the elements of original_cols according to CSV quoting
//    rules, and the joins the escaped quoted strings with commas using
//    JoinStrings().  Note that JoinCSVLineWithDelimiter() will not necessarily
//    return the same string originally passed in to
//    SplitCSVLineWithDelimiter(), since SplitCSVLineWithDelimiter() can handle
//    gratuitous spacing and quoting. 'output' must point to an empty string.
//
//    Example:
//     [Google], [x], [Buchheit, Paul], [string with " quoite in it], [ space ]
//     --->  [Google,x,"Buchheit, Paul","string with "" quote in it"," space "]
//
// JoinCSVLine()
//    A convenience wrapper around JoinCSVLineWithDelimiter which uses
//    ',' as the delimiter.
// ----------------------------------------------------------------------
void JoinCSVLine(const vector<string>& original_cols, string* output);
string JoinCSVLine(const vector<string>& original_cols);
void JoinCSVLineWithDelimiter(const vector<string>& original_cols,
                              char delimiter,
                              string* output);

// ----------------------------------------------------------------------
// JoinElements()
//    This merges a container of any type supported by StrAppend() with delim
//    inserted as separators between components.  This is essentially a
//    templatized version of JoinUsingToBuffer().
//
// JoinElementsIterator()
//    Same as JoinElements(), except that the input elements are specified
//    with a pair of forward iterators.
// ----------------------------------------------------------------------

template <class ITERATOR>
void JoinElementsIterator(ITERATOR first,
                          ITERATOR last,
                          StringPiece delim,
                          string* result) {
  result->clear();
  for (ITERATOR it = first; it != last; ++it) {
    if (it != first) {
      StrAppend(result, delim);
    }
    StrAppend(result, *it);
  }
}

template <class ITERATOR>
string JoinElementsIterator(ITERATOR first,
                            ITERATOR last,
                            StringPiece delim) {
  string result;
  JoinElementsIterator(first, last, delim, &result);
  return result;
}

template <class CONTAINER>
inline void JoinElements(const CONTAINER& components,
                         StringPiece delim,
                         string* result) {
  JoinElementsIterator(components.begin(), components.end(), delim, result);
}

template <class CONTAINER>
inline string JoinElements(const CONTAINER& components, StringPiece delim) {
  string result;
  JoinElements(components, delim, &result);
  return result;
}

template <class CONTAINER>
void JoinInts(const CONTAINER& components,
              const char* delim,
              string* result) {
  JoinElements(components, delim, result);
}

template <class CONTAINER>
inline string JoinInts(const CONTAINER& components,
                       const char* delim) {
  return JoinElements(components, delim);
}

#endif  // STRINGS_JOIN_H_
