// Copyright 2008 and onwards Google Inc.  All rights reserved.

#include "supersonic/utils/strings/join.h"

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/strings/ascii_ctype.h"
#include "supersonic/utils/strings/escaping.h"
#include "supersonic/utils/stl_util.h"

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or integers, with no delimiter.  This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, StringPieces, strings, and integer values.
// ----------------------------------------------------------------------

// Append is merely a version of memcpy that returns the address of the byte
// after the area just overwritten.  It comes in multiple flavors to minimize
// call overhead.
static char *Append1(char *out, const AlphaNum &x) {
  memcpy(out, x.data(), x.size());
  return out + x.size();
}

static char *Append2(char *out, const AlphaNum &x1, const AlphaNum &x2) {
  memcpy(out, x1.data(), x1.size());
  out += x1.size();

  memcpy(out, x2.data(), x2.size());
  return out + x2.size();
}

static char *Append4(char *out,
                     const AlphaNum &x1, const AlphaNum &x2,
                     const AlphaNum &x3, const AlphaNum &x4) {
  memcpy(out, x1.data(), x1.size());
  out += x1.size();

  memcpy(out, x2.data(), x2.size());
  out += x2.size();

  memcpy(out, x3.data(), x3.size());
  out += x3.size();

  memcpy(out, x4.data(), x4.size());
  return out + x4.size();
}

string StrCat(const AlphaNum &a) {
  return string(a.data(), a.size());
}

string StrCat(const AlphaNum &a, const AlphaNum &b) {
  string result;
  STLStringResizeUninitialized(&result, a.size() + b.size());
  char *const begin = &*result.begin();
  char *out = Append2(begin, a, b);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c) {
  string result;
  STLStringResizeUninitialized(&result, a.size() + b.size() + c.size());
  char *const begin = &*result.begin();
  char *out = Append2(begin, a, b);
  out = Append1(out, c);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d) {
  string result;
  STLStringResizeUninitialized(&result,
                               a.size() + b.size() + c.size() + d.size());
  char *const begin = &*result.begin();
  char *out = Append4(begin, a, b, c, d);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e) {
  string result;
  STLStringResizeUninitialized(&result,
      a.size() + b.size() + c.size() + d.size() + e.size());
  char *const begin = &*result.begin();
  char *out = Append4(begin, a, b, c, d);
  out = Append1(out, e);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f) {
  string result;
  STLStringResizeUninitialized(&result,
      a.size() + b.size() + c.size() + d.size() + e.size() + f.size());
  char *const begin = &*result.begin();
  char *out = Append4(begin, a, b, c, d);
  out = Append2(out, e, f);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
              const AlphaNum &g) {
  string result;
  STLStringResizeUninitialized(&result,
      a.size() + b.size() + c.size() + d.size() + e.size()
               + f.size() + g.size());
  char *const begin = &*result.begin();
  char *out = Append4(begin, a, b, c, d);
  out = Append2(out, e, f);
  out = Append1(out, g);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

string StrCat(const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
              const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
              const AlphaNum &g, const AlphaNum &h) {
  string result;
  STLStringResizeUninitialized(&result,
      a.size() + b.size() + c.size() + d.size() + e.size()
               + f.size() + g.size() + h.size());
  char *const begin = &*result.begin();
  char *out = Append4(begin, a, b, c, d);
  out = Append4(out, e, f, g, h);
  DCHECK_EQ(out, begin + result.size());
  return result;
}

// StrCat with this many params is exceedingly rare, but it has been
// requested...  therefore we'll rely on default arguments to make calling
// slightly less efficient, to preserve code size.

namespace strings {
namespace internal {
string StrCatNineOrMore(const AlphaNum *a, ...) {
  string result;

  va_list args;
  va_start(args, a);
  size_t size = a->size();
  while (const AlphaNum *arg = va_arg(args, const AlphaNum *)) {
    size += arg->size();
  }
  STLStringResizeUninitialized(&result, size);
  va_end(args);
  va_start(args, a);
  char *const begin = &*result.begin();
  char *out = Append1(begin, *a);
  while (const AlphaNum *arg = va_arg(args, const AlphaNum *)) {
    out = Append1(out, *arg);
  }
  va_end(args);
  DCHECK_EQ(out, begin + size);
  return result;
}
}  // namespace internal
}  // namespace strings

AlphaNum gEmptyAlphaNum("");

// It's possible to call StrAppend with a StringPiece that is itself a fragment
// of the string we're appending to.  However the results of this are random.
// Therefore, check for this in debug mode.  Use unsigned math so we only have
// to do one comparison.
#define DCHECK_NO_OVERLAP(dest, src) \
    DCHECK_GT(uintptr_t((src).data() - (dest).data()), uintptr_t((dest).size()))


void StrAppend(string *result, const AlphaNum &a) {
  DCHECK_NO_OVERLAP(*result, a);
  result->append(a.data(), a.size());
}

void StrAppend(string *result, const AlphaNum &a, const AlphaNum &b) {
  DCHECK_NO_OVERLAP(*result, a);
  DCHECK_NO_OVERLAP(*result, b);
  string::size_type old_size = result->size();
  STLStringResizeUninitialized(result, old_size + a.size() + b.size());
  char *const begin = &*result->begin();
  char *out = Append2(begin + old_size, a, b);
  DCHECK_EQ(out, begin + result->size());
}

void StrAppend(string *result,
               const AlphaNum &a, const AlphaNum &b, const AlphaNum &c) {
  DCHECK_NO_OVERLAP(*result, a);
  DCHECK_NO_OVERLAP(*result, b);
  DCHECK_NO_OVERLAP(*result, c);
  string::size_type old_size = result->size();
  STLStringResizeUninitialized(result,
                               old_size + a.size() + b.size() + c.size());
  char *const begin = &*result->begin();
  char *out = Append2(begin + old_size, a, b);
  out = Append1(out, c);
  DCHECK_EQ(out, begin + result->size());
}

void StrAppend(string *result,
               const AlphaNum &a, const AlphaNum &b,
               const AlphaNum &c, const AlphaNum &d) {
  DCHECK_NO_OVERLAP(*result, a);
  DCHECK_NO_OVERLAP(*result, b);
  DCHECK_NO_OVERLAP(*result, c);
  DCHECK_NO_OVERLAP(*result, d);
  string::size_type old_size = result->size();
  STLStringResizeUninitialized(result,
      old_size + a.size() + b.size() + c.size() + d.size());
  char *const begin = &*result->begin();
  char *out = Append4(begin + old_size, a, b, c, d);
  DCHECK_EQ(out, begin + result->size());
}

// StrAppend with this many params is even rarer than with StrCat.
// Therefore we'll again rely on default arguments to make calling
// slightly less efficient, to preserve code size.
void StrAppend(string *result,
               const AlphaNum &a, const AlphaNum &b, const AlphaNum &c,
               const AlphaNum &d, const AlphaNum &e, const AlphaNum &f,
               const AlphaNum &g, const AlphaNum &h, const AlphaNum &i) {
  DCHECK_NO_OVERLAP(*result, a);
  DCHECK_NO_OVERLAP(*result, b);
  DCHECK_NO_OVERLAP(*result, c);
  DCHECK_NO_OVERLAP(*result, d);
  DCHECK_NO_OVERLAP(*result, e);
  DCHECK_NO_OVERLAP(*result, f);
  DCHECK_NO_OVERLAP(*result, g);
  DCHECK_NO_OVERLAP(*result, h);
  DCHECK_NO_OVERLAP(*result, i);
  string::size_type old_size = result->size();
  STLStringResizeUninitialized(result,
      old_size + a.size() + b.size() + c.size() + d.size()
               + e.size() + f.size() + g.size() + h.size() + i.size());
  char *const begin = &*result->begin();
  char *out = Append4(begin + old_size, a, b, c, d);
  out = Append4(out, e, f, g, h);
  out = Append1(out, i);
  DCHECK_EQ(out, begin + result->size());
}



// ----------------------------------------------------------------------
// JoinUsing()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    the return result is dynamically allocated using "new char[]".
//    It is the caller's responsibility to "delete []" the
//
//    If result_length_p is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsing(const vector<const char*>& components,
                const char* delim,
                int*  result_length_p) {
  const int num_components = components.size();
  const int delim_length = strlen(delim);
  int num_chars = (num_components > 1)
                ? delim_length * (num_components - 1)
                : 0;
  for (int i = 0; i < num_components; ++i)
    num_chars += strlen(components[i]);

  char* res_buffer = new char[num_chars+1];
  return JoinUsingToBuffer(components, delim, num_chars+1,
                           res_buffer, result_length_p);
}

// ----------------------------------------------------------------------
// JoinUsingToBuffer()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
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
                         int*  result_length_p) {
  CHECK(result_buffer != NULL);
  const int num_components = components.size();
  const int max_str_len = result_buffer_size - 1;
  char* curr_dest = result_buffer;
  int num_chars = 0;
  for (int i = 0; (i < num_components) && (num_chars < max_str_len); ++i) {
    const char* curr_src = components[i];
    while ((*curr_src != '\0') && (num_chars < max_str_len)) {
      *curr_dest = *curr_src;
      ++num_chars;
      ++curr_dest;
      ++curr_src;
    }
    if (i != (num_components-1)) {  // not the last component ==> add separator
      curr_src = delim;
      while ((*curr_src != '\0') && (num_chars < max_str_len)) {
        *curr_dest = *curr_src;
        ++num_chars;
        ++curr_dest;
        ++curr_src;
      }
    }
  }

  if (result_buffer_size > 0)
    *curr_dest = '\0';  // add null termination
  if (result_length_p != NULL)  // set string length value
    *result_length_p = num_chars;

  return result_buffer;
}

// ----------------------------------------------------------------------
// JoinStrings()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    it uses strings instead of char *s.
//
// ----------------------------------------------------------------------

void JoinStringsInArray(string const* const* components,
                        int num_components,
                        const char* delim,
                        string * result) {
  CHECK(result != NULL);
  result->clear();
  for (int i = 0; i < num_components; i++) {
    if (i>0) {
      (*result) += delim;
    }
    (*result) += *(components[i]);
  }
}

void JoinStringsInArray(string const *components,
                        int num_components,
                        const char *delim,
                        string *result) {
  JoinStringsIterator(components,
                      components + num_components,
                      delim,
                      result);
}

// ----------------------------------------------------------------------
// JoinMapKeysAndValues()
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
                          string* result) {
  JoinKeysAndValuesIterator(components.begin(), components.end(),
                            intra_delim, inter_delim,
                            result);
}

void JoinVectorKeysAndValues(const vector< pair<string, string> >& components,
                             const StringPiece& intra_delim,
                             const StringPiece& inter_delim,
                             string* result) {
  JoinKeysAndValuesIterator(components.begin(), components.end(),
                            intra_delim, inter_delim,
                            result);
}

// ----------------------------------------------------------------------
// JoinCSVLine()
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
// ----------------------------------------------------------------------
void JoinCSVLineWithDelimiter(const vector<string>& cols, char delimiter,
                              string* output) {
  CHECK(output);
  CHECK(output->empty());
  vector<string> quoted_cols;

  const string delimiter_str(1, delimiter);
  const string escape_chars = delimiter_str + "\"";

  // If the string contains the delimiter or " anywhere, or begins or ends with
  // whitespace (ie ascii_isspace() returns true), escape all double-quotes and
  // bracket the string in double quotes. string.rbegin() evaluates to the last
  // character of the string.
  for (int i = 0; i < cols.size(); ++i) {
    if ((cols[i].find_first_of(escape_chars) != string::npos) ||
        (!cols[i].empty() && (ascii_isspace(*cols[i].begin()) ||
                              ascii_isspace(*cols[i].rbegin())))) {
      // Double the original size, for escaping, plus two bytes for
      // the bracketing double-quotes, and one byte for the closing \0.
      int size = 2 * cols[i].size() + 3;
      scoped_array<char> buf(new char[size]);

      // Leave space at beginning and end for bracketing double-quotes.
      int escaped_size = strings::EscapeStrForCSV(cols[i].c_str(),
                                                  buf.get() + 1, size - 2);
      CHECK_GE(escaped_size, 0) << "Buffer somehow wasn't large enough.";
      CHECK_GE(size, escaped_size + 3)
        << "Buffer should have one space at the beginning for a "
        << "double-quote, one at the end for a double-quote, and "
        << "one at the end for a closing '\0'";
      *buf.get() = '"';
      *((buf.get() + 1) + escaped_size) = '"';
      *((buf.get() + 1) + escaped_size + 1) = '\0';
      quoted_cols.push_back(string(buf.get(), buf.get() + escaped_size + 2));
    } else {
      quoted_cols.push_back(cols[i]);
    }
  }
  JoinStrings(quoted_cols, delimiter_str, output);
}

void JoinCSVLine(const vector<string>& cols, string* output) {
  JoinCSVLineWithDelimiter(cols, ',', output);
}

string JoinCSVLine(const vector<string>& cols) {
  string output;
  JoinCSVLine(cols, &output);
  return output;
}
