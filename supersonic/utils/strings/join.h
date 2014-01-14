// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for joining ranges of elements with an element separator.
//
#ifndef STRINGS_JOIN_H_
#define STRINGS_JOIN_H_

#include <stdio.h>
#include <string.h>

#include <unordered_map>  // Not used in this file.
#include <unordered_set>  // Not used in this file.
#include <iterator>
#include "supersonic/utils/std_namespace.h"
#include <map>
using std::map;
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/template_util.h"
#include "supersonic/utils/strings/join_internal.h"
#include "supersonic/utils/strings/numbers.h"
#include "supersonic/utils/strings/strcat.h"    // For backward compatibility.
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/hash/hash.h"

#ifdef LANG_CXX11
#include <initializer_list>  // NOLINT(build/include_order)
#include <tuple>  // NOLINT(build/include_order)
#endif  // LANG_CXX11

namespace strings {

//                              strings::Join()
//
// The strings::Join() function joins the given range of elements, with each
// element separated by the given separator string, and returns the result as a
// string. Ranges may be specified by passing a container with begin() and end()
// members, a brace-initialized std::initializer_list, as individual begin
// and end iterators, or as a std::tuple of heterogeneous objects. The separator
// string is taken as a StringPiece, which means it can be specified as a
// string literal, C-string, C++ string, etc. By default, non-string
// elements are converted to strings using AlphaNum, which yields the same
// behavior as using StrCat(). This means that strings::Join() works
// out-of-the-box on collections of strings, ints, floats, doubles, etc.  An
// optional final argument of a "Formatter" (details below) function object
// may be given. This object will be responsible for converting each
// argument in the Range to a string.
//
// Example 1:
//   // Joins a collection of strings. This also works with a collection of
//   // StringPiece or even const char*.
//   vector<string> v = util::gtl::Container("foo", "bar", "baz");
//   string s = strings::Join(v, "-");
//   EXPECT_EQ("foo-bar-baz", s);
//
// Example 2:
//   // Joins the values in the given std::initializer_list<> specified using
//   // brace initialization. This also works with an initializer_list of ints
//   // or StringPiece--any AlphaNum-compatible type.
//   string s = strings::Join({"foo", "bar", "baz"}, "-");
//   EXPECT_EQ("foo-bar-baz", s);
//
// Example 3:
//   // Joins a collection of ints. This also works with floats, doubles,
//   // int64s; any StrCat-compatible type.
//   vector<int> v = util::gtl::Container(1, 2, 3, -4);
//   string s = strings::Join(v, "-");
//   EXPECT_EQ("1-2-3--4", s);
//
// Example 4:
//   // Joins a collection of pointer-to-int. By default, pointers are
//   // dereferenced and the pointee is formatted using AlphaNum.
//   int x = 1, y = 2, z = 3;
//   vector<int*> v = util::gtl::Container(&x, &y, &z);
//   string s = strings::Join(v, "-");
//   EXPECT_EQ("1-2-3", s);
//
// Example 5:
//   // Joins a map, with each key-value pair separated by an equals sign.
//   // This would also work with, say, a vector<pair<>>.
//    map<string, int> m = util::gtl::Container(
//        std::make_pair("a", 1),
//        std::make_pair("b", 2),
//        std::make_pair("c", 3));
//   string s = strings::Join(m, ",", strings::PairFormatter("="));
//   EXPECT_EQ("a=1,b=2,c=3", s);
//
// Example 6:
//   // These examples show how strings::Join() handles a few common edge cases.
//   vector<string> v_empty;
//   EXPECT_EQ("", strings::Join(v_empty, "-"));
//
//   vector<string> v_one_item = util::gtl::Container("foo");
//   EXPECT_EQ("foo", strings::Join(v_one_item, "-"));
//
//   vector<string> v_empty_string = util::gtl::Container("");
//   EXPECT_EQ("", strings::Join(v_empty_string, "-"));
//
//   vector<string> v_one_item_empty_string = util::gtl::Container("a", "");
//   EXPECT_EQ("a-", strings::Join(v_one_item_empty_string, "-"));
//
//   vector<string> v_two_empty_string = util::gtl::Container("", "");
//   EXPECT_EQ("-", strings::Join(v_two_empty_string, "-"));
//
// Example 7:
//   // Join a std::tuple<T...>.
//   string s = strings::Join(std::make_tuple(123, "abc", 0.456), "-");
//   EXPECT_EQ("123-abc-0.456", s);
//

//
// Formatters
//
// A Formatter is a function object that is responsible for formatting its
// argument as a string and appending it to the given output string. Formatters
// are an extensible part of the Join2 API: They allow callers to provide their
// own conversion function to enable strings::Join() work with arbitrary types.
//
// The following is an example Formatter that simply uses StrAppend to format an
// integer as a string.
//
//   struct MyFormatter {
//     void operator()(string* out, int i) const {
//       StrAppend(out, i);
//     }
//   };
//
// You would use the above formatter by passing an instance of it as the final
// argument to strings::Join():
//
//   vector<int> v = util::gtl::Container(1, 2, 3, 4);
//   string s = strings::Join(v, "-", MyFormatter());
//   EXPECT_EQ("1-2-3-4", s);
//
// The following standard formatters are provided with the Join2 API.
//
// - AlphaNumFormatter (the default)
// - PairFormatter
// - DereferenceFormatter
//

// AlphaNumFormatter()
//
// Default formatter used if none is specified. Uses AlphaNum to convert numeric
// arguments to strings.
inline internal::AlphaNumFormatterImpl AlphaNumFormatter() {
  return internal::AlphaNumFormatterImpl();
}

// PairFormatter()
//
// Formats a std::pair by putting the given separator between the pair's .first
// and .second members. The separator argument is required. By default, the
// first and second members are themselves formatted using AlphaNumFormatter(),
// but the caller may specify other formatters to use for the members.
template <typename FirstFormatter, typename SecondFormatter>
inline internal::PairFormatterImpl<FirstFormatter, SecondFormatter>
PairFormatter(FirstFormatter f1, StringPiece sep, SecondFormatter f2) {
  return internal::PairFormatterImpl<FirstFormatter, SecondFormatter>(
      f1, sep, f2);
}
inline internal::PairFormatterImpl<
  internal::AlphaNumFormatterImpl,
  internal::AlphaNumFormatterImpl>
PairFormatter(StringPiece sep) {
  return PairFormatter(AlphaNumFormatter(), sep, AlphaNumFormatter());
}

// DereferenceFormatter()
//
// Dereferences its argument then formats it using AlphaNumFormatter (by
// default), or the given formatter if one is explicitly given. This is useful
// for formatting a container of pointer-to-T. This pattern often shows up when
// joining repeated fields in protocol buffers.
template <typename Formatter>
internal::DereferenceFormatterImpl<Formatter>
DereferenceFormatter(Formatter f) {
  return internal::DereferenceFormatterImpl<Formatter>(f);
}
inline internal::DereferenceFormatterImpl<internal::AlphaNumFormatterImpl>
DereferenceFormatter() {
  return internal::DereferenceFormatterImpl<internal::AlphaNumFormatterImpl>(
      AlphaNumFormatter());
}

//
// strings::Join() overloads
//

template <typename Iterator, typename Formatter>
string Join(Iterator start, Iterator end, StringPiece sep, Formatter fmt) {
  return internal::JoinAlgorithm(start, end, sep, fmt);
}

template <typename Range, typename Formatter>
string Join(const Range& range, StringPiece separator, Formatter fmt) {
  return Join(range.begin(), range.end(), separator, fmt);
}

#ifdef LANG_CXX11
template <typename T, typename Formatter>
string Join(std::initializer_list<T> il, StringPiece separator, Formatter fmt) {
  return Join(il.begin(), il.end(), separator, fmt);
}

template <typename... T, typename Formatter>
string Join(const std::tuple<T...>& value, StringPiece separator,
            Formatter fmt) {
  return internal::JoinAlgorithm(value, separator, fmt);
}
#endif  // LANG_CXX11

template <typename Iterator>
string Join(Iterator start, Iterator end, StringPiece separator) {
  // No formatter was explicitly given, so a default must be chosen.
  typedef typename std::iterator_traits<Iterator>::value_type ValueType;
  typedef typename internal::DefaultFormatter<ValueType>::Type Formatter;
  return internal::JoinAlgorithm(start, end, separator, Formatter());
}

template <typename Range>
string Join(const Range& range, StringPiece separator) {
  return Join(range.begin(), range.end(), separator);
}

#ifdef LANG_CXX11
template <typename T>
string Join(std::initializer_list<T> il, StringPiece separator) {
  return Join(il.begin(), il.end(), separator);
}

template <typename... T>
string Join(const std::tuple<T...>& value, StringPiece separator) {
  return internal::JoinAlgorithm(value, separator, AlphaNumFormatter());
}
#endif  // LANG_CXX11

}  // namespace strings

// ----------------------------------------------------------------------
// LEGACY(jgm): Utilities provided in util/csv/writer.h are now preferred for
//
// Example for CSV formatting a single record (a sequence container of string,
// char*, or StringPiece values) using the util::csv::WriteRecordToString helper
// function:
//   std::vector<string> record = ...;
//   string line = util::csv::WriteRecordToString(record);
//
// NOTE: When writing many records, use the util::csv::Writer class directly.
//
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
//
//                              DEPRECATED(jgm)
//                  Everything is deprecated from here down.
//                        Use strings::Join() instead.
//
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// DEPRECATED(jgm): Use strings::Join().
//
// JoinStrings(), JoinStringsIterator()
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
//    There are two flavors of each function, one flavor returns the
//    concatenated string, another takes a pointer to the target string. In
//    the latter case the target string is cleared and overwritten.
// ----------------------------------------------------------------------

// DEPRECATED(jgm): Use strings::Join()
template <class CONTAINER>
void JoinStrings(const CONTAINER& components,
                 StringPiece delim,
                 string* result) {
  *result = strings::Join(components, delim);
}

// DEPRECATED(jgm): Use strings::Join()
template <class ITERATOR>
void JoinStringsIterator(const ITERATOR& start,
                         const ITERATOR& end,
                         StringPiece delim,
                         string* result) {
  *result = strings::Join(start, end, delim);
}

#endif  // STRINGS_JOIN_H_
