// This file declares INTERNAL parts of the Join API that are inlined/templated
// or otherwise need to be available at compile time. The main abstractions
// defined in this file are:
//
//   - A handful of default Formatters
//   - JoinAlgorithm() overloads
//   - JoinTuple()
//
// DO NOT INCLUDE THIS FILE DIRECTLY. Use this file by including
// strings/join.h.
//
// IWYU pragma: private, include "strings/join.h"

#ifndef STRINGS_JOIN_INTERNAL_H_
#define STRINGS_JOIN_INTERNAL_H_

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/strings/strcat.h"

namespace strings {
namespace internal {

//
// Formatter objects
//
// The following are implementation classes for standard Formatter objects. The
// factory functions that users will call to create and use these formatters are
// defined and documented in strings/join.h.
//

// A type that's used to overload the JoinAlgorithm() function (defined below)
// for ranges that do not require additional formatting (e.g., a range of
// strings).
struct NoFormatter {};

// The default formatter. Converts alpha-numeric types to strings.
struct AlphaNumFormatterImpl {
  void operator()(string* out, const AlphaNum& a) const {
    out->append(a.data(), a.size());
  }
};

// Formats a std::pair<>. The 'first' member is formatted using f1_ and the
// 'second' member is formatted using f2_. sep_ is the separator.
template <typename F1, typename F2>
class PairFormatterImpl {
 public:
  PairFormatterImpl(F1 f1, StringPiece sep, F2 f2)
      : f1_(f1), sep_(sep.ToString()), f2_(f2) {}

  template <typename T>
  void operator()(string* out, const T& p) const {
    f1_(out, p.first);
    out->append(sep_);
    f2_(out, p.second);
  }

 private:
  F1 f1_;
  string sep_;
  F2 f2_;
};

// Wraps another formatter and dereferences the argument to operator() then
// passes the dereferenced argument to the wrapped formatter. This can be
// useful, for example, to join a vector<int*>.
template <typename Formatter>
class DereferenceFormatterImpl {
 public:
  DereferenceFormatterImpl() : f_() {}
  explicit DereferenceFormatterImpl(Formatter f) : f_(f) {}

  template <typename T>
  void operator()(string* out, T* t) const {
    f_(out, *t);
  }

 private:
  Formatter f_;
};

// DefaultFormatter<T> is a traits class that selects a default Formatter to use
// for the given type T. The ::Type member names the Formatter to use. This is
// used by the strings::Join() functions that do NOT take a Formatter argument,
// in which case a default Formatter must be chosen.
//
// AlphaNumFormatterImpl is the default in the base template, followed by
// specializations for other types.
template <typename ValueType> struct DefaultFormatter {
  typedef AlphaNumFormatterImpl Type;
};
template <> struct DefaultFormatter<const char*> {
  typedef AlphaNumFormatterImpl Type;
};
template <> struct DefaultFormatter<char*> {
  typedef AlphaNumFormatterImpl Type;
};
template <> struct DefaultFormatter<string> {
  typedef NoFormatter Type;
};
template <> struct DefaultFormatter<StringPiece> {
  typedef NoFormatter Type;
};
template <typename ValueType> struct DefaultFormatter<ValueType*> {
  typedef DereferenceFormatterImpl<AlphaNumFormatterImpl> Type;
};

//
// JoinAlgorithm() functions
//

// The main joining algorithm. This simply joins the elements in the given
// iterator range, each separated by the given separator, into an output string,
// and formats each element using the provided Formatter object.
template <typename Iterator, typename Formatter>
string JoinAlgorithm(Iterator start, Iterator end, StringPiece s, Formatter f) {
  string result;
  StringPiece sep;
  for (Iterator it = start; it != end; ++it) {
    result.append(sep.data(), sep.size());
    f(&result, *it);
    sep = s;
  }
  return result;
}

// A joining algorithm that's optimized for an iterator range of string-like
// objects that do not need any additional formatting. The is to optimize the
// common case of joining, say, a vector<string> or a vector<StringPiece>.
//
// This is an overload of the previous JoinAlgorithm() function. Here the
// Formatter argument is of type NoFormatter. Since NoFormatter is an internal
// type, this overload is only invoked when strings::Join() is called with a
// vector of string-like objects (e.g., string, StringPiece), and an explicit
// Formatter argument was NOT specified.
//
// The optimization is that the needed space will be reserved in the output
// string to avoid the need to resize while appending. To do this, the iterator
// range will be traversed twice: once to calculate the total needed size, and
// then again to copy the elements and delimiters to the output string.
template <typename Iterator>
string JoinAlgorithm(Iterator start, Iterator end, StringPiece s, NoFormatter) {
  string result;

  if (start != end) {
    // Calculates space to reserve
    size_t length = 0, num_elements = 0;
    for (Iterator it = start; it != end; ++it) {
      length += it->size();
      ++num_elements;
    }
    // Adds the size of all the separators
    length += s.size() * (num_elements - 1);
    result.reserve(length);
  }

  // Joins strings
  StringPiece sep;
  for (Iterator it = start; it != end; ++it) {
    result.append(sep.data(), sep.size());
    result.append(it->data(), it->size());
    sep = s;
  }

  return result;
}

#ifdef LANG_CXX11
// JoinTupleLoop implements a loop over the elements of a std::tuple, which
// are heterogeneous. The primary template matches the tuple interior case. It
// continues the iteration after appending a separator (for nonzero indices)
// and formatting an element of the tuple. The specialization for the I=N case
// matches the end-of-tuple, and terminates the iteration.
template <typename Tup, size_t I, size_t N, typename Formatter>
struct JoinTupleLoop {
  void operator()(string* out, const Tup& tup, StringPiece sep,
                  Formatter fmt) const {
    if (I > 0) out->append(sep.data(), sep.size());
    fmt(out, std::get<I>(tup));
    JoinTupleLoop<Tup, I + 1, N, Formatter>()(out, tup, sep, fmt);
  }
};
template <typename Tup, size_t N, typename Formatter>
struct JoinTupleLoop<Tup, N, N, Formatter> {
  void operator()(string* out, const Tup& tup, StringPiece sep,
                  Formatter fmt) const {}
};

template <typename... T, typename Formatter>
string JoinAlgorithm(const std::tuple<T...>& tup, StringPiece sep,
                     Formatter fmt) {
  typedef typename std::tuple<T...> Tup;
  const size_t kTupSize = std::tuple_size<Tup>::value;
  string result;
  JoinTupleLoop<Tup, 0, kTupSize, Formatter>()(&result, tup, sep, fmt);
  return result;
}
#endif  // LANG_CXX11

}  // namespace internal
}  // namespace strings

#endif  // STRINGS_JOIN_INTERNAL_H_
