// Copyright 2010 Google Inc.  All Rights Reserved
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

#include "supersonic/base/infrastructure/types_infrastructure.h"

#include <time.h>

#include <cmath>
#include <limits>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/walltime.h"
#include "supersonic/utils/strings/numbers.h"
#include "supersonic/utils/strings/strip.h"
#include "supersonic/utils/walltime.h"

namespace {

const char* const kDefaultDateTimeParseFormat = " %Y/%m/%d-%H:%M:%S ";
const char* const kDefaultDateTimePrintFormat = "%Y/%m/%d-%H:%M:%S";

const char* const kDefaultDateParseFormat = " %Y/%m/%d ";
const char* const kDefaultDatePrintFormat = "%Y/%m/%d";

}  // end anonymous namespace

namespace supersonic {

// Specializations of PrintTyped.

template<> void PrintTyped<INT32>(const int32& value, string* const target) {
  char buffer[kFastToBufferSize];
  target->append(FastInt32ToBuffer(value, buffer));
}

template<> void PrintTyped<UINT32>(const uint32& value, string* const target) {
  char buffer[kFastToBufferSize];
  target->append(FastUInt32ToBuffer(value, buffer));
}

template<> void PrintTyped<INT64>(const int64& value, string* const target) {
  char buffer[kFastToBufferSize];
  target->append(FastInt64ToBuffer(value, buffer));
}

template<> void PrintTyped<UINT64>(const uint64& value, string* const target) {
  char buffer[kFastToBufferSize];
  target->append(FastUInt64ToBuffer(value, buffer));
}

template<> void PrintTyped<FLOAT>(const float& value, string* const target) {
  target->append(SimpleFtoa(value));
}

template<> void PrintTyped<DOUBLE>(const double& value, string* const target) {
  target->append(SimpleDtoa(value));
}

template<> void PrintTyped<BOOL>(const bool& value, string* const target) {
  target->append((value) ? "TRUE" : "FALSE");
}

template<> void PrintTyped<ENUM>(const int32& value, string* const target) {
  // NOTE: usually, this default won't be used; the higher-level function
  // (e.g. ViewPrinter) will handle ENUMs in a special way.
  return PrintTyped<INT32>(value, target);
}

template<> void PrintTyped<STRING>(const StringPiece& value,
                                   string* const target) {
  value.AppendToString(target);
}

// TODO(user): Support for the entire int64 datetime range, print
// microseconds as well.
template<> void PrintTyped<DATETIME>(const int64& value, string* const target) {
  const time_t time = value / 1000000;
  const size_t previous_size = target->size();
  // Show in UTC.
  StringAppendStrftime(target, kDefaultDateTimePrintFormat, time, false);
  // StringAppendStrftime() reports errors by leaving target unmodified.
  if (target->size() == previous_size) {
    target->append("NULL");
  }
}

// TODO(user): Support for the entire int32 date range.
template<> void PrintTyped<DATE>(const int32& value, string* const target) {
  const time_t time = value * (24 * 3600);
  const size_t previous_size = target->size();
  // Show in UTC.
  StringAppendStrftime(target, kDefaultDatePrintFormat, time, false);
  // StringAppendStrftime() reports errors by leaving target unmodified.
  if (target->size() == previous_size) {
    target->append("NULL");
  }
}

template<>
void PrintTyped<BINARY>(const StringPiece& value, string* const target) {
  StringAppendF(target, "<0x");
  // This is to enforce uniform printing as the signed-ness of the char type is
  // platform dependent.
  const unsigned char* p = reinterpret_cast<const unsigned char*>(value.data());
  for (int i = 0; i < value.size(); ++i) {
    if (i % 4 == 0) StringAppendF(target, " ");
    StringAppendF(target, "%02x", *p++);
  }
  StringAppendF(target, ">");
}

template<>
void PrintTyped<DATA_TYPE>(const DataType& value, string* const target) {
  target->append(DataType_Name(value));
}

template<DataType type>
void DefaultPrinter(const VariantConstPointer value, string* target) {
  DCHECK(target);
  if (value.is_null()) {
    target->append("NULL");
  } else {
    PrintTyped<type>(*value.as<type>(), target);
  }
}

struct PrinterResolver {
  template<DataType type>
  AttributePrinter operator()() const { return &DefaultPrinter<type>; }
};

AttributePrinter GetDefaultPrinterFn(DataType type) {
  return TypeSpecialization<AttributePrinter, PrinterResolver>(type);
}

// Specializations of ParseTyped.

template<> bool ParseTyped<INT32>(const char* value, int32* target) {
  return safe_strto32(value, target);
}

template<> bool ParseTyped<UINT32>(const char* value, uint32* target) {
  return safe_strtou32(value, target);
}

template<> bool ParseTyped<INT64>(const char* value, int64* target) {
  return safe_strto64(value, target);
}

template<> bool ParseTyped<UINT64>(const char* value, uint64* target) {
  return safe_strtou64(value, target);
}

template<> bool ParseTyped<FLOAT>(const char* value, float* target) {
  return safe_strtof(value, target);
}

template<> bool ParseTyped<DOUBLE>(const char* value, double* target) {
  return safe_strtod(value, target);
}

template<> bool ParseTyped<BOOL>(const char* value, bool* target) {
  // Common case (no trimming needed): avoid malloc.
  if (strcasecmp("true", value) == 0 ||
      strcasecmp("yes", value) == 0) {
    *target = true;
    return true;
  } else if (strcasecmp("false", value) == 0 ||
             strcasecmp("no", value) == 0) {
    *target = false;
    return true;
  } else {
    // Fall back to the version that does the trimming.
    // TODO(user):  Do this without malloc as well.
    string s(value);
    TrimString(&s, " \t\r");
    if (strcasecmp("true", s.c_str()) == 0 ||
        strcasecmp("yes", s.c_str()) == 0) {
      *target = true;
      return true;
    } else if (strcasecmp("false", s.c_str()) == 0  ||
               strcasecmp("no", s.c_str()) == 0) {
      *target = false;
      return true;
    } else {
      return false;
    }
  }
}

// TODO(user):  Fix WallTime_Parse_Timezone():
// - It doesn't handle the time before 1970 correctly (time is set to -1, but
// true is returned).
// - It doesn't accept fractional seconds if the format contains whitespace
// at either ends.
bool ParseDateTime(const string& value,
                   const string& format,
                   int64* const target) {
  WallTime time = 0;
  if (!WallTime_Parse_Timezone(value.c_str(),
                               format.c_str(),
                               NULL,  // no default value for missing fields
                               false,  // use UTC,
                               &time)) {
    return false;
  }
  if (time < 0) {
    return false;
  }
  const double time_microseconds = floor(time * 1e6 + .5);
  if (time_microseconds >= numeric_limits<int64>::min() &&
      time_microseconds <= numeric_limits<int64>::max()) {
    *target = time_microseconds;
    return true;
  } else {
    return false;
  }
}

template<>
bool ParseTyped<DATETIME>(const char* value, int64* target) {
  return ParseDateTime(value, kDefaultDateTimeParseFormat, target);
}

// TODO(user): Test it after ParseDateTime is fixed (see TODOs above).
template<>
bool ParseTyped<DATE>(const char* value, int32* target) {
  int64 time;
  if (ParseDateTime(value, kDefaultDateParseFormat, &time)) {
    // The divisor is > 2^32, so the result won't overflow int32.
    static const int64 microseconds_per_day = 24LL * 3600LL * 1000000LL;
    if (time >= 0) {
      *target = time / microseconds_per_day;
    } else {
      // Revert sign, round up (instead of down), revert sign again.
      *target = -((microseconds_per_day - time - 1) / microseconds_per_day);
    }
    return true;
  } else {
    return false;
  }
}

template<>
bool ParseTyped<DATA_TYPE>(const char* value, DataType* target) {
  return DataType_Parse(string(value), target);
}

template<DataType type>
bool DefaultParser(const char* value, const VariantPointer target) {
  DCHECK(value != NULL);
  DCHECK(!target.is_null());
  return ParseTyped<type>(value, target.as<type>());
}

template<>
bool DefaultParser<ENUM>(const char* value, const VariantPointer target) {
  LOG(FATAL) << "Parser for ENUM is not defined";
}

template<>
bool DefaultParser<STRING>(const char* value, const VariantPointer target) {
  LOG(FATAL) << "Parser for STRING is not defined";
}

template<>
bool DefaultParser<BINARY>(const char* value, const VariantPointer target) {
  LOG(FATAL) << "Parser for BINARY is not defined";
}

struct ParserResolver {
  template<DataType type>
  AttributeParser operator()() const { return &DefaultParser<type>; }
};

AttributeParser GetDefaultParserFn(DataType type) {
  return TypeSpecialization<AttributeParser, ParserResolver>(type);
}

// NOTE(user): many of the following aren't used in the existing code, and
// perhaps never will be in the current form. Revisit once we consider the
// API functionally 'code complete'. (i.e. when we have at least MergeUnionAll,
// and MergeJoin).
// TODO(user): reconcile with TypeSpecialization(), at least for stuff like
// GetSortComparator that depends on a single type.

// Following magic is to materialize gozillion function templates and be able
// to return pointers to these functions at run time. The problem, in general,
// is that we have multiple 'dimensions' of specialization: e.g. left type,
// right type, left_not_null, right_not_null, the operator we're interested in
// (hasher, comparator, ...), and often some other properties (descending
// order, ...). General approach is to resolve one dimension at a time, through
// a swith or if statement, and invoke increasingly specialized template
// resolvers.

// Functors to abstract away an operation to be performed on two parameters.
// Each must provide:
// 1. a templatized struct Bind, with
//    1a. operator() that performs the operation in question on two typed
//        parameters;
//    1b. a static method 'Evaluate', accepting two const void* parameters,
//        and calling the operator() with the appropriate static cast;
// 2. typedef RuntimeFunction, declaring the signature of functions that
//    this operation generates.

struct Equal {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind
      : public EqualityWithNullsComparator<left_type, right_type,
                                           left_not_null, right_not_null> {
    static bool Evaluate(const VariantConstPointer left,
                         const VariantConstPointer right) {
      Bind op;
      return op(left.as<left_type>(), right.as<right_type>());
    }
  };
  typedef EqualityComparator RuntimeFunction;
};

// Helper base class for Bind on 'inequality' functors. See
// InequalityWithNullsComparator docstring (types.h) for parameter description.
template<DataType left_type, DataType right_type,
         bool left_not_null, bool right_not_null,
         bool descending, bool weak, bool ignore_equal_null>
struct Inequality
    : public InequalityWithNullsComparator<left_type, right_type,
                                           left_not_null, right_not_null,
                                           descending, weak,
                                           ignore_equal_null> {
  static ComparisonResult Evaluate(const VariantConstPointer left,
                                   const VariantConstPointer right) {
    Inequality op;
    return op(left.as<left_type>(), right.as<right_type>());
  }
};

// Generates functions for non-terminal elements in sort comparators,
// ascending order. These comparators don't distinguish between RESULT_EQUAL
// and RESULT_EQUAL_NULL (returning RESULT_EQUAL in both cases).
struct AscendingSortComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  false, false, true> {};
  typedef InequalityComparator RuntimeFunction;
};

// Generates functions for terminal (last) elements in sort comparators,
// ascending order. Such comparators are 'weak' (don't distinguish RESULT_EQUAL
// from RESULT_GREATER), thus a bit cheaper.
struct AscendingSortTerminalComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  false, true, true> {};
  typedef InequalityComparator RuntimeFunction;
};

// Generates functions for non-terminal elements in sort comparators,
// descending order. These comparators don't distinguish between RESULT_EQUAL
// and RESULT_EQUAL_NULL (returning RESULT_EQUAL in both cases).
struct DescendingSortComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  true, false, true> {};
  typedef InequalityComparator RuntimeFunction;
};

// Generates functions for terminal (last) elements in sort comparators,
// descending order. Such comparators are 'weak' (don't distinguish RESULT_EQUAL
// from RESULT_GREATER), thus a bit cheaper.
struct DescendingSortTerminalComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  true, true, true> {};
  typedef InequalityComparator RuntimeFunction;
};

// Generates functions for comparators in merge-joins, ascending order.
// Such comparators always require non-weak semantics, and that RESULT_EQUALS is
// distinguished from RESULT_EQUALS_NULL.
struct AscendingMergeJoinComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  false, false, false> {};
  typedef InequalityComparator RuntimeFunction;
};

// Generates functions for comparators in merge-joins, descending order.
// Such comparators always require non-weak semantics, and that RESULT_EQUALS is
// distinguished from RESULT_EQUALS_NULL.
struct DescendingMergeJoinComparator {
  template<DataType left_type, DataType right_type,
           bool left_not_null, bool right_not_null>
  struct Bind : public Inequality<left_type, right_type,
                                  left_not_null, right_not_null,
                                  true, false, false> {};
  typedef InequalityComparator RuntimeFunction;
};

// The successive resolve magic below.

template<typename Op,
         DataType left_type, DataType right_type,
         bool left_not_null, bool right_not_null>
typename Op::RuntimeFunction BindComparator() {
  return &Op::template Bind<left_type, right_type,
                            left_not_null, right_not_null>::Evaluate;
}

template<typename Op, DataType left_type, DataType right_type>
typename Op::RuntimeFunction ResolveComparatorForKnownTypes(
    bool left_not_null,
    bool right_not_null) {
  if (left_not_null) {
    if (right_not_null) {
      return BindComparator<Op, left_type, right_type, true, true>();
    } else {
      return BindComparator<Op, left_type, right_type, true, false>();
    }
  } else {
    if (right_not_null) {
      return BindComparator<Op, left_type, right_type, false, true>();
    } else {
      return BindComparator<Op, left_type, right_type, false, false>();
    }
  }
}

template<typename Op, DataType left_type>
typename Op::RuntimeFunction ResolveComparatorForNumericLeftType(
    DataType right_type,
    bool left_not_null,
    bool right_not_null) {
  switch (right_type) {
    case INT32:
      return ResolveComparatorForKnownTypes<Op, left_type, INT32>(
          left_not_null, right_not_null);
    case UINT32:
      return ResolveComparatorForKnownTypes<Op, left_type, UINT32>(
          left_not_null, right_not_null);
    case INT64:
      return ResolveComparatorForKnownTypes<Op, left_type, INT64>(
          left_not_null, right_not_null);
    case UINT64:
      return ResolveComparatorForKnownTypes<Op, left_type, UINT64>(
          left_not_null, right_not_null);
    case FLOAT:
      return ResolveComparatorForKnownTypes<Op, left_type, FLOAT>(
          left_not_null, right_not_null);
    case DOUBLE:
      return ResolveComparatorForKnownTypes<Op, left_type, DOUBLE>(
          left_not_null, right_not_null);
    default:
      LOG(FATAL) << "Can't compare numeric type "
                 << TypeTraits<left_type>::name() << " to non-numeric type "
                 << GetTypeInfo(right_type).name();
  }
}

template<typename Op>
typename Op::RuntimeFunction ResolveComparatorForHomogeneousTypes(
    DataType type,
    bool left_not_null,
    bool right_not_null) {
  switch (type) {
    case STRING:
      return ResolveComparatorForKnownTypes<Op, STRING, STRING>(
          left_not_null, right_not_null);
    case BINARY:
      return ResolveComparatorForKnownTypes<Op, BINARY, BINARY>(
          left_not_null, right_not_null);
    case BOOL:
      return ResolveComparatorForKnownTypes<Op, BOOL, BOOL>(
          left_not_null, right_not_null);
    case DATETIME:
      return ResolveComparatorForKnownTypes<Op, DATETIME, DATETIME>(
          left_not_null, right_not_null);
    case DATE:
      return ResolveComparatorForKnownTypes<Op, DATE, DATE>(
          left_not_null, right_not_null);
    case DATA_TYPE:
      // TODO(user): inqeuality (as opposed to equality) should fail.
      return ResolveComparatorForKnownTypes<Op, DATA_TYPE, DATA_TYPE>(
          left_not_null, right_not_null);
    case ENUM:
      // For the purpose of default ordering (e.g. when ORDER BY uses an
      // ENUM column), enums sort and compare by the value number (not name).
      return ResolveComparatorForKnownTypes<Op, ENUM, ENUM>(
          left_not_null, right_not_null);
    default:
      LOG(FATAL) << "Comparator undefined for type "
                 << GetTypeInfo(type).name();
  }
}

template<typename Op>
typename Op::RuntimeFunction ResolveComparator(DataType left_type,
                                               DataType right_type,
                                               bool left_not_null,
                                               bool right_not_null) {
  switch (left_type) {
    case INT32:
      return ResolveComparatorForNumericLeftType<Op, INT32>(
          right_type, left_not_null, right_not_null);
    case UINT32:
      return ResolveComparatorForNumericLeftType<Op, UINT32>(
          right_type, left_not_null, right_not_null);
    case INT64:
      return ResolveComparatorForNumericLeftType<Op, INT64>(
          right_type, left_not_null, right_not_null);
    case UINT64:
      return ResolveComparatorForNumericLeftType<Op, UINT64>(
          right_type, left_not_null, right_not_null);
    case FLOAT:
      return ResolveComparatorForNumericLeftType<Op, FLOAT>(
          right_type, left_not_null, right_not_null);
    case DOUBLE:
      return ResolveComparatorForNumericLeftType<Op, DOUBLE>(
          right_type, left_not_null, right_not_null);
    default:
      CHECK_EQ(left_type, right_type)
          << "Don't know how to compare non-numeric types "
          << GetTypeInfo(left_type).name() << " and "
          << GetTypeInfo(right_type).name();
      return ResolveComparatorForHomogeneousTypes<Op>(
          left_type, left_not_null, right_not_null);
  }
}

struct DefaultSortComparatorResolver {
  DefaultSortComparatorResolver(bool descending,
                                bool is_not_null,
                                bool terminal)
      : descending(descending),
        is_not_null(is_not_null),
        terminal(terminal) {}

  template<DataType type>
  InequalityComparator operator()() const {
    if (!terminal) {
      return !descending
        ? ResolveComparatorForKnownTypes<AscendingSortComparator,
                                         type, type>(is_not_null, is_not_null)
        : ResolveComparatorForKnownTypes<DescendingSortComparator,
                                         type, type>(is_not_null, is_not_null);
    } else {
      return !descending
        ? ResolveComparatorForKnownTypes<AscendingSortTerminalComparator,
                                         type, type>(is_not_null, is_not_null)
        : ResolveComparatorForKnownTypes<DescendingSortTerminalComparator,
                                         type, type>(is_not_null, is_not_null);
    }
  }
  bool descending;
  bool is_not_null;
  bool terminal;
};

InequalityComparator GetSortComparator(DataType type,
                                       bool descending,
                                       bool is_not_null,
                                       bool terminal) {
  DefaultSortComparatorResolver resolver(descending, is_not_null, terminal);
  return TypeSpecialization<InequalityComparator,
                            DefaultSortComparatorResolver>(type, resolver);
}

template<DataType type, bool is_not_null>
size_t DefaultHashComputer(const VariantConstPointer datum) {
  HashComputer<type, operators::Hash, is_not_null> hash;
  return hash(datum.as<type>());
}

struct DefaultHasherResolver {
  explicit DefaultHasherResolver(bool is_not_null) : is_not_null(is_not_null) {}
  template<DataType type>
  Hasher operator()() const {
    if (is_not_null) {
      return &DefaultHashComputer<type, true>;
    } else {
      return &DefaultHashComputer<type, false>;
    }
  }
  bool is_not_null;
};

Hasher GetHasher(DataType type, bool is_not_null) {
  DefaultHasherResolver resolver(is_not_null);
  return TypeSpecialization<Hasher,
                            DefaultHasherResolver>(type, resolver);
}

template<DataType type, bool update, bool is_not_null>
void DefaultColumnHashComputer(const VariantConstPointer data,
                               bool_const_ptr is_null,
                               size_t const row_count,
                               size_t* hashes) {
  ColumnHashComputer<type, operators::Hash, update, is_not_null> hash;
  return hash(data.as<type>(), is_null, row_count, hashes);
}

struct DefaultColumnHasherResolver {
  DefaultColumnHasherResolver(bool update, bool is_not_null)
      : update(update),
        is_not_null(is_not_null) {}
  template<DataType type>
  ColumnHasher operator()() const {
    if (update) {
      if (is_not_null) {
        return &DefaultColumnHashComputer<type, true, true>;
      } else {
        return &DefaultColumnHashComputer<type, true, false>;
      }
    } else {
      if (is_not_null) {
        return &DefaultColumnHashComputer<type, false, true>;
      } else {
        return &DefaultColumnHashComputer<type, false, false>;
      }
    }
  }
  bool update;
  bool is_not_null;
};

ColumnHasher GetColumnHasher(DataType type, bool update, bool is_not_null) {
  DefaultColumnHasherResolver resolver(update, is_not_null);
  return TypeSpecialization<ColumnHasher,
                            DefaultColumnHasherResolver>(type, resolver);
}

EqualityComparator GetEqualsComparator(DataType left_type,
                                       DataType right_type,
                                       bool left_not_null,
                                       bool right_not_null) {
  return ResolveComparator<Equal>(left_type, right_type,
                                  left_not_null, right_not_null);
}

InequalityComparator GetMergeComparator(DataType left_type,
                                        DataType right_type,
                                        bool left_not_null,
                                        bool right_not_null,
                                        bool descending) {
  return !descending
    ? ResolveComparator<AscendingMergeJoinComparator>(
        left_type, right_type,
        left_not_null, right_not_null)
    : ResolveComparator<DescendingMergeJoinComparator>(
        left_type, right_type,
        left_not_null, right_not_null);
}

#undef CPP_TYPE

}  // namespace supersonic
