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
//
// Supersonic type system. Contains compile-time type traits, and corresponding
// run-time type information.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_TYPES_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_TYPES_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/macros.h"
#include "supersonic/proto/supersonic.pb.h"
#include <google/protobuf/descriptor.h>
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// Common traits.

struct BasicNumericTypeTraits {
  static const bool is_variable_length = false;
  static const bool is_numeric = true;
  static const bool is_unsigned = false;
};

struct BasicIntegerTypeTraits : public BasicNumericTypeTraits {
  static const bool is_integer = true;
  static const bool is_floating_point = false;
};

struct BasicFloatingPointTypeTraits : public BasicNumericTypeTraits {
  static const bool is_integer = false;
  static const bool is_floating_point = true;
};

struct BasicVariableLengthTypeTraits {
  typedef StringPiece cpp_type;
  typedef string hold_type;
  static const bool is_variable_length = true;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(const cpp_type& data) {
    return data.ToString();
  }
  // Note that this reference is critical since the created StringPiece points
  // to that data.
  static cpp_type HoldTypeToCppType(const hold_type& data) {
    return StringPiece(data);
  }
};

template<DataType datatype> struct BasicTypeTraits {};

// Specializations.

template<> struct BasicTypeTraits<INT32> : public BasicIntegerTypeTraits {
  typedef int32 cpp_type;
  typedef int32 hold_type;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<INT64> : public BasicIntegerTypeTraits {
  typedef int64 cpp_type;
  typedef int64 hold_type;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<UINT32> : public BasicIntegerTypeTraits {
  typedef uint32 cpp_type;
  typedef uint32 hold_type;
  static const bool is_unsigned = true;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<UINT64> : public BasicIntegerTypeTraits {
  typedef uint64 cpp_type;
  typedef uint64 hold_type;
  static const bool is_unsigned = true;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<FLOAT> : public BasicFloatingPointTypeTraits {
  typedef float cpp_type;
  typedef float hold_type;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<DOUBLE>
    : public BasicFloatingPointTypeTraits {
  typedef double cpp_type;
  typedef double hold_type;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<BOOL> {
  typedef bool cpp_type;
  typedef bool hold_type;
  static const bool is_variable_length = false;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<ENUM> {
  typedef int32 cpp_type;
  typedef int32 hold_type;
  static const bool is_variable_length = false;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<STRING>
    : public BasicVariableLengthTypeTraits {};  // NOLINT

template<> struct BasicTypeTraits<BINARY>
    : public BasicVariableLengthTypeTraits {};  // NOLINT

template<> struct BasicTypeTraits<DATETIME> {
  typedef int64 cpp_type;
  typedef int64 hold_type;
  static const bool is_variable_length = false;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<DATE> {
  typedef int32 cpp_type;
  typedef int32 hold_type;
  static const bool is_variable_length = false;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

template<> struct BasicTypeTraits<DATA_TYPE> {
  typedef DataType cpp_type;
  typedef DataType hold_type;
  static const bool is_variable_length = false;
  static const bool is_numeric = false;
  static const bool is_integer = false;
  static const bool is_floating_point = false;
  static hold_type CppTypeToHoldType(cpp_type data) {
    return data;
  }
  static cpp_type HoldTypeToCppType(hold_type data) {
    return data;
  }
};

// The following specialized templates enable compile-time inference of type
// properties (size, name, C++ type, etc.)
template<DataType datatype> struct TypeTraits
    : public BasicTypeTraits<datatype> {
  // hold_type is the same as cpp_type for non-variable length types. If the
  // type is variable length, the cpp_type does not own the contents, while
  // hold_type does.
  typedef typename BasicTypeTraits<datatype>::cpp_type cpp_type;
  typedef typename BasicTypeTraits<datatype>::hold_type hold_type;
  static const DataType type = datatype;
  static const size_t size = sizeof(cpp_type);
  static const google::protobuf::EnumValueDescriptor* descriptor() {
    return DataType_descriptor()->FindValueByNumber(datatype);
  }
  static const char* name() {
    return descriptor()->name().c_str();
  }
  static hold_type CppTypeToHoldType(const cpp_type& data) {
    return BasicTypeTraits<datatype>::CppTypeToHoldType(data);
  }
  // Note that we need to pass reference since StringPiece needs to original
  // string and not the copied string.
  static cpp_type HoldTypeToCppType(const hold_type& data) {
    return BasicTypeTraits<datatype>::HoldTypeToCppType(data);
  }
};

// Represents 'RowID'. Should be used everywhere when we refer to row IDs.
typedef int64 rowid_t;

// Represents 'row count'. Unsigned. Should be used everywhere we refer to
// row counts or positive row offsets.
typedef uint64 rowcount_t;

// Lets to obtain the DataType enumeration that corresponds to the specified
// C++ type.
template<typename T> struct InverseTypeTraits {};

template<> struct InverseTypeTraits<int32> {
  static const DataType supersonic_type = INT32;
};

template<> struct InverseTypeTraits<uint32> {
  static const DataType supersonic_type = UINT32;
};

template<> struct InverseTypeTraits<int64> {
  static const DataType supersonic_type = INT64;
};

template<> struct InverseTypeTraits<uint64> {
  static const DataType supersonic_type = UINT64;
};

template<> struct InverseTypeTraits<float> {
  static const DataType supersonic_type = FLOAT;
};

template<> struct InverseTypeTraits<double> {
  static const DataType supersonic_type = DOUBLE;
};

template<> struct InverseTypeTraits<bool> {
  static const DataType supersonic_type = BOOL;
};

template<> struct InverseTypeTraits<StringPiece> {
  static const DataType supersonic_type = BINARY;
};

template<> struct InverseTypeTraits<DataType> {
  static const DataType supersonic_type = DATA_TYPE;
};

// Convenience alias for a DataType enum that corresponds to the rowid_t type.
// If you want to put row IDs in a column, this should be the type of that
// column.
const DataType kRowidDatatype = InverseTypeTraits<rowid_t>::supersonic_type;

// Dynamic type information. Each value from enum Type has a corresponding
// singleton TypeInfo object, that can be obtained by calling GetTypeInfo(type).
class TypeInfo {
 public:
  // Returns the enum Type value that this typeinfo corresponds to.
  DataType type() const { return type_; }

  // Returns the string name of this type.
  const string& name() const { return name_; }

  // Returns the number of bytes that a value of that type uses in memory.
  size_t size() const { return size_; }

  // Returns the log2 of the size(), so that offsets can be quickly computed
  // using left shift.
  size_t log2_size() const { return log2_size_; }

  // Returns if the type represents a variable length type, allocated using
  // an arena and represented by StringPiece.
  bool is_variable_length() const { return is_variable_length_; }

  // Returns true if the type represents a number; false otherwise.
  bool is_numeric() const { return is_numeric_; }

  // Returns true if the type represents an integer number; false otherwise.
  bool is_integer() const { return is_integer_; }

  // Returns true if the type represents a floating point number; false
  // otherwise.
  bool is_floating_point() const { return is_floating_point_; }

 private:
  // Templatized constructor, called by TypeInfoResolver.
  template<typename Type> TypeInfo(Type ignored);

  const DataType type_;
  const string name_;
  const size_t size_;
  const size_t log2_size_;
  const bool is_numeric_;
  const bool is_integer_;
  const bool is_floating_point_;
  const bool is_variable_length_;
  friend class TypeInfoResolver;
  DISALLOW_COPY_AND_ASSIGN(TypeInfo);
};

// Returns a pointer to the singleton TypeInfo object associated with the
// specified enum Type value. The object is statically-created and owned
// by the framework.
const TypeInfo& GetTypeInfo(DataType type);

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_TYPES_H_
