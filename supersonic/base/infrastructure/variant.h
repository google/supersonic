// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: ptab@google.com (Piotr Tabor)


#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_H_

#include "supersonic/utils/macros.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// VariantData is a type that can hold a single value of any DataType or NULL.
class VariantDatum {
 public:
  // Creates not initialized VariantData.
  VariantDatum() : is_null_(true) {}

  bool is_null() const { return is_null_; }
  DataType type() const { return type_; }

  template <DataType data_type>
  const typename TypeTraits<data_type>::hold_type& typed_value() const {
    CHECK_EQ(data_type, type_);
    CHECK(!is_null_);
    return value<data_type>();
  }

  template <DataType data_type>
  void set_typed_value(typename TypeTraits<data_type>::cpp_type value) {
    is_null_ = false;
    type_ = data_type;
    set_value<data_type>(value);
  }

  template <DataType data_type>
  static VariantDatum Create(typename TypeTraits<data_type>::cpp_type value) {
    VariantDatum data;
    data.set_typed_value<data_type>(value);
    return data;
  }

  static VariantDatum CreateNull(DataType type) {
    return VariantDatum(type);
  }

 private:
  // Construct NULL of given type.
  explicit VariantDatum(const DataType& type)
  : type_(type),
    is_null_(true) {}

  template <DataType data_type>
  const typename TypeTraits<data_type>::hold_type& value() const;


  template <DataType data_type>
  void set_value(typename TypeTraits<data_type>::cpp_type value);

  DataType type_;
  bool is_null_;

  union {
    TypeTraits<INT32>::hold_type value_int32_;
    TypeTraits<INT64>::hold_type value_int64_;
    TypeTraits<UINT32>::hold_type value_uint32_;
    TypeTraits<UINT64>::hold_type value_uint64_;
    TypeTraits<FLOAT>::hold_type value_float_;
    TypeTraits<DOUBLE>::hold_type value_double_;
    TypeTraits<BOOL>::hold_type value_bool_;
    TypeTraits<ENUM>::hold_type value_enum_;
    TypeTraits<DATE>::hold_type value_date_;
    TypeTraits<DATETIME>::hold_type value_datetime_;
    TypeTraits<DATA_TYPE>::hold_type value_data_type_;
  };
  // For STRING and BINARY.
  // Can't be part of the union because string does not have trivial copy
  // constructor.
  string value_variable_length_;

  // Copyable.
};

// --------------- typed_value<...> --------------------------------------------

template <>
inline const TypeTraits<INT32>::hold_type& VariantDatum::value<INT32>() const {
  return value_int32_;
}

template <>
inline const TypeTraits<INT64>::hold_type& VariantDatum::value<INT64>() const {
  return value_int64_;
}

template <>
inline const TypeTraits<UINT32>::hold_type& VariantDatum::value<UINT32>()
    const {
  return value_uint32_;
}

template <>
inline const TypeTraits<UINT64>::hold_type& VariantDatum::value<UINT64>()
    const {
  return value_uint64_;
}

template <>
inline const TypeTraits<FLOAT>::hold_type& VariantDatum::value<FLOAT>() const {
  return value_float_;
}

template <>
inline const TypeTraits<DOUBLE>::hold_type& VariantDatum::value<DOUBLE>()
    const {
  return value_double_;
}

template <>
inline const TypeTraits<BOOL>::hold_type& VariantDatum::value<BOOL>() const {
  return value_bool_;
}

template <>
inline const TypeTraits<ENUM>::hold_type& VariantDatum::value<ENUM>() const {
  return value_enum_;
}

template <>
inline const TypeTraits<DATE>::hold_type& VariantDatum::value<DATE>() const {
  return value_date_;
}

template <>
inline const TypeTraits<DATETIME>::hold_type& VariantDatum::value<DATETIME>()
    const {
  return value_datetime_;
}

template <>
inline const TypeTraits<DATA_TYPE>::hold_type& VariantDatum::value<DATA_TYPE>()
    const {
  return value_data_type_;
}

template <>
inline const TypeTraits<STRING>::hold_type& VariantDatum::value<STRING>()
    const {
  return value_variable_length_;
}

template <>
inline const TypeTraits<BINARY>::hold_type& VariantDatum::value<BINARY>()
    const {
  return value_variable_length_;
}

// --------------- set_value<...> ----------------------------------------

template <>
inline void VariantDatum::set_value<INT32>(TypeTraits<INT32>::cpp_type value) {
  value_int32_ = value;
}

template <>
inline void VariantDatum::set_value<INT64>(TypeTraits<INT64>::cpp_type value) {
  value_int64_ = value;
}

template <>
inline void VariantDatum::set_value<UINT32>(
    TypeTraits<UINT32>::cpp_type value) {
  value_uint32_ = value;
}

template <>
inline void VariantDatum::set_value<UINT64>(
    TypeTraits<UINT64>::cpp_type value) {
  value_uint64_ = value;
}

template <>
inline void VariantDatum::set_value<FLOAT>(TypeTraits<FLOAT>::cpp_type value) {
  value_float_ = value;
}

template <>
inline void VariantDatum::set_value<DOUBLE>(
    TypeTraits<DOUBLE>::cpp_type value) {
  value_double_ = value;
}

template <>
inline void VariantDatum::set_value<BOOL>(TypeTraits<BOOL>::cpp_type value) {
  value_bool_ = value;
}

template <>
inline void VariantDatum::set_value<DATE>(TypeTraits<DATE>::cpp_type value) {
  value_date_ = value;
}

template <>
inline void VariantDatum::set_value<DATETIME>(
    TypeTraits<DATETIME>::cpp_type value) {
  value_datetime_ = value;
}

template <>
inline void VariantDatum::set_value<DATA_TYPE>(
    TypeTraits<DATA_TYPE>::cpp_type value) {
  value_data_type_ = value;
}

template <>
inline void VariantDatum::set_value<BINARY>(
    TypeTraits<BINARY>::cpp_type value) {
  value.CopyToString(&value_variable_length_);
}

template <>
inline void VariantDatum::set_value<STRING>(
    TypeTraits<STRING>::cpp_type value) {
  value.CopyToString(&value_variable_length_);
}

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_VARIANT_H_
