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

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/arena.h"

namespace supersonic {

static const int kInitialArenaBufferSize = 16;
static const int kMaxArenaBufferSize = 16 * 1024 * 1024;

EnumDefinition::Rep::Rep(BufferAllocator* buffer_allocator)
    : buffer_allocator_(buffer_allocator),
      arena_(buffer_allocator, kInitialArenaBufferSize,
             kMaxArenaBufferSize) {}

FailureOrVoid EnumDefinition::Rep::Add(const int32 number, StringPiece name) {
  if (name_to_number_.count(name) > 0) {
    THROW(new Exception(
        ERROR_DUPLICATE_ENUM_VALUE_NAME,
        StrCat("Name \"", name, "\" has been used in this enum previously "
               "(for #", name_to_number_[name], ")")));
  }
  if (number_to_name_.count(number) > 0) {
    THROW(new Exception(
        ERROR_DUPLICATE_ENUM_VALUE_NAME,
        StrCat("#", number, " has been used in this enum previously "
               "(with value \"", number_to_name_[number], "\")")));
  }
  const char* content = arena_.AddStringPieceContent(name);
  if (content == NULL) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        "Can't allocate memory for enum definition"));
  }
  StringPiece content_copy(content, name.size());
  name_to_number_[content_copy] = number;
  number_to_name_[number] = content_copy;
  return Success();
}

void EnumDefinition::Rep::CopyFrom(const EnumDefinition::Rep& other) {
  arena_.Reset();
  name_to_number_.clear();
  number_to_name_.clear();
  for (auto i : other.number_to_name_) {
    SucceedOrDie(Add(i.first, i.second));
  }
}

FailureOr<StringPiece> EnumDefinition::Rep::NumberToName(int32 number) const {
  auto iterator = number_to_name_.find(number);
  if (iterator == number_to_name_.end()) {
    THROW(new Exception(
        ERROR_UNDEFINED_ENUM_VALUE_NUMBER,
        StrCat("Enum #", number, " not found")));
  }
  return Success(iterator->second);
}

FailureOr<int32> EnumDefinition::Rep::NameToNumber(StringPiece name) const {
  auto iterator = name_to_number_.find(name);
  if (iterator == name_to_number_. end()) {
    THROW(new Exception(
        ERROR_UNDEFINED_ENUM_VALUE_NAME,
        StrCat("Enum name \"", name, "\" not found")));
  }
  return Success(iterator->second);
}

FailureOrVoid EnumDefinition::Rep::VerifyEquals(const EnumDefinition::Rep& a,
                                                const EnumDefinition::Rep& b) {
  // Verified by the EnumDefinition.
  CHECK_EQ(a.name_to_number_.size(), b.name_to_number_.size());
  for (auto a_iterator : a.number_to_name_) {
    auto b_iterator = b.number_to_name_.find(a_iterator.first);
    if (b_iterator == b.number_to_name_.end()) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StrCat("Only one of the enumerations has the entry #",
                 a_iterator.first, " (with name \"", a_iterator.second,
                 "\")")));
    } else if (a_iterator.second != b_iterator->second) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_TYPE_MISMATCH,
          StrCat("The enumerations have different names for #",
                 a_iterator.first, " (\"", a_iterator.second,
                 "\" vs \"", b_iterator->second, "\")")));
    }
  }
  return Success();
}

EnumDefinition::EnumDefinition() {}

FailureOrVoid EnumDefinition::AddEntry(const int32 number, StringPiece name) {
  // Lazy, to avoid creating for non-enums.
  if (rep_.get() == NULL) {
    // NOTE(user): uses the unbounded (heap) allocator for now. Given that
    // tools normally load protocol descriptors into memory anyway, it seems
    // safe (as it will only cause growth of the constant memory overhead).
    rep_.reset(new Rep(HeapBufferAllocator::Get()));
  } else if (!rep_.unique()) {
    Rep* new_rep = new Rep(rep_->buffer_allocator());
    new_rep->CopyFrom(*rep_);
    rep_.reset(new_rep);
  }
  return rep_->Add(number, name);
}

size_t EnumDefinition::entry_count() const {
  return rep_.get() == NULL ? 0 : rep_->entry_count();
}

FailureOr<StringPiece> EnumDefinition::NumberToName(int32 number) const {
  if (rep_.get() == NULL) {
    THROW(new Exception(ERROR_UNDEFINED_ENUM_VALUE_NUMBER,
                        "The enum is empty"));
  } else {
    return rep_->NumberToName(number);
  }
}

FailureOr<int32> EnumDefinition::NameToNumber(StringPiece name) const {
  if (rep_.get() == NULL) {
    THROW(new Exception(ERROR_UNDEFINED_ENUM_VALUE_NAME, "The enum is empty"));
  } else {
    return rep_->NameToNumber(name);
  }
}

FailureOrVoid EnumDefinition::VerifyEquals(const EnumDefinition& a,
                                           const EnumDefinition& b) {
  if (a.rep_.get() == b.rep_.get()) return Success();
  if (a.entry_count() != b.entry_count()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        StrCat("Enumerations have different numbers of elements: ",
               a.entry_count(), " vs ", b.entry_count())));
  }
  if (a.entry_count() == 0) return Success();
  return EnumDefinition::Rep::VerifyEquals(*a.rep_, *b.rep_);
}

bool TupleSchema::CanMerge(const TupleSchema& a, const TupleSchema& b) {
  for (int i = 0; i < a.attribute_count(); ++i) {
    if (b.LookupAttributePosition(a.attribute(i).name()) >= 0) {
      return false;
    }
  }
  for (int i = 0; i < b.attribute_count(); i++) {
    if (a.LookupAttributePosition(b.attribute(i).name()) >= 0) {
      return false;
    }
  }
  return true;
}

Attribute::~Attribute() {}

TupleSchema TupleSchema::Merge(const TupleSchema& a, const TupleSchema& b) {
  FailureOr<TupleSchema> result = TryMerge(a, b);
  CHECK(result.is_success())
      << "TupleSchema::Merge failed, "
      << result.exception().PrintStackTrace();
  return result.get();
}

FailureOr<TupleSchema> TupleSchema::TryMerge(const TupleSchema& a,
                                             const TupleSchema& b) {
  TupleSchema result(a);
  for (int i = 0; i < b.attribute_count(); ++i) {
    if (!result.add_attribute(b.attribute(i))) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_EXISTS,
          StrCat("Can't merge schemas, ambiguous attribute name: ",
                 b.attribute(i).name())));
    }
  }
  return Success(result);
}

bool TupleSchema::EqualByType(const TupleSchema& other) const {
  if (rep_.get() == other.rep_.get()) return true;
  if (attribute_count() != other.attribute_count())
    return false;
  for (int i = 0; i < attribute_count(); ++i) {
    if (attribute(i).type() != other.attribute(i).type())
      return false;
  }
  return true;
}

string TupleSchema::Rep::GetHumanReadableSpecification() const {
  string result;
  for (int i = 0; i < attribute_count(); ++i) {
    if (i > 0) result += ", ";
    result += attribute(i).name();
    result += ": ";
    result += GetTypeInfo(attribute(i).type()).name();
    if (!attribute(i).is_nullable()) result += " NOT NULL";
  }
  return result;
}

}  // namespace supersonic
