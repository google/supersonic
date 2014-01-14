// Copyright 2010 Google Inc. All Rights Reserved.
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

#include "supersonic/cursor/infrastructure/view_printer.h"

#include <ostream>
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/strings/numbers.h"

namespace supersonic {

void ViewPrinter::AppendSchemaToStream(
    const TupleSchema& schema, std::ostream* s) const {
  for (size_t i = 0; i < schema.attribute_count(); ) {
      const Attribute& attribute = schema.attribute(i);
      string header = StrCat(
          GetTypeInfo(attribute.type()).name(), " ",
          "\"", attribute.name(), "\"",
          attribute.is_nullable() ? " (N)" : "");
      *s <<  Expand(header);
      if (++i < schema.attribute_count())
        *s << ", ";
    }
}

void ViewPrinter::AppendViewToStream(const View& view, std::ostream* s) const {
  if (include_header_in_representation_) {
    *s << "View"
       << "; rows: " << view.row_count()
       << "; schema: ";
    AppendSchemaToStream(view.schema(), s);
    *s << std::endl;
  }
  if (include_rows_in_representation_) {
    for (size_t i = 0; i < view.row_count(); i++) {
      *s << StringPrintf("%4" PRIuS ":  ", i);
      AppendRowToStream(view, i, s);
      *s << std::endl;
    }
  }
}

void ViewPrinter::AppendResultViewToStream(const ResultView& result_view,
                                           std::ostream* s) const {
  if (result_view.has_data()) {
    *s << "\n";
    AppendViewToStream(result_view.view(), s);
  } else if (result_view.is_failure()) {
    *s << " Failed: " << result_view.exception().message() <<"\n";
  } else if (result_view.is_bos()) {
    *s << " Before input\n";
  } else if (result_view.is_eos()) {
    *s << " End of stream\n";
  } else if (result_view.is_waiting_on_barrier()) {
    *s << " Waiting on barrier\n";
  } else {
    LOG(FATAL) << "Cannot interpret the status of result_view.";
  }
}


void ViewPrinter::AppendRowToStream(const View& view,
                                    size_t row_id,
                                    std::ostream* s) const {
  for (size_t i = 0; i < view.schema().attribute_count(); ) {
    const Column& column = view.column(i);
    if (column.is_null() != NULL && column.is_null()[row_id]) {
      *s << Expand("NULL");
    } else {
      VariantConstPointer value = column.data_plus_offset(row_id);
      string result;
      if (column.type_info().type() == ENUM) {
        *s << SucceedOrDie(column.attribute().enum_definition().
                  NumberToName(*value.as<ENUM>()));
      } else {
        GetDefaultPrinterFn(column.type_info().type())(value, &result);
      }
#ifndef NDEBUG
      if (column.type_info().type() == BOOL) {
        const uint8 int_value =
            *reinterpret_cast<const uint8*>(value.as<BOOL>());
        // Numeric value of a bool should be 0 or 1. Report it if it isn't.
        if (int_value > 1) {
          char buffer[kFastToBufferSize];
          result.append(" (");
          result.append(FastUInt32ToBuffer(int_value, buffer));
          result.append(")");
        }
      }
#endif
      *s << Expand(result);
    }
    if (++i < view.schema().attribute_count())
      *s << ", ";
  }
}

const string ViewPrinter::Expand(const string& value) const {
  if (value.size() >= min_column_length_) {
    return value;
  } else {
    string padding(min_column_length_ - value.size(), ' ');
    return StrCat(padding, value);
  }
}

}  // namespace supersonic
