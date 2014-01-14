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

// Serialization of view in readable form to given output stream.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_PRINTER_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_PRINTER_H_

#include <stddef.h>

#include <iosfwd>
#include <ostream>
#include <string>
namespace supersonic {using std::string; }

namespace supersonic {

class ResultView;
class TupleSchema;
class View;

class ViewPrinter {
 public:
  ViewPrinter()
      : include_header_in_representation_(true),
        include_rows_in_representation_(true),
        min_column_length_(0) {}

  ViewPrinter(bool include_header_in_representation,
              bool include_rows_in_representation)
      : include_header_in_representation_(include_header_in_representation),
        include_rows_in_representation_(include_rows_in_representation),
        min_column_length_(0) {}

  ViewPrinter(bool include_header_in_representation,
              bool include_rows_in_representation,
              int min_column_length)
      : include_header_in_representation_(include_header_in_representation),
        include_rows_in_representation_(include_rows_in_representation),
        min_column_length_(min_column_length) {}

  void AppendSchemaToStream(const TupleSchema& typle_schema,
                            std::ostream* s) const;

  void AppendViewToStream(const View& view, std::ostream* s) const;
  void AppendResultViewToStream(const ResultView& view, std::ostream* s) const;

  void AppendRowToStream(const View& view,
                         size_t row_id,
                         std::ostream* s) const;

  struct StreamResultViewAdapter {
    StreamResultViewAdapter(const ViewPrinter &vp,
                            const ResultView &rv)
        : vp(vp), rv(rv) { }

    friend std::ostream& operator<<(
        std::ostream& os, const ViewPrinter::StreamResultViewAdapter &srva) {
      srva.vp.AppendResultViewToStream(srva.rv, &os);
      return os;
    }

    const ViewPrinter &vp;
    const ResultView &rv;
  };

 private:
  const string Expand(const string& value) const;

  const bool include_header_in_representation_;
  const bool include_rows_in_representation_;
  const int min_column_length_;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_VIEW_PRINTER_H_
