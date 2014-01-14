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

#include "supersonic/cursor/infrastructure/ordering.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/projector.h"

namespace supersonic {

FailureOrOwned<const BoundSortOrder> SortOrder::Bind(
    const TupleSchema& source_schema) const {
  std::unique_ptr<BoundSingleSourceProjector> projector(
      new BoundSingleSourceProjector(source_schema));
  vector<ColumnOrder> column_order;
  for (size_t i = 0; i < projectors_.size(); ++i) {
    FailureOrOwned<const BoundSingleSourceProjector> component =
        projectors_[i]->Bind(source_schema);
    PROPAGATE_ON_FAILURE(component);
    for (size_t j = 0; j < component->result_schema().attribute_count();
         ++j) {
      if (!projector->AddAs(
              component->source_attribute_position(j),
              component->result_schema().attribute(j).name())) {
        THROW(new Exception(
            ERROR_ATTRIBUTE_EXISTS,
            StringPrintf(
                "Duplicate attribute name \"%s\" in result schema (%s)",
                component->result_schema().attribute(j).name().c_str(),
                component->result_schema().
                    GetHumanReadableSpecification().c_str())));
      }
      column_order.push_back(column_order_[i]);
    }
  }
  return Success(new BoundSortOrder(projector.release(), column_order));
}

}  // namespace supersonic
