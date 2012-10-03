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

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"

#include "supersonic/cursor/core/specification_builder.h"

#include "supersonic/proto/specification.pb.h"

namespace supersonic {

ExtendedSortSpecificationBuilder* ExtendedSortSpecificationBuilder::Add(
    const string& attribute_name,
    const ColumnOrder& column_order,
    bool is_case_sensitive) {
  ExtendedSortSpecification::Key* new_key = specification_->add_keys();
  new_key->set_attribute_name(attribute_name);
  new_key->set_column_order(ColumnOrder(column_order));
  new_key->set_case_sensitive(is_case_sensitive);
  return this;
}

ExtendedSortSpecificationBuilder* ExtendedSortSpecificationBuilder::SetLimit(
    size_t limit) {
  CHECK(!specification_->has_limit());
  specification_->set_limit(limit);
  return this;
}

ExtendedSortSpecification* ExtendedSortSpecificationBuilder::Build() {
  CHECK(specification_ != NULL) << "You can build the specification only once.";
  if (specification_->keys_size() == 0) {
    LOG(WARNING) << "Creating an ExtendedSortSpecificationBuilder without"
                 << "any keys";
  }
  return specification_.release();
}

}  // namespace supersonic

