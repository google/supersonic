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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#ifndef SUPERSONIC_PUBLIC_SUPERSONIC_H_
#define SUPERSONIC_PUBLIC_SUPERSONIC_H_

#include "supersonic/base/exception/exception.h"  // IWYU pragma: keep
#include "supersonic/base/exception/result.h"  // IWYU pragma: keep
#include "supersonic/base/infrastructure/block.h"  // IWYU pragma: keep
#include "supersonic/base/infrastructure/init.h"  // IWYU pragma: keep

#include "supersonic/base/infrastructure/projector.h"  // IWYU pragma: keep
#include "supersonic/base/infrastructure/tuple_schema.h"  // IWYU pragma: keep
#include "supersonic/base/infrastructure/types.h"  // IWYU pragma: keep
#include "supersonic/base/infrastructure/types_infrastructure.h"  // IWYU pragma: keep
#include "supersonic/base/memory/arena.h"  // IWYU pragma: keep
#include "supersonic/base/memory/memory.h"  // IWYU pragma: keep
#include "supersonic/cursor/base/cursor.h"  // IWYU pragma: keep
#include "supersonic/cursor/base/lookup_index.h"  // IWYU pragma: keep
#include "supersonic/cursor/base/operation.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/aggregate.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/coalesce.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/compute.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/filter.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/generate.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/hash_join.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/limit.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/ownership_taker.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/project.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/sort.h"  // IWYU pragma: keep
#include "supersonic/cursor/core/scan_view.h"  // IWYU pragma: keep
// TODO(tkaftal): Add support for union cursor.
#include "supersonic/cursor/infrastructure/basic_cursor.h"  // IWYU pragma: keep
#include "supersonic/cursor/infrastructure/basic_operation.h"  // IWYU pragma: keep
#include "supersonic/cursor/infrastructure/ordering.h"  // IWYU pragma: keep
#include "supersonic/cursor/infrastructure/table.h"  // IWYU pragma: keep
#include "supersonic/cursor/infrastructure/value_ref.h"  // IWYU pragma: keep
#include "supersonic/cursor/infrastructure/writer.h"  // IWYU pragma: keep
#include "supersonic/expression/base/expression.h"  // IWYU pragma: keep
#include "supersonic/expression/core/arithmetic_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/arithmetic_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/comparison_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/comparison_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/date_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/date_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/elementary_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/elementary_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/math_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/math_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/projecting_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/projecting_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/string_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/core/string_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/ext/hashing/hashing_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/terminal_expressions.h"  // IWYU pragma: keep
#include "supersonic/serialization/build_expression_from_proto.h"  // IWYU pragma: keep

#endif  // SUPERSONIC_PUBLIC_SUPERSONIC_H_
