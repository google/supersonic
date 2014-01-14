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

#include "supersonic/expression/core/projecting_bound_expressions.h"

#include <stdint.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <utility>
#include "supersonic/utils/std_namespace.h"
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/vector/vector_logic.h"
#include "supersonic/utils/iterator_adaptors.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/mathlimits.h"

namespace supersonic {

namespace {

// Projection on the input directly.
class BoundInputProjectionExpression : public BoundExpression {
 public:
  explicit BoundInputProjectionExpression(
      const BoundSingleSourceProjector* projector)
      : BoundExpression(projector->result_schema()),
        projector_(projector) {}

  virtual ~BoundInputProjectionExpression() {}

  virtual rowcount_t row_capacity() const {
    return MathLimits<rowcount_t>::kMax;
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vector) {
    CHECK_EQ(projector_->result_schema().attribute_count(),
             skip_vector.column_count());
    projector_->Project(input, my_view());
    my_view()->set_row_count(input.row_count());
    for (int i = 0; i < skip_vector.column_count(); ++i) {
      if (my_view()->schema().attribute(i).is_nullable()) {
        // Copy the input nulls onto the skip_vectors.
        vector_logic::Or(skip_vector.column(i), my_view()->column(i).is_null(),
                         input.row_count(), skip_vector.column(i));
        my_view()->mutable_column(i)->ResetIsNull(skip_vector.column(i));
      }
    }
    return Success(*my_view());
  }

  bool is_constant() const { return false; }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    const TupleSchema& input_schema = projector_->source_schema();
    for (int i = 0; i < input_schema.attribute_count(); ++i) {
      if (projector_->IsAttributeProjected(i)) {
        referred_attribute_names->insert(input_schema.attribute(i).name());
      }
    }
  }

 private:
  scoped_ptr<const BoundSingleSourceProjector> projector_;
  DISALLOW_COPY_AND_ASSIGN(BoundInputProjectionExpression);
};

// Projection on sub-expressions.
class BoundProjectionExpression : public BoundExpression {
 public:
  explicit BoundProjectionExpression(const BoundMultiSourceProjector* projector,
                                     BoundExpressionList* arguments)
      : BoundExpression(projector->result_schema()),
        projector_(projector),
        arguments_(arguments),
        // TODO(onufry): pass the allocator here and remove the reference to the
        // HeapBufferAllocator.
        // For each input we need to pass a set of skip vectors. For each
        // attribute that gets projected to more than one output column we need
        // to allocate an additional skip vector (because we cannot use the
        // one received from the caller). Thus, we need to allocate a space for
        // as many skip vectors as any single input expression has columns that
        // get multiprojected.
        // We also allocate additional storage (1 vector) for a skip vector
        // prefilled with true values (meaning skip all), which will be passed
        // along to any columns that do not get projected onto the output.
        local_skip_vector_storage_(MaxNumberOfMultiProjectedColumns(
            arguments, projector) + 1, HeapBufferAllocator::Get()) {
          local_skip_vector_list_.resize(arguments->size());
          for (int i = 0; i < arguments->size(); ++i) {
            // For each input expression we allocate a BoolView that will hold
            // its skip vectors.
            local_skip_vector_list_[i].reset(new BoolView(
                arguments->get(i)->result_schema().attribute_count()));
          }
        }

  virtual ~BoundProjectionExpression() {}

  virtual rowcount_t row_capacity() const {
    return MathLimits<rowcount_t>::kMax;
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    // We did not reallocate memory at binding time, so we have to do it at
    // evaluation time. Hopefully still only once (this is a sort of a delayed
    // binding).
    // TODO(onufry): Nonetheless this should go to binding time, when an
    // allocator and the desired row capacity are made available.
    if (input.row_count() > local_skip_vector_storage_.row_capacity()) {
      // 3/2 is an arbitrary constant, we want to allocate somewhat more memory
      // to avoid future reallocations.
      PROPAGATE_ON_FAILURE(local_skip_vector_storage_.TryReallocate(
          (input.row_count() * 3) / 2));
      // Pre-fill the first vector of the block with TRUE. This will be used by
      // all the attributes which are not projected onto the output.
      bit_pointer::FillWithTrue(local_skip_vector_storage_.view().column(0),
                                local_skip_vector_storage_.row_capacity());
    }
    argument_results_.clear();
    for (size_t argument = 0; argument < arguments_->size(); ++argument) {
      local_skip_vector_list_[argument]->set_row_count(input.row_count());
      size_t column_count =
          arguments_->get(argument)->result_schema().attribute_count();
      // The index of a free skip vector in the BoolBlock, that can be used for
      // passing over to multi-projected columns.
      size_t current_skip_vector_index = 1;
      for (size_t column = 0; column < column_count; ++column) {
        size_t number_of_projections =
            projector_->NumberOfProjectionsForAttribute(argument, column);
        if (number_of_projections == 0) {
          // The column is not projected at all, we will use the skip vector
          // prefilled with true values, which is the first vector in the
          // storage.
          local_skip_vector_list_[argument]->ResetColumn(
              column, local_skip_vector_storage_.view().column(0));
          continue;
        }
        pair<PositionIterator, PositionIterator> projected_positions =
            projector_->ProjectedAttributePositions(argument, column);
        // If there is only a single projections (the most typical case), we
        // will simply use the skip vector that was given to us on the input.
        if (number_of_projections == 1) {
          local_skip_vector_list_[argument]->ResetColumn(
              column, skip_vectors.column(*projected_positions.first));
          continue;
        }
        // Now the unhappy case - we project to more than one output column. We
        // need to AND all the skip_vectors (we can skip only these rows which
        // are not required by _any_ of the projections). We will also need to
        // OR the resulting skip_vector onto the targets.
        // We take the first free skip vector for ourselves, and mark it as
        // used.
        DCHECK(current_skip_vector_index
               < local_skip_vector_storage_.column_count());
        bool_ptr current_skip_vector = local_skip_vector_storage_.view()
            .column(current_skip_vector_index++);
        // Take the first projections skip vector, copy it, and then AND all the
        // other skip vectors.
        bit_pointer::FillFrom(current_skip_vector,
                              skip_vectors.column(*projected_positions.first),
                              input.row_count());
        PositionIterator it = projected_positions.first;
        for (++it; it != projected_positions.second; ++it) {
          vector_logic::And(current_skip_vector,
                            skip_vectors.column(*it),
                            input.row_count(),
                            current_skip_vector);
        }
        local_skip_vector_list_[argument]->ResetColumn(
            column, current_skip_vector);
      }
      // Children evaluation.
      EvaluationResult result = arguments_->get(argument)->
          DoEvaluate(input, *local_skip_vector_list_[argument].get());
      PROPAGATE_ON_FAILURE(result);
      argument_results_.push_back(&result.get());
      // Now for any expressions that were projected to multiple columns we need
      // to copy the nulls obtained during the calculation onto the original
      // skip_vector.
      current_skip_vector_index = 1;
      for (size_t column = 0; column < column_count; ++column) {
        if (projector_->NumberOfProjectionsForAttribute(argument, column) > 1) {
          bool_ptr current_skip_vector = local_skip_vector_storage_.view()
              .column(current_skip_vector_index++);
          pair<PositionIterator, PositionIterator> projected_positions =
              projector_->ProjectedAttributePositions(argument, column);
          PositionIterator it = projected_positions.first;
          for (++it; it != projected_positions.second; ++it) {
            vector_logic::Or(current_skip_vector,
                             skip_vectors.column(*projected_positions.first),
                             input.row_count(),
                             skip_vectors.column(*projected_positions.first));
            ++projected_positions.first;
          }
        }
      }
    }
    projector_->Project(argument_results_.begin(), argument_results_.end(),
                        my_view());
    my_view()->set_row_count(input.row_count());
    for (int i = 0; i < my_view()->schema().attribute_count(); ++i) {
      my_view()->mutable_column(i)->ResetIsNull(skip_vectors.column(i));
    }
    return Success(*my_view());
  }

  bool is_constant() const { return false; }

  // Ignores whether the input is projected or not (because the input attribute
  // is still required to exists in the input schema).
  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    arguments_->CollectReferredAttributeNames(referred_attribute_names);
  }

 private:
  // Does not take ownership of anything.
  // Calculates the max over all input expressions of the number of columns of
  // the expression that get projected to more than one column of the output.
  static size_t MaxNumberOfMultiProjectedColumns(
      const BoundExpressionList* arguments,
      const BoundMultiSourceProjector* projector) {
    size_t result = 0;
    for (size_t argument = 0; argument < arguments->size(); ++argument) {
      size_t current_result = 0;
      for (size_t column = 0;
           column < arguments->get(argument)->result_schema().attribute_count();
           ++column) {
        if (projector->NumberOfProjectionsForAttribute(argument, column) > 1) {
          current_result += 1;
        }
      }
      result = std::max(result, current_result);
    }
    return result;
  }

  scoped_ptr<const BoundMultiSourceProjector> projector_;
  scoped_ptr<BoundExpressionList> arguments_;
  vector<const View*> argument_results_;
  util::gtl::PointerVector<BoolView> local_skip_vector_list_;
  BoolBlock local_skip_vector_storage_;
  DISALLOW_COPY_AND_ASSIGN(BoundProjectionExpression);
};

}  // namespace

// --------------------------- Input projections -------------------------------

FailureOrOwned<BoundExpression> BoundAttributeAt(const TupleSchema& schema,
                                                 size_t position) {
  scoped_ptr<const SingleSourceProjector> projector(
      ProjectAttributeAt(position));
  return BoundInputAttributeProjection(schema, *(projector.get()));
}

FailureOrOwned<BoundExpression> BoundNamedAttribute(const TupleSchema& schema,
                                                    const string& name) {
  scoped_ptr<const SingleSourceProjector> projector(
      ProjectNamedAttribute(name));
  return BoundInputAttributeProjection(schema, *(projector.get()));
}

FailureOrOwned<BoundExpression> BoundInputAttributeProjection(
    const TupleSchema& schema,
    const SingleSourceProjector& projector) {
  FailureOrOwned<const BoundSingleSourceProjector> result =
      projector.Bind(schema);
  PROPAGATE_ON_FAILURE(result);
  return Success(new BoundInputProjectionExpression(result.release()));
}

// ---------------------------- Expression projections -------------------------

FailureOrOwned<BoundExpression> BoundProjection(
    const BoundMultiSourceProjector* projector,
    BoundExpressionList* arguments) {
  return Success(new BoundProjectionExpression(projector, arguments));
}

FailureOrOwned<BoundExpression> BoundRenameCompoundExpression(
    const vector<string>& names,
    BoundExpressionList* expressions) {
  scoped_ptr<BoundExpressionList> expressions_ptr(expressions);

  vector<const TupleSchema*> schemas;
  for (int i = 0; i < expressions_ptr->size(); ++i) {
    schemas.push_back(&expressions_ptr->get(i)->result_schema());
  }
  scoped_ptr<BoundMultiSourceProjector> projector(
      new BoundMultiSourceProjector(schemas));
  int name_pos = 0;
  for (int i = 0; i < schemas.size(); ++i) {
    for (int j = 0; j < schemas[i]->attribute_count(); j++) {
      projector->AddAs(i, j, names[name_pos++]);
    }
  }
  return BoundProjection(projector.release(), expressions_ptr.release());
}

FailureOrOwned<BoundExpression> BoundCompoundExpression(
    BoundExpressionList* expressions) {
  scoped_ptr<BoundExpressionList> expressions_ptr(expressions);

  vector<const TupleSchema*> schemas;
  for (int i = 0; i < expressions_ptr->size(); ++i) {
    schemas.push_back(&expressions_ptr->get(i)->result_schema());
  }
  scoped_ptr<BoundMultiSourceProjector> projector(
      new BoundMultiSourceProjector(schemas));
  for (int i = 0; i < schemas.size(); ++i) {
    for (int j = 0; j < schemas[i]->attribute_count(); j++) {
      projector->Add(i, j);
    }
  }
  return BoundProjection(projector.release(), expressions_ptr.release());
}

FailureOrOwned<BoundExpression> BoundAlias(const string& new_name,
                                           BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  PROPAGATE_ON_FAILURE(CheckAttributeCount("ALIAS", argument->result_schema(),
                                           /* expected argument count = */ 1));
  return BoundRenameCompoundExpression(
      vector<string>(1, new_name),
      (new BoundExpressionList())->add(argument));
}


}  // namespace supersonic
