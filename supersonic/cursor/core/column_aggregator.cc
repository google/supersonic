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

#include "supersonic/cursor/core/column_aggregator.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <ext/hash_set>
using __gnu_cxx::hash;
using __gnu_cxx::hash_set;
using __gnu_cxx::hash_multiset;
#include <map>
using std::map;
using std::multimap;
#include <string>
using std::string;
#include <vector>
using std::vector;

#include "supersonic/utils/casts.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/aggregation_operators.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/operators.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/hash/hash.h"

namespace supersonic {
namespace aggregations {

using util::gtl::PointerVector;

ColumnAggregator::ColumnAggregator() {}
ColumnAggregator::~ColumnAggregator() {}

// Internal interface for updating and reseting result of an aggregation.
// Extends ColumnAggregator interface with a method needed by
// DistinctAggregator.
class ColumnAggregatorInternal : public ColumnAggregator {
 public:
  ColumnAggregatorInternal() {}
  virtual ~ColumnAggregatorInternal() {}

  // Like UpdateAggregation, but takes into account only input values which
  // indexes are in selected_inputs_indexes.
  virtual FailureOrVoid UpdateAggregationForSelectedInputs(
      const Column* input,
      rowcount_t input_row_count,
      const rowid_t result_index_map[],
      const vector<rowid_t>& selected_inputs_indexes) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnAggregatorInternal);
};

// Generic column aggregator supporting all operators except COUNT.
template<Aggregation Aggregation, DataType InputType, DataType OutputType>
class ColumnAggregatorImpl : public ColumnAggregatorInternal {
 public:
  typedef typename TypeTraits<InputType>::cpp_type cpp_input_type;
  typedef typename TypeTraits<OutputType>::cpp_type cpp_output_type;

  ColumnAggregatorImpl(Block* result_block, int result_column_index)
      : allocated_buffers_(),
        assignment_operator_(result_block->allocator()),
        aggregation_operator_(result_block->allocator()),
        result_block_(result_block),
        result_column_index_(result_column_index),
        result_data_(NULL),
        result_is_null_(NULL) {
    Rebind(0, result_block->row_capacity());
  }

  virtual ~ColumnAggregatorImpl() {}

  virtual void Rebind(rowcount_t previous_capacity, rowcount_t new_capacity) {
    result_data_ = result_block_->mutable_column(result_column_index_)->
        template mutable_typed_data<OutputType>();
    result_is_null_ = result_block_->mutable_column(result_column_index_)->
        mutable_is_null();
    CHECK_NOTNULL(result_is_null_);
    allocated_buffers_.resize(new_capacity);
    if (new_capacity > previous_capacity) {
      Clear(previous_capacity, new_capacity - previous_capacity);
    }
  }

  virtual FailureOrVoid UpdateAggregation(const Column* input,
                                          rowcount_t input_row_count,
                                          const rowid_t result_index_map[]) {
    bool input_always_not_null = (input->is_null() == NULL);
    for (rowid_t i = 0; i < input_row_count; ++i) {
      if (input_always_not_null || !input->is_null()[i]) {
        if (!UpdateAggregatedValue(
                input->data().as<InputType>()[i],
                result_index_map[i])) {
          THROW(new Exception(
              ERROR_MEMORY_EXCEEDED,
              "Aggregator memory exceeded. Not enough memory to aggregate "
              "variable-length type elements."));
        }
      }
    }
    return Success();
  }

  virtual void Reset() {
    if (TypeTraits<OutputType>::is_variable_length) {
      FreeAllocatedBuffers();
    }
    Clear(0, result_block_->row_capacity());
  }

  virtual FailureOrVoid UpdateAggregationForSelectedInputs(
      const Column* input,
      rowcount_t input_row_count,
      const rowid_t result_index_map[],
      const vector<rowid_t>& selected_inputs_indexes) {
    for (vector<rowid_t>::const_iterator it = selected_inputs_indexes.begin();
         it != selected_inputs_indexes.end(); ++it) {
      if (!UpdateAggregatedValue(
              input->data().as<InputType>()[*it],
              result_index_map[*it])) {
        THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            "Aggregator memory exceeded. Not enough memory to aggregate "
            "variable-length type elements."));
      }
    }
    return Success();
  }

 private:
  bool UpdateAggregatedValue(const cpp_input_type& input_data,
                             rowid_t result_index) {
    if (result_is_null_[result_index]) {
      result_is_null_[result_index] = false;
      return assignment_operator_(
          input_data, &result_data_[result_index],
          &allocated_buffers_[result_index]);
    } else {
      return aggregation_operator_(
          input_data, &result_data_[result_index],
          &allocated_buffers_[result_index]);
    }
  }

  // Clears a region of the aggregation result, specified by the offset and
  // length. Called (1) by Rebind, after reallocating the underlying buffer to
  // initialize the newly created entries, and (2) by Reset, when resetting the
  // aggregator to a pristine state.
  void Clear(rowcount_t offset, rowcount_t length) {
    bit_pointer::FillWithTrue(result_is_null_ + offset, length);
  }

  void FreeAllocatedBuffers() {
    // Free all buffers but do not change the size of allocated buffers vector.
    for (PointerVector<Buffer>::iterator it = allocated_buffers_.begin();
         it != allocated_buffers_.end(); ++it) {
      it->reset();
    }
  }

  PointerVector<Buffer> allocated_buffers_;
  AssignmentOperator<InputType, OutputType> assignment_operator_;
  AggregationOperator<Aggregation, InputType,
                      OutputType>  aggregation_operator_;
  Block* const result_block_;
  const int result_column_index_;
  cpp_output_type* result_data_;
  bool_ptr result_is_null_;

  DISALLOW_COPY_AND_ASSIGN(ColumnAggregatorImpl);
};

// Column aggregator for COUNT.
template<DataType OutputType>
class CountColumnAggregatorImpl : public ColumnAggregatorInternal {
 public:
  COMPILE_ASSERT(TypeTraits<OutputType>::is_integer, output_type_not_integer);
  typedef typename TypeTraits<OutputType>::cpp_type cpp_output_type;

  CountColumnAggregatorImpl(Block* result_block,
                            int result_column_index) :
      result_column_(result_block->mutable_column(result_column_index)),
      result_data_(NULL),
      row_capacity_(result_block->row_capacity()) {
    Rebind(0, row_capacity_);
  }

  virtual ~CountColumnAggregatorImpl() {}

  virtual FailureOrVoid UpdateAggregation(const Column* input,
                                          rowcount_t input_row_count,
                                          const rowid_t result_index_map[]) {
    bool check_input_nullability = (input != NULL && input->is_null() != NULL);
    for (rowid_t i = 0; i < input_row_count; ++i) {
      // Do not count NULL values.
      if (check_input_nullability && input->is_null()[i]) {
        continue;
      }
      rowid_t result_index = result_index_map[i];
      result_data_[result_index] += 1;
    }
    return Success();
  }

  virtual void Rebind(rowcount_t previous_capacity, rowcount_t new_capacity) {
    result_data_ = result_column_->template mutable_typed_data<OutputType>();
    row_capacity_ = new_capacity;
    if (new_capacity > previous_capacity) {
      Clear(previous_capacity, new_capacity - previous_capacity);
    }
  }

  virtual void Reset() {
    Clear(0, row_capacity_);
  }

  virtual FailureOrVoid UpdateAggregationForSelectedInputs(
      const Column* input,
      rowcount_t input_row_count,
      const rowid_t result_index_map[],
      const vector<rowid_t>& selected_inputs_indexes) {
    for (vector<rowid_t>::const_iterator it = selected_inputs_indexes.begin();
         it != selected_inputs_indexes.end(); ++it) {
      size_t result_index = result_index_map[*it];
      result_data_[result_index] += 1;
    }
    return Success();
  }

 private:
  void Clear(rowcount_t offset, rowcount_t length) {
    memset(&result_data_[offset], 0, sizeof(cpp_output_type) * length);
  }

  OwnedColumn* result_column_;
  cpp_output_type* result_data_;
  rowcount_t row_capacity_;

  DISALLOW_COPY_AND_ASSIGN(CountColumnAggregatorImpl);
};


// Wrapper around stl hash set to allow to store data from StringPieces.
// TODO(user): Google's more efficient implementation of hash set optimized for
// usage without remove() could be used here to improve performance. Also when
// allocator with soft quota is available it should be used here to allow to
// report OOM errors.
template<typename T>
struct HashSet {
  void insert(const T& value) {
    distinct_values_.insert(value);
  }

  bool has_value(const T& value) {
    return (distinct_values_.find(value) != distinct_values_.end());
  }

  void Reset() {
    distinct_values_.clear();
  }

 private:
  hash_set<T, operators::Hash> distinct_values_;
};

template<>
struct HashSet<StringPiece> {
  void insert(const StringPiece& value) {
    distinct_values_.insert(value.as_string());
  }

  bool has_value(const StringPiece& value) {
    return (distinct_values_.find(value.as_string()) != distinct_values_.end());
  }

  void Reset() {
    distinct_values_.clear();
  }

 private:
  hash_set<string> distinct_values_;
};

template<DataType InputType>
class DistinctAggregator : public ColumnAggregator {
 public:
  typedef typename TypeTraits<InputType>::cpp_type cpp_input_type;

  DistinctAggregator(ColumnAggregatorInternal* aggregator, Block* result_block)
      : aggregator_(aggregator) {}

  virtual ~DistinctAggregator() {}

  virtual FailureOrVoid UpdateAggregation(const Column* input,
                                          rowcount_t input_row_count,
                                          const rowid_t result_index_map[]) {
    vector<rowid_t> selected_inputs_indexes;
    CHECK_NOTNULL(input);
    bool check_input_nullability = input->is_null() != NULL;
    for (rowid_t i = 0; i < input_row_count; ++i) {
      // NULL values do not count as distinct.
      if (check_input_nullability && input->is_null()[i]) {
        continue;
      }
      cpp_input_type input_val = input->data().as<InputType>()[i];
      rowid_t result_index = result_index_map[i];

      if (result_index >= distinct_values_.size()) {
        distinct_values_.resize(result_index + 1);
      }

      HashSet<cpp_input_type>* distinct_values_set =
          &distinct_values_[result_index];
      if (!distinct_values_set->has_value(input_val)) {
        distinct_values_set->insert(input_val);
        selected_inputs_indexes.push_back(i);
      }
    }
    if (selected_inputs_indexes.size()) {
      aggregator_->UpdateAggregationForSelectedInputs(
          input, input_row_count, result_index_map, selected_inputs_indexes);
    }
    return Success();
  }

  virtual void Rebind(rowcount_t previous_capacity, rowcount_t new_capacity) {
    aggregator_->Rebind(previous_capacity, new_capacity);
  }

  virtual void Reset() {
    for (rowid_t i = 0; i < distinct_values_.size(); ++i) {
      distinct_values_[i].Reset();
    }
    aggregator_->Reset();
  }

 private:
  scoped_ptr<ColumnAggregatorInternal> aggregator_;

  // Vector of sets holding distinct values for each unique key. Not initialized
  // to have as many elements as result_block to avoid possibly unnecessary
  // allocations of hash sets. New hash sets are added only when they are
  // needed.
  vector<HashSet<cpp_input_type> > distinct_values_;
  DISALLOW_COPY_AND_ASSIGN(DistinctAggregator);
};


// Hides implementation details for ColumnAggregatorFactory from the user.
class ColumnAggregatorFactoryImpl {
 public:
  ColumnAggregatorFactoryImpl();
  ~ColumnAggregatorFactoryImpl();


  FailureOrOwned<ColumnAggregator> CreateAggregator(
      Aggregation aggregation_operator,
      DataType input_type,
      Block* result_block,
      int result_column_index);

  FailureOrOwned<ColumnAggregator> CreateDistinctAggregator(
      Aggregation aggregation_operator,
      DataType input_type,
      Block* result_block,
      int result_column_index);

  FailureOrOwned<ColumnAggregator> CreateCountAggregator(
      Block* result_block,
      int result_column_index);

  FailureOrOwned<ColumnAggregator> CreateDistinctCountAggregator(
      DataType input_type,
      Block* result_block,
      int result_column_index);


 private:
  typedef ColumnAggregator* (*AggregatorCreatorFunction)(
      Block* result_block,
      int result_column_index);

  typedef ColumnAggregator* (*CountAggregatorCreatorFunction)(
      Block* result_block,
      int result_column_index);

  typedef ColumnAggregator* (*DistinctAggregatorCreatorFunction)(
      ColumnAggregatorInternal* aggregator,
      Block* result_block);

  bool IsAggregationSupported(Aggregation aggregation_operator, DataType t1,
                              DataType t2);

  // Maps aggregation operator, input type and output type to factory method
  // that creates ColumnAggregator objects thath handles such aggregation.
  map<Aggregation, map<DataType, map<DataType, AggregatorCreatorFunction> > >
      aggregator_factory_;

  // Maps output type to factory method that creates ColumnAggregator objects
  // that handles COUNT aggregation and store result to table of output type.
  map<DataType, CountAggregatorCreatorFunction>  count_aggregator_factory_;

  map<DataType, DistinctAggregatorCreatorFunction> distinct_aggregator_factory_;

  DISALLOW_COPY_AND_ASSIGN(ColumnAggregatorFactoryImpl);
};

// Factory methods to create ColumnAggregators.
template<Aggregation aggregation, DataType input_type, DataType output_type>
ColumnAggregator* AggregatorCreator(Block* result_block,
                                    int result_column_index) {
  return new ColumnAggregatorImpl<aggregation, input_type, output_type>(
      result_block, result_column_index);
}

template<DataType output_type>
ColumnAggregator* CountAggregatorCreator(Block* result_block,
                                         int result_column_index) {
  return new CountColumnAggregatorImpl<output_type>(
      result_block, result_column_index);
}

template<DataType input_type>
ColumnAggregator* DistinctAggregatorCreator(
    ColumnAggregatorInternal* aggregator, Block* output_block) {
  return new DistinctAggregator<input_type>(aggregator, output_block);
}

// Helper macros to create factory method for each supported aggregation
#define NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, t1)         \
  do {                                                                  \
    factory[agg][t1][INT32] = AggregatorCreator<agg, t1, INT32>;        \
    factory[agg][t1][INT64] = AggregatorCreator<agg, t1, INT64>;        \
    factory[agg][t1][UINT32] = AggregatorCreator<agg, t1, UINT32>;      \
    factory[agg][t1][UINT64] = AggregatorCreator<agg, t1, UINT64>;      \
    factory[agg][t1][FLOAT] = AggregatorCreator<agg, t1, FLOAT>;        \
    factory[agg][t1][DOUBLE] = AggregatorCreator<agg, t1, DOUBLE>;      \
  } while (0)

#define NUMERIC_TYPE_FACTORY_INIT(factory, agg)                         \
  do {                                                                  \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, INT32);         \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, INT64);         \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, UINT32);        \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, UINT64);        \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, FLOAT);         \
    NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE(factory, agg, DOUBLE);        \
  } while (0)

#define NON_NUMERIC_TYPE_FACTORY_INIT(factory, agg)                     \
  do {                                                                  \
    factory[agg][BOOL][BOOL] = AggregatorCreator<agg, BOOL, BOOL>;      \
    factory[agg][DATE][DATE] = AggregatorCreator<agg, DATE, DATE>;      \
    factory[agg][DATETIME][DATETIME] =                                  \
        AggregatorCreator<agg, DATETIME, DATETIME> ;                    \
    factory[agg][STRING][STRING] =                                      \
        AggregatorCreator<agg, STRING, STRING>;                         \
  } while (0)


ColumnAggregatorFactoryImpl::ColumnAggregatorFactoryImpl() {
  NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, SUM);
  NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, MIN);
  NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, MAX);
  NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, FIRST);
  NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, LAST);

  NON_NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, MIN);
  NON_NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, MAX);
  NON_NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, FIRST);
  NON_NUMERIC_TYPE_FACTORY_INIT(aggregator_factory_, LAST);

  aggregator_factory_[CONCAT][INT32][STRING] =
      AggregatorCreator<CONCAT, INT32, STRING>;
  aggregator_factory_[CONCAT][INT64][STRING] =
      AggregatorCreator<CONCAT, INT64, STRING>;
  aggregator_factory_[CONCAT][UINT32][STRING] =
      AggregatorCreator<CONCAT, UINT32, STRING>;
  aggregator_factory_[CONCAT][UINT64][STRING] =
      AggregatorCreator<CONCAT, UINT64, STRING>;
  aggregator_factory_[CONCAT][FLOAT][STRING] =
      AggregatorCreator<CONCAT, FLOAT, STRING>;
  aggregator_factory_[CONCAT][DOUBLE][STRING] =
      AggregatorCreator<CONCAT, DOUBLE, STRING>;
  aggregator_factory_[CONCAT][BOOL][STRING] =
      AggregatorCreator<CONCAT, BOOL, STRING>;
  aggregator_factory_[CONCAT][DATE][STRING] =
      AggregatorCreator<CONCAT, DATE, STRING>;
  aggregator_factory_[CONCAT][DATETIME][STRING] =
      AggregatorCreator<CONCAT, DATETIME, STRING>;
  aggregator_factory_[CONCAT][STRING][STRING] =
      AggregatorCreator<CONCAT, STRING, STRING>;

  count_aggregator_factory_[INT32] = CountAggregatorCreator<INT32>;
  count_aggregator_factory_[INT64] = CountAggregatorCreator<INT64>;
  count_aggregator_factory_[UINT32] = CountAggregatorCreator<UINT32>;
  count_aggregator_factory_[UINT64] = CountAggregatorCreator<UINT64>;

  distinct_aggregator_factory_[INT32] = DistinctAggregatorCreator<INT32>;
  distinct_aggregator_factory_[INT64] = DistinctAggregatorCreator<INT64>;
  distinct_aggregator_factory_[UINT32] = DistinctAggregatorCreator<UINT32>;
  distinct_aggregator_factory_[UINT64] = DistinctAggregatorCreator<UINT64>;
  distinct_aggregator_factory_[FLOAT] = DistinctAggregatorCreator<FLOAT>;
  distinct_aggregator_factory_[DOUBLE] = DistinctAggregatorCreator<DOUBLE>;
  distinct_aggregator_factory_[BOOL] = DistinctAggregatorCreator<BOOL>;
  distinct_aggregator_factory_[DATE] = DistinctAggregatorCreator<DATE>;
  distinct_aggregator_factory_[DATETIME] = DistinctAggregatorCreator<DATETIME>;
  distinct_aggregator_factory_[STRING] = DistinctAggregatorCreator<STRING>;
}

#undef NUMERIC_TYPE_FACTORY_INIT
#undef NUMERIC_TYPE_FACTORY_INIT_SECOND_TYPE
#undef NON_NUMERIC_TYPE_FACTORY_INIT

ColumnAggregatorFactoryImpl::~ColumnAggregatorFactoryImpl() {}

FailureOrOwned<ColumnAggregator> ColumnAggregatorFactoryImpl::CreateAggregator(
    Aggregation aggregation_operator,
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  CHECK_NOTNULL(result_block);
  CHECK_LT(result_column_index, result_block->schema().attribute_count());
  const Attribute& column_attrribute =
      result_block->schema().attribute(result_column_index);
  CHECK(column_attrribute.is_nullable());

  DataType output_type = column_attrribute.type();

  if (!IsAggregationSupported(aggregation_operator, input_type, output_type)) {
    THROW(new Exception(
        ERROR_INVALID_ARGUMENT_TYPE,
        StringPrintf("Aggregation not supported. Aggregation function %s not "
                     "defined for types %s and %s.",
                     Aggregation_Name(aggregation_operator).c_str(),
                     DataType_Name(input_type).c_str(),
                     DataType_Name(output_type).c_str())));
  }
  return Success(
      aggregator_factory_[aggregation_operator][input_type][output_type](
          result_block, result_column_index));
}

FailureOrOwned<ColumnAggregator>
ColumnAggregatorFactoryImpl::CreateDistinctAggregator(
    Aggregation aggregation_operator,
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  FailureOrOwned<ColumnAggregator> aggregator =
      CreateAggregator(aggregation_operator,
                       input_type,
                       result_block,
                       result_column_index);
  if (aggregator.is_failure()) {
    return aggregator;
  }

  CHECK(distinct_aggregator_factory_.find(input_type) !=
        distinct_aggregator_factory_.end());

  return Success(
      distinct_aggregator_factory_[input_type](
          down_cast<ColumnAggregatorInternal*>(aggregator.release()),
          result_block));
}

FailureOrOwned<ColumnAggregator>
ColumnAggregatorFactoryImpl::CreateCountAggregator(
    Block* result_block,
    int result_column_index) {
  CHECK_NOTNULL(result_block);
  CHECK_LT(result_column_index, result_block->schema().attribute_count());
  const Attribute& column_attrribute =
      result_block->schema().attribute(result_column_index);
  CHECK(!column_attrribute.is_nullable());

  DataType output_type = column_attrribute.type();

  if (count_aggregator_factory_.find(output_type) ==
      count_aggregator_factory_.end()) {
    THROW(new Exception(
        ERROR_INVALID_ARGUMENT_TYPE,
        StringPrintf("Aggregation not supported. Count can not store result "
                     "in output column of type %d.", output_type)));
  }
  return Success(
      count_aggregator_factory_[output_type](
          result_block, result_column_index));
}

FailureOrOwned<ColumnAggregator> ColumnAggregatorFactoryImpl::
CreateDistinctCountAggregator(
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  FailureOrOwned<ColumnAggregator> count_aggregator = CreateCountAggregator(
      result_block, result_column_index);
  if (count_aggregator.is_failure()) { return count_aggregator; }
  CHECK(distinct_aggregator_factory_.find(input_type) !=
        distinct_aggregator_factory_.end());

  return Success(
      distinct_aggregator_factory_[input_type](
          down_cast<ColumnAggregatorInternal*>(count_aggregator.release()),
          result_block));
}

bool ColumnAggregatorFactoryImpl::IsAggregationSupported(
    Aggregation aggregation_operator, DataType t1, DataType t2) {
  if (aggregator_factory_.find(aggregation_operator) ==
      aggregator_factory_.end() ||
      aggregator_factory_[aggregation_operator].find(t1) ==
      aggregator_factory_[aggregation_operator].end() ||
      aggregator_factory_[aggregation_operator][t1].find(t2) ==
      aggregator_factory_[aggregation_operator][t1].end()) {
    return false;
  }
  return true;
}

ColumnAggregatorFactory::ColumnAggregatorFactory()
    : pimpl_(new ColumnAggregatorFactoryImpl) {}

ColumnAggregatorFactory::~ColumnAggregatorFactory() {}

FailureOrOwned<ColumnAggregator> ColumnAggregatorFactory::CreateAggregator(
    Aggregation aggregation_operator,
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  return pimpl_->CreateAggregator(aggregation_operator,
                                  input_type,
                                  result_block,
                                  result_column_index);
}

FailureOrOwned<ColumnAggregator>
ColumnAggregatorFactory::CreateDistinctAggregator(
    Aggregation aggregation_operator,
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  return pimpl_->CreateDistinctAggregator(aggregation_operator,
                                          input_type,
                                          result_block,
                                          result_column_index);
}

FailureOrOwned<ColumnAggregator>
ColumnAggregatorFactory::CreateCountAggregator(
    Block* result_block,
    int result_column_index) {
  return pimpl_->CreateCountAggregator(result_block, result_column_index);
}

FailureOrOwned<ColumnAggregator>
ColumnAggregatorFactory::CreateDistinctCountAggregator(
    DataType input_type,
    Block* result_block,
    int result_column_index) {
  return pimpl_->CreateDistinctCountAggregator(input_type,
                                               result_block,
                                               result_column_index);
}

}  // namespace aggregations
}  // namespace supersonic
