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
// Author:  onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/core/comparison_bound_expressions.h"

#include <stddef.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <unordered_set>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/expression/templated/cast_bound_expression.h"
#include "supersonic/expression/vector/vector_logic.h"

namespace supersonic {

class BufferAllocator;
namespace operators {
struct Equal;
struct Hash;
struct Less;
}  // namespace operators

namespace {

const int kBinarySearchOverHashLimit = 16;

// A class used only in BoundInSetExpression. This keeps an array of hold_type.
template<DataType data_type>
class SortedVector {
  typedef typename TypeTraits<data_type>::cpp_type cpp_type;

 public:
  SortedVector() : is_initialized_(false), size_(0) {}

  ~SortedVector() {}

  inline size_t size() const { return size_; }

  // Initialize SortedVector. Can only be called once.
  template<class InputIterator>
  void insert(InputIterator first, InputIterator last) {
    CHECK(!is_initialized_);
    is_initialized_ = true;
    while (first != last) {
      CHECK(size_ < kBinarySearchOverHashLimit);
      container_[size_] = *first;
      ++first;
      ++size_;
    }
    sort(container_, container_ + size_, operators::Less());
  }

  // Returns 0-based index of the first occurrence of searched_value in the
  // vector, or size() if no such value is found.
  // find() is meant to be used conjunctively with end() to mimic the behavior
  // of regular STL's containers. An example usage is:
  // bool value_is_present = sorted_vector.find(value) != sorted_vector.end();
  inline size_t find(const cpp_type& searched_value) const {
    // This is called during DoEvaluation, so it's a DCHECK.
    DCHECK(is_initialized_);
    // It's binary search.
    size_t lower_bound, upper_bound, mid;
    lower_bound = 0;
    upper_bound = size();
    while (lower_bound < upper_bound) {
      mid = (lower_bound + upper_bound) >> 1;
      ComparisonResult result = ThreeWayCompare<data_type, data_type, false>(
          container_[mid], searched_value);
      switch (result) {
        case RESULT_EQUAL:
          return mid;
        case RESULT_GREATER:
          upper_bound = mid;
          break;
        case RESULT_LESS:
          lower_bound = mid + 1;
          break;
        default:
          LOG(FATAL) << "Unexpected error at SortedVector.find()";
      }
    }
    return size();
  }

  // See find().
  inline size_t end() const {
    return size();
  }

 private:
  bool is_initialized_;
  size_t size_;
  cpp_type container_[kBinarySearchOverHashLimit];
  DISALLOW_COPY_AND_ASSIGN(SortedVector);
};

template<DataType data_type, typename set_type>
class BoundInSetExpression : public BasicBoundExpression {
  typedef typename TypeTraits<data_type>::cpp_type cpp_type;
  typedef typename TypeTraits<data_type>::hold_type hold_type;

 public:
  // Needle expression, haystack arguments, and haystack constants are expected
  // to evaluate to the same data type (which is the templated data_type). It is
  // assumed that all expressions in haystack_arguments cannot be resolved to
  // constant.
  BoundInSetExpression(BufferAllocator* allocator,
                       string name,
                       BoundExpression* needle_expression,
                       BoundExpressionList* haystack_arguments,
                       const vector<hold_type>& haystack_constants,
                       bool contains_null_constant)
      : BasicBoundExpression(
          CreateSchema(name,
                       BOOL,
                       contains_null_constant ? NULLABLE : NullabilityOr(
                           GetExpressionNullability(needle_expression),
                           GetExpressionListNullability(haystack_arguments))),
          allocator),
        needle_expression_(needle_expression),
        haystack_arguments_(haystack_arguments),
        local_skip_vector_storage_(3, allocator),
        local_skip_vectors_(1),
        contains_null_constant_(contains_null_constant) {
    CHECK(CheckExpressionType(
        data_type, needle_expression_.get()).is_success());
    for (int i = 0; i < haystack_arguments_->size(); ++i) {
      CHECK(CheckExpressionListMembersType(
          data_type, haystack_arguments_.get()).is_success());
    }
    if (TypeTraits<data_type>::is_variable_length) {
      // We need to keep original data only for var-length types.
      // Variable set_ will hold references to the data actually stored in
      // values_.
      haystack_constants_ = haystack_constants;
      haystack_set_.insert(haystack_constants_.begin(),
                           haystack_constants_.end());
    } else {
      haystack_set_.insert(haystack_constants.begin(),
                           haystack_constants.end());
    }
  }
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    // Normally we would reset arenas here, but in this case we know the block
    // is actually boolean.
    bool_ptr skip_vector = skip_vectors.column(0);
    const size_t row_count = input.row_count();
    // Step 1: Evaluate the needle expression.
    EvaluationResult needle_expression_result =
        needle_expression_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(needle_expression_result);
    // Step 2: Prepare the output data and initialize it.
    // We will treat it as a vector that's true for a particular row if at
    // least one of the following is true:
    // a) it's skipped either because needle expression evaluates to null
    //    or it's skipped by the given skip_vector.
    // b) it's match has been found.
    bit_pointer::FillFrom(skipped_or_matched_or_needle_expression_null(),
                          skip_vector, row_count);
    // Step 3: Try to match the expression with the constant-valued set
    // expressions.
    DCHECK_EQ(needle_expression_result.get().schema().attribute(0).type(),
              data_type);
    const cpp_type* input_data =
        needle_expression_result.get().column(0).typed_data<data_type>();
    rowcount_t count = row_count;
    bool_ptr output_data = skipped_or_matched_or_needle_expression_null();
    // NOTE(onufry): The selectivity threshold depends on the size of the set we
    // check. We also set the selectivity level to 100 (always select) if the
    // hold type is variable length. We could probably tweak it some more, but
    // I don't really think it's worth it.
    if (!SelectivityIsGreaterThan(output_data, count,
                                  std::min(haystack_set_.size() * 10,
                                           size_t(100)))
        && !TypeTraits<data_type>::is_variable_length) {
      while (count-- > 0) {
        *output_data++ =
            haystack_set_.find(*input_data++) != haystack_set_.end();
      }
    } else {
      while (count-- > 0) {
        if (!*output_data) {
          *output_data = haystack_set_.find(*input_data) != haystack_set_.end();
        }
        ++input_data;
        ++output_data;
      }
    }
    // For performance, if all expression in the set are non-null constants, we
    // immediately return.
    if (haystack_arguments_->size() == 0 && !contains_null_constant_) {
      bit_pointer::FillFrom(
          my_block()->mutable_column(0)->template mutable_typed_data<BOOL>(),
          skipped_or_matched_or_needle_expression_null(),
          row_count);
      my_view()->set_row_count(row_count);
      my_view()->mutable_column(0)->ResetIsNull(skip_vector);
      return Success(*my_view());
    }
    // Step 4: We try to match the expression with the expression-valued set
    // expressions.
    if (contains_null_constant_) {
      bit_pointer::FillWithTrue(has_null(), row_count);
    } else {
      bit_pointer::FillWithFalse(has_null(), row_count);
    }
    for (size_t set_index = 0; set_index < haystack_arguments_->size();
         ++set_index) {
      // Step 4a: make a local copy of the following boolean column. The reason
      // is that the haystack argument modifies the input skip vector if the
      // resulting row is null, whereas in the In expression it is not
      // necessarily the case that the output is null if one of the haystack
      // argument evaluate to null.
      bit_pointer::FillFrom(skipped_or_matched_or_needle_expression_null_copy(),
                            skipped_or_matched_or_needle_expression_null(),
                            row_count);
      // Step 4b: Evaluate the expression (only on the rows not skipped).
      local_skip_vectors_.ResetColumn(
          0, skipped_or_matched_or_needle_expression_null_copy());
      EvaluationResult haystack_result =
          haystack_arguments_->get(set_index)->DoEvaluate(input,
                                                          local_skip_vectors_);
      PROPAGATE_ON_FAILURE(haystack_result);
      // Convenience shortcuts.
      const cpp_type* haystack_expression_iterator =
          haystack_result.get().column(0).typed_data<data_type>();
      const cpp_type* needle_expression_iterator =
          needle_expression_result.get().column(0).typed_data<data_type>();
      // Step 4c: We iterate each row and if it is not skipped and if the set
      // expression being evaluated is equal to base, we mark this row as
      // matched.
      // TODO(user): Determine if it's more optimal to use the selectivity
      // threshold similar to part 3.
      bool_ptr skip_iterator = skipped_or_matched_or_needle_expression_null();
      operators::Equal equal;
      // For performance, differentiate rows having nulls and not.
      if (haystack_arguments_.get()->get(set_index)->result_schema().
          attribute(0).is_nullable()) {
        bool_const_ptr is_null_iterator = haystack_result.get().column(0)
            .is_null();
        bool_ptr has_null_iterator = has_null();
        for (size_t row_id = 0; row_id < row_count; ++row_id) {
          // We only need to check equality between needle and haystack
          // if needle is not null, not skipped, not yet matched
          // (all three covered by skip_iterator) and haystack is not
          // null.
          *skip_iterator |= !*skip_iterator && !*is_null_iterator && equal(
              *haystack_expression_iterator, *needle_expression_iterator);
          *has_null_iterator |= *is_null_iterator;
          ++skip_iterator;
          ++haystack_expression_iterator;
          ++needle_expression_iterator;
          ++is_null_iterator;
          ++has_null_iterator;
        }
      } else {
        for (size_t row_id = 0; row_id < row_count; ++row_id) {
          // See the comment above.
          *skip_iterator |= !*skip_iterator && equal(
              *haystack_expression_iterator,
              *needle_expression_iterator);
          ++skip_iterator;
          ++haystack_expression_iterator;
          ++needle_expression_iterator;
        }
      }
    }
    // Step 5: Update which rows are going to be set as NULLs.
    // Note: is_null_result is reusing the memory of
    // skipped_or_matched_or_needle_expression_null_copy (since it is not
    // intended to be used after step 4.
    vector_logic::AndNot(skipped_or_matched_or_needle_expression_null(),
                         has_null(), row_count, is_null_result());
    vector_logic::Or(is_null_result(), skip_vector,
                     row_count, is_null_result());
    // Step 6: Update the input skip_vector.
    bit_pointer::FillFrom(skip_vector, is_null_result(), row_count);
    // Step 7: Update the output
    bit_pointer::FillFrom(
        my_block()->mutable_column(0)->template mutable_typed_data<BOOL>(),
        skipped_or_matched_or_needle_expression_null(),
        row_count);
    my_view()->set_row_count(row_count);
    my_view()->mutable_column(0)->ResetIsNull(is_null_result());
    return Success(*my_view());
  }

  // Needle in <haystack_constants, haystack_arguments> can be resolved if
  // needle expression is a constant and at least one of the following is true:
  // a) haystack_arguments is empty.
  // b) needle expression evaluates null.
  // c) the value of the needle expression is present in haystack_constants.
  bool can_be_resolved() const {
    if (!needle_expression_->is_constant()) {
      return false;
    } else {
      if (haystack_arguments_->size() == 0) {
        // Case a) happens.
        return true;
      }
      bool is_null;
      FailureOr<hold_type> value = GetConstantBoundExpressionValue<data_type>(
          needle_expression_.get(), &is_null);
      // If actual evaluation of needle expression fails, we can't propagate
      // the failure inside this method. So we say that this can be evaluated,
      // and then when the real evaluation is done, the failure will be
      // correctly propagated.
      if (value.is_failure()) {
        return true;
      }
      if (is_null) {
        // Case b) happens.
        return true;
      }
      if (haystack_set_.find(value.get()) != haystack_set_.end()) {
        // Case c) happens.
        return true;
      }
      return false;
    }
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    haystack_arguments_->
        CollectReferredAttributeNames(referred_attribute_names);
    needle_expression_->CollectReferredAttributeNames(referred_attribute_names);
  }

 private:
  virtual FailureOrVoid PostInit() {
    PROPAGATE_ON_FAILURE_WITH_CONTEXT(
        local_skip_vector_storage_.TryReallocate(my_block()->row_capacity()),
        "", result_schema().GetHumanReadableSpecification());
    return Success();
  }
  scoped_ptr<BoundExpression> needle_expression_;
  scoped_ptr<BoundExpressionList> haystack_arguments_;
  // Marks which rows has at least one null in one of its expressions.
  bool_ptr has_null() { return local_skip_vector_storage_.view().column(0); }
  // Stores the skip vector copy for set expressions' DoEvaluate.
  bool_ptr skipped_or_matched_or_needle_expression_null_copy() {
    return local_skip_vector_storage_.view().column(1);
  }
  // Stores which rows should have null in the return value.
  // Also see notes for step 5.
  bool_ptr is_null_result() {
    return local_skip_vector_storage_.view().column(1);
  }
  // Stores which rows should have null in the return value.
  // Also see notes for step 5.
  bool_ptr skipped_or_matched_or_needle_expression_null() {
    return local_skip_vector_storage_.view().column(2);
  }
  // Local storage.
  BoolBlock local_skip_vector_storage_;
  // Local storage for the pointers to the data of the children.
  BoolView local_skip_vectors_;
  // Local storage for keeping values of haystack constants in the event that
  // they are of variable-length. This is because in this case the actual
  // content of them is not stored in haystack_set_ (for example, StringPiece
  // does not actually own the string).
  vector<hold_type> haystack_constants_;
  set_type haystack_set_;
  // True if it has a NULL as one of its constants.
  bool contains_null_constant_;
  DISALLOW_COPY_AND_ASSIGN(BoundInSetExpression);
};

// --------------------- Tools for comparisons ---------------------------------

// Two equal non-integer types on input, templated output.
// Factory creation using types infrastructure.
template<OperatorId op>
struct EqualTypeFactoryCreator {
  template<DataType type>
  BinaryExpressionFactory* operator()() const {
    return new SpecializedBinaryFactory<op, type, type, BOOL>();
  }
};

template<OperatorId op>
BinaryExpressionFactory* CreateTwoEqualTypesComparisonFactory(DataType type) {
  return TypeSpecialization<BinaryExpressionFactory*,
         EqualTypeFactoryCreator<op> >(type);
}

// A helper, inserts the right (numeric) type into an comparison with a defined
// left-hand type.
template<OperatorId op, DataType left_type>
BinaryExpressionFactory* InsertRightType(DataType right_type) {
  switch (right_type) {
    case INT32:
      return new SpecializedBinaryFactory<op, left_type, INT32, BOOL>();
    case INT64:
      return new SpecializedBinaryFactory<op, left_type, INT64, BOOL>();
    case UINT32:
      return new SpecializedBinaryFactory<op, left_type, UINT32, BOOL>();
    case UINT64:
      return new SpecializedBinaryFactory<op, left_type, UINT64, BOOL>();
    default: return NULL;
  }
}

// A comparison factory for two integer types.
template<OperatorId op>
BinaryExpressionFactory* CreateTwoTypesBinaryFactory(DataType left_type,
                                                     DataType right_type) {
  switch (left_type) {
    case INT32: return InsertRightType<op, INT32>(right_type);
    case INT64: return InsertRightType<op, INT64>(right_type);
    case UINT32: return InsertRightType<op, UINT32>(right_type);
    case UINT64: return InsertRightType<op, UINT64>(right_type);
    default: return NULL;
  }
}

FailureOrOwned<BoundExpression> EqualTypeComparison(
    BufferAllocator* allocator,
    rowcount_t max_row_count,
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    OperatorId operator_id,
    DataType type) {
  BinaryExpressionFactory* factory = NULL;
  switch (operator_id) {
    case OPERATOR_EQUAL:
        factory = CreateTwoEqualTypesComparisonFactory<OPERATOR_EQUAL>(type);
        break;
    case OPERATOR_NOT_EQUAL:
        factory =
            CreateTwoEqualTypesComparisonFactory<OPERATOR_NOT_EQUAL>(type);
        break;
    case OPERATOR_LESS:
        factory = CreateTwoEqualTypesComparisonFactory<OPERATOR_LESS>(type);
        break;
    case OPERATOR_LESS_OR_EQUAL:
        factory =
            CreateTwoEqualTypesComparisonFactory<OPERATOR_LESS_OR_EQUAL>(type);
        break;
    default:
        LOG(FATAL) << "Unknown operator in Comparison creation";
  }
  return RunBinaryFactory(factory, allocator, max_row_count, left_ptr,
                          right_ptr, "Comparison");
}

template<OperatorId op>
FailureOrOwned<BoundExpression>
    AsymmetricOperatorDifferentIntegerTypesComparison(
        BufferAllocator* allocator,
        rowcount_t max_row_count,
        BoundExpression* left,
        BoundExpression* right) {
  COMPILE_ASSERT(op == OPERATOR_LESS || op == OPERATOR_LESS_OR_EQUAL,
                 unexpected_operator_given_as_asymmetric_comparison);
  BinaryExpressionFactory* factory = CreateTwoTypesBinaryFactory<op>(
      GetExpressionType(left), GetExpressionType(right));
  return RunBinaryFactory(factory, allocator, max_row_count, left, right,
                          "Asymmetric comparison");
}

template<OperatorId op>
FailureOrOwned<BoundExpression>
    SymmetricOperatorDifferentIntegerTypesComparison(
        BufferAllocator* allocator,
        rowcount_t max_row_count,
        BoundExpression* left,
        BoundExpression* right) {
  COMPILE_ASSERT(op == OPERATOR_EQUAL || op == OPERATOR_NOT_EQUAL,
                 unexpected_operator_given_as_symmetric_comparison);
  DataType left_type = GetExpressionType(left);
  DataType right_type = GetExpressionType(right);
  // For EQUAL and NOT_EQUAL, which are symmetric, we can shave off another 12
  // template expansions by assuring that the left-hand argument is smaller
  // (in some arbitrary ordering) than the right-hand. We use the ordering
  // INT32 < UINT32 < INT64 < UINT64.
  BinaryExpressionFactory* factory = NULL;
  // The sequence of if's, instead of a cross-specialization, is in order to
  // assure that we do not instantiate unnecessary templates.
  if (left_type == INT32 && right_type == UINT32) {
    factory = new SpecializedBinaryFactory<op, INT32, UINT32, BOOL>();
  }
  if (left_type == INT32 && right_type == INT64) {
    factory = new SpecializedBinaryFactory<op, INT32, INT64, BOOL>();
  }
  if (left_type == INT32 && right_type == UINT64) {
    factory = new SpecializedBinaryFactory<op, INT32, UINT64, BOOL>();
  }
  if (left_type == UINT32 && right_type == INT64) {
    factory = new SpecializedBinaryFactory<op, UINT32, INT64, BOOL>();
  }
  if (left_type == UINT32 && right_type == UINT64) {
    factory = new SpecializedBinaryFactory<op, UINT32, UINT64, BOOL>();
  }
  if (left_type == INT64 && right_type == UINT64) {
    factory = new SpecializedBinaryFactory<op, INT64, UINT64, BOOL>();
  }
  // The symmetric cases (where we want to exchange the arguments and run
  // again):
  if ((left_type == UINT32 && right_type == INT32) ||
      (left_type == INT64  && right_type == INT32) ||
      (left_type == UINT64 && right_type == INT32) ||
      (left_type == INT64  && right_type == UINT32) ||
      (left_type == UINT64 && right_type == UINT32) ||
      (left_type == UINT64 && right_type == INT64)) {
    return SymmetricOperatorDifferentIntegerTypesComparison<op>(
        allocator, max_row_count, right, left);
  }
  return RunBinaryFactory(factory, allocator, max_row_count, left, right,
                          "Symmetric comparison");
}

FailureOrOwned<BoundExpression> IntegerDifferentTypeComparison(
    BufferAllocator* allocator,
    rowcount_t max_row_count,
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    OperatorId operator_id) {
  DataType left_type = GetExpressionType(left_ptr);
  DataType right_type = GetExpressionType(right_ptr);
  DCHECK(GetTypeInfo(left_type).is_integer());
  DCHECK(GetTypeInfo(right_type).is_integer());
  DCHECK(left_type != right_type);
  if (operator_id == OPERATOR_EQUAL) {
    return SymmetricOperatorDifferentIntegerTypesComparison<
        OPERATOR_EQUAL>(allocator, max_row_count, left_ptr, right_ptr);
  }
  if (operator_id == OPERATOR_NOT_EQUAL) {
    return SymmetricOperatorDifferentIntegerTypesComparison<
        OPERATOR_NOT_EQUAL>(allocator, max_row_count, left_ptr, right_ptr);
  }
  if (operator_id == OPERATOR_LESS) {
    return AsymmetricOperatorDifferentIntegerTypesComparison<
        OPERATOR_LESS>(allocator, max_row_count, left_ptr, right_ptr);
  }
  if (operator_id == OPERATOR_LESS_OR_EQUAL) {
    return AsymmetricOperatorDifferentIntegerTypesComparison<
        OPERATOR_LESS_OR_EQUAL>(allocator, max_row_count, left_ptr, right_ptr);
  }
  LOG(FATAL) << "Unexpected operator in IntegerDifferentTypeComparison";
}

// We explore all possibilities by hand here. The reason we do this is that
// there is a lot of template expansion that is performed, and this impacts
// binary size and compile times - thus we attempt to expand only those
// templates that are strictly necessary.
FailureOrOwned<BoundExpression> GenerateComparison(BufferAllocator* allocator,
                                                   rowcount_t max_row_count,
                                                   BoundExpression* left_ptr,
                                                   BoundExpression* right_ptr,
                                                   OperatorId operator_id) {
  scoped_ptr<BoundExpression> left(left_ptr);
  scoped_ptr<BoundExpression> right(right_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount("Comparison",
                                           left.get()->result_schema(),
                                           1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount("Comparison",
                                           right.get()->result_schema(),
                                           1));
  DataType left_type = GetExpressionType(left.get());
  DataType right_type = GetExpressionType(right.get());
  // The easy case.
  if (left_type == right_type) {
    return EqualTypeComparison(allocator, max_row_count, left.release(),
                               right.release(), operator_id, left_type);
  }
  // Now we have to deal with non-equal types. We will do something only if both
  // the types are numeric.
  if (!GetTypeInfo(left_type).is_numeric() ||
      !GetTypeInfo(right_type).is_numeric()) {
    THROW(new Exception(
        ERROR_ATTRIBUTE_TYPE_MISMATCH,
        "Cannot compare expressions of different, non-numeric types"));
  }
  // If any of the types is DOUBLE, compare as doubles (force cast).
  if (left_type == DOUBLE || right_type == DOUBLE) {
    return EqualTypeComparison(allocator, max_row_count, left.release(),
                               right.release(), operator_id, DOUBLE);
  }
  // If any of the types is FLOAT, compare as floats (force cast).
  if (left_type == FLOAT || right_type == FLOAT) {
    // We perform force casts to FLOAT here, non-explicit, as we want to force
    // (the normally not performed) INT64->FLOAT casts.
    FailureOrOwned<BoundExpression> cast_left = BoundInternalCast(
        allocator, max_row_count, left.release(), FLOAT, false);
    PROPAGATE_ON_FAILURE(cast_left);
    FailureOrOwned<BoundExpression> cast_right = BoundInternalCast(
        allocator, max_row_count, right.release(), FLOAT, false);
    PROPAGATE_ON_FAILURE(cast_right);
    return EqualTypeComparison(allocator, max_row_count, cast_left.release(),
                               cast_right.release(), operator_id, FLOAT);
  }
  // Now we have two integers of different types.
  return IntegerDifferentTypeComparison(
      allocator, max_row_count, left.release(), right.release(), operator_id);
}

// ----------------In set expression instantiation -----------------------------

// Called by a TypeSpecialization.
template<DataType data_type>
FailureOrOwned<BoundExpression> BoundInSetDataTypeAware(
    BoundExpression* input_needle_expression,
    BoundExpressionList* input_haystack_arguments,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> needle_expression(input_needle_expression);
  scoped_ptr<BoundExpressionList> haystack_arguments(input_haystack_arguments);
  for (int i = 0; i < haystack_arguments->size(); ++i) {
    string name = StringPrintf("The %dth element on the in list", i);
    BoundExpression* child = haystack_arguments->get(i);
    PROPAGATE_ON_FAILURE(
        CheckAttributeCount(name, child->result_schema(), 1));
  }

  string name = StrCat(GetExpressionName(needle_expression.get()),
                       " IN (",
                       haystack_arguments->ToString(/* verbose = */ false),
                       ")");
  // Evaluates constant expressions and store them in a vector to gain
  // performance.
  typedef typename TypeTraits<data_type>::cpp_type cpp_type;
  typedef typename TypeTraits<data_type>::hold_type hold_type;
  vector<hold_type> haystack_constants;
  scoped_ptr<BoundExpressionList> haystack_non_constant_expressions(
      new BoundExpressionList());
  bool contains_null_constant = false;
  for (int i = 0; i < haystack_arguments->size(); ++i) {
    if (haystack_arguments->get(i)->is_constant()) {
      scoped_ptr<BoundExpression> scoped_arguments_element(
          haystack_arguments->release(i));
      bool is_null;
      FailureOr<hold_type> value = GetConstantBoundExpressionValue<data_type>(
          scoped_arguments_element.get(), &is_null);
      // The binding will fail on X in <0, 1/0> currently.
      PROPAGATE_ON_FAILURE(value);
      if (is_null) {
        contains_null_constant = true;
        continue;
      }
      haystack_constants.push_back(value.get());
    } else {
      haystack_non_constant_expressions->add(haystack_arguments->release(i));
    }
  }

  // This part is responsible for choosing between using binary search or
  // hashing for matching needle with constant-valued haystack expressions.
  BasicBoundExpression* bound;
  typedef typename TypeTraits<data_type>::cpp_type cpp_type;
  // Note: SortedVector and hash_set stores cpp_type instead of hold_type. If
  // the data type is of variable length, the copying is done in
  // BoundInSetExpression.
  if (haystack_constants.size() <= kBinarySearchOverHashLimit) {
    bound = new BoundInSetExpression<data_type,
        SortedVector<data_type> >(
          allocator,
          name,
          needle_expression.release(),
          haystack_non_constant_expressions.release(),
          haystack_constants,
          contains_null_constant);
  } else {
    bound = new BoundInSetExpression<data_type,
        std::unordered_set<cpp_type, operators::Hash, operators::Equal> >(
            allocator,
            name,
            needle_expression.release(),
            haystack_non_constant_expressions.release(),
            haystack_constants,
            contains_null_constant);
  }
  return InitBasicExpression(max_row_count, bound, allocator);
}

// Functor used in the TypeSpecialization setup (see types_infrastructure.h).
struct InSetResolver {
  InSetResolver(BoundExpression* needle_expression,
                BoundExpressionList* haystack_arguments,
                BufferAllocator* allocator,
                rowcount_t max_row_count)
      : allocator_(allocator),
        needle_expression_(needle_expression),
        haystack_arguments_(haystack_arguments),
        max_row_count_(max_row_count) {}

  template<DataType data_type>
  FailureOrOwned<BoundExpression> operator()() const {
    return BoundInSetDataTypeAware<data_type>(
        needle_expression_, haystack_arguments_, allocator_, max_row_count_);
  }
  BufferAllocator* allocator_;
  BoundExpression* needle_expression_;
  BoundExpressionList* haystack_arguments_;
  rowcount_t max_row_count_;
};

}  // namespace

// -------------------------- IsOdd and IsEven ---------------------------------

FailureOrOwned<BoundExpression> BoundIsOdd(BoundExpression* argument,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count) {
  return CreateUnaryIntegerInputExpression<OPERATOR_IS_ODD, BOOL>(
      allocator, max_row_count, argument);
}

FailureOrOwned<BoundExpression> BoundIsEven(BoundExpression* argument,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  return CreateUnaryIntegerInputExpression<OPERATOR_IS_EVEN, BOOL>(
      allocator, max_row_count, argument);
}

// ---------------- In expression set expression instantiation -----------------

// Called by the general In expression.
FailureOrOwned<BoundExpression> BoundInSet(
    BoundExpression* input_needle_expression,
    BoundExpressionList* input_haystack_arguments,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  scoped_ptr<BoundExpression> needle_expression(input_needle_expression);
  scoped_ptr<BoundExpressionList> haystack_arguments(input_haystack_arguments);
  PROPAGATE_ON_FAILURE(CheckAttributeCount("Needle Bound Expression",
                                           needle_expression->result_schema(),
                                           1));
  for (int i = 0; i < haystack_arguments->size(); ++i) {
    PROPAGATE_ON_FAILURE(CheckAttributeCount(
        StringPrintf("The %d-th expression in a Bound Expression List", i),
        haystack_arguments->get(i)->result_schema(),
        1));
  }
  // Determine the most general data type.
  DataType common_data_type =
      needle_expression->result_schema().attribute(0).type();
  for (int i = 0; i < haystack_arguments->size(); ++i) {
    FailureOr<DataType> new_common_data_type =
        CalculateCommonType(common_data_type,
                            GetExpressionType(haystack_arguments->get(i)));
    PROPAGATE_ON_FAILURE(new_common_data_type);
    common_data_type = new_common_data_type.get();
  }

  // Cast each expression to common data type. Note that they will still be
  // casted even if the data type is already the same because it incurs no
  // extra cost in that case.
  FailureOrOwned<BoundExpression> converted = BoundInternalCast(
      allocator,
      max_row_count,
      needle_expression.release(),
      common_data_type,
      true);  // Disallow down-casting
  PROPAGATE_ON_FAILURE(converted);
  needle_expression.reset(converted.release());
  scoped_ptr<BoundExpressionList> converted_arguments(
      new BoundExpressionList());
  for (int i = 0; i < haystack_arguments->size(); ++i) {
    FailureOrOwned<BoundExpression> converted = BoundInternalCast(
        allocator, max_row_count, haystack_arguments->release(i),
        common_data_type, true);
    PROPAGATE_ON_FAILURE(converted);
    converted_arguments->add(converted.release());
  }

  InSetResolver resolver(needle_expression.release(),
                         converted_arguments.release(),
                         allocator,
                         max_row_count);
  return TypeSpecialization<FailureOrOwned<BoundExpression>,
      InSetResolver> (common_data_type, resolver);
}

// ------------------- Comparison instantiation --------------------------------

#define DEFINE_BINARY_COMPARISON_EXPRESSION(expression_name, operator_id)      \
FailureOrOwned<BoundExpression> expression_name(BoundExpression* left,         \
                                                BoundExpression* right,        \
                                                BufferAllocator* allocator,    \
                                                rowcount_t max_row_count) {    \
  return GenerateComparison(allocator, max_row_count,                          \
                            left, right, operator_id);                         \
}

DEFINE_BINARY_COMPARISON_EXPRESSION(BoundEqual, OPERATOR_EQUAL);
DEFINE_BINARY_COMPARISON_EXPRESSION(BoundNotEqual, OPERATOR_NOT_EQUAL);
DEFINE_BINARY_COMPARISON_EXPRESSION(BoundLess, OPERATOR_LESS);
DEFINE_BINARY_COMPARISON_EXPRESSION(BoundLessOrEqual,
                                    OPERATOR_LESS_OR_EQUAL);

FailureOrOwned<BoundExpression> BoundGreater(BoundExpression* left,
                                             BoundExpression* right,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  // We implement Greater in terms of Less to decrease the binary size.
  return GenerateComparison(allocator, max_row_count,
                            right, left, OPERATOR_LESS);
}

FailureOrOwned<BoundExpression> BoundGreaterOrEqual(BoundExpression* left,
                                                    BoundExpression* right,
                                                    BufferAllocator* allocator,
                                                    rowcount_t max_row_count) {
  return GenerateComparison(allocator, max_row_count,
                            right, left, OPERATOR_LESS_OR_EQUAL);
}

#undef DEFINE_BINARY_COMPARISON_EXPRESSION

}  // namespace supersonic
