## Process this file with automake to produce Makefile.in

# Make sure that when we re-make ./configure, we get the macros we need
ACLOCAL_AMFLAGS = -I m4

# These are good warnings to turn on by default
if GCC
AM_CXXFLAGS = -Wall -Wwrite-strings -Woverloaded-virtual -Wno-sign-compare
endif

AM_CPPFLAGS = -Wno-deprecated \
              -DNDEBUG \
              -DSUPERSONIC_FORCE_FAILURE_CHECK=1

GMOCK_FLAGS = -DGTEST_USE_OWN_TR1_TUPLE=1

if USE_RE2
RE2_LIBS = -lre2
else
RE2_LIBS =
endif

TEST_FLAGS = -I$(top_srcdir)/gmock/fused-src $(GMOCK_FLAGS)

COMPILER_FLAGS = $(PTHREAD_CFLAGS) \
    $(AM_CXXFLAGS) \
    $(GFLAGS_CFLAGS) \
    $(GLOG_CFLAGS) \
    $(PROTO_CFLAGS) \
    $(BOOST_CPPFLAGS)

SUPERSONIC_LIB_LD_FLAGS =
SUPERSONIC_LIB_LD_ADD = \
    libsupersonic.la \
    $(BOOST_LDFLAGS) \
    $(BOOST_SYSTEM_LIB) \
    $(BOOST_TIMER_LIB) \
    $(GFLAGS_LIBS) \
    $(GLOG_LIBS) \
    $(PROTO_LIBS)

SUPERSONIC_TESTS_FLAGS = $(COMPILER_FLAGS) $(TEST_FLAGS)
SUPERSONIC_TESTS_LD_FLAGS = $(SUPERSONIC_LIB_LD_FLAGS)
SUPERSONIC_TESTS_LD_ADD = $(SUPERSONIC_LIB_LD_ADD) libsupersonic_testutils.la libsupersonic_testutils_gunit.la libsupersonic_gmock.la libsupersonic_gmock_main.la

# Library
lib_LTLIBRARIES = libsupersonic.la

libsupersonic_la_CXXFLAGS = $(SUPERSONIC_TESTS_FLAGS)
libsupersonic_la_LDFLAGS =
libsupersonic_la_LIBADD =

# Testing utils
noinst_LTLIBRARIES =
noinst_LTLIBRARIES += libsupersonic_gmock.la
libsupersonic_gmock_la_CXXFLAGS = $(COMPILER_FLAGS) $(TEST_FLAGS)
libsupersonic_gmock_la_SOURCES = gmock/fused-src/gmock-gtest-all.cc
libsupersonic_gmock_ladir = $(includedir)
libsupersonic_gmock_la_HEADERS = \
    gmock/fused-src/gtest/gtest.h \
    gmock/fused-src/gmock/gmock.h

noinst_LTLIBRARIES += libsupersonic_gmock_main.la
libsupersonic_gmock_main_la_CXXFLAGS = $(COMPILER_FLAGS) $(TEST_FLAGS)
libsupersonic_gmock_main_la_SOURCES = test_utils/supersonic_test_main.cc

lib_LTLIBRARIES += libsupersonic_testutils.la

libsupersonic_testutils_la_CXXFLAGS = $(COMPILER_FLAGS)
libsupersonic_testutils_la_LDFLAGS =  $(SUPERSONIC_LIB_LD_FLAGS)
libsupersonic_testutils_la_LIBADD =   $(SUPERSONIC_LIB_LD_ADD)

libsupersonic_testutils_ladir = $(includedir)
nobase_libsupersonic_testutils_la_HEADERS = \
    supersonic/testing/block_builder.h \
    supersonic/testing/repeating_block.h \
    supersonic/testing/streamable.h \
    supersonic/testing/view_comparator.h

libsupersonic_testutils_la_SOURCES = \
    supersonic/testing/block_builder.cc \
    supersonic/testing/repeating_block.cc \
    supersonic/testing/streamable.cc \
    supersonic/testing/view_comparator.cc

# libsupersonic_testutils_gunit

lib_LTLIBRARIES += libsupersonic_testutils_gunit.la

libsupersonic_testutils_gunit_la_CXXFLAGS = $(COMPILER_FLAGS) $(TEST_FLAGS)
libsupersonic_testutils_gunit_la_LDFLAGS =  $(SUPERSONIC_LIB_LD_FLAGS)
libsupersonic_testutils_gunit_la_LIBADD =   $(SUPERSONIC_LIB_LD_ADD) libsupersonic_testutils.la libsupersonic_gmock.la

libsupersonic_testutils_gunit_ladir = $(includedir)
nobase_libsupersonic_testutils_gunit_la_HEADERS = \
    supersonic/testing/comparable_cursor.h \
    supersonic/testing/comparable_tuple_schema.h \
    supersonic/testing/comparable_view.h \
    supersonic/testing/comparators.h \
    supersonic/testing/expression_test_helper.h \
    supersonic/testing/operation_testing.h \
    supersonic/testing/proto_matcher.h \
    supersonic/testing/row.h \
    supersonic/testing/short_circuit_tester.h \
    supersonic/cursor/base/cursor_mock.h

libsupersonic_testutils_gunit_la_SOURCES = \
    supersonic/testing/comparable_cursor.cc \
    supersonic/testing/comparable_tuple_schema.cc \
    supersonic/testing/comparable_view.cc \
    supersonic/testing/expression_test_helper.cc \
    supersonic/testing/operation_testing.cc \
    supersonic/testing/row.cc \
    supersonic/testing/short_circuit_tester.cc

# Unit tests
supersonic_guidetest_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
supersonic_guidetest_LDFLAGS =  $(SUPERSONIC_TESTS_LD_FLAGS)
supersonic_guidetest_LDADD =    $(SUPERSONIC_TESTS_LD_ADD)

supersonic_guidetest_SOURCES = \
    test/smoke_test.cc \
    test/guide/group_sort.cc \
    test/guide/join.cc \
    test/guide/primer.cc

TESTS = supersonic_guidetest

# Unit Tests
#
## base
test_base_exception_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_base_exception_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_base_exception_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_base_exception_SOURCES  = \
    supersonic/base/exception/exception_test.cc \
    supersonic/base/exception/result_test.cc
TESTS += test_base_exception

test_base_infrastructure_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_base_infrastructure_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_base_infrastructure_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_base_infrastructure_SOURCES  = \
    supersonic/base/infrastructure/aggregation_operators_test.cc \
    supersonic/base/infrastructure/bit_pointers_test.cc \
    supersonic/base/infrastructure/block_test.cc \
    supersonic/base/infrastructure/copy_column_test.cc \
    supersonic/base/infrastructure/double_buffered_block_test.cc \
    supersonic/base/infrastructure/operators_test.cc \
    supersonic/base/infrastructure/projector_test.cc \
    supersonic/base/infrastructure/types_infrastructure_test.cc \
    supersonic/base/infrastructure/types_test.cc \
    supersonic/base/infrastructure/variant_pointer_test.cc \
    supersonic/base/infrastructure/view_copier_test.cc
TESTS += test_base_infrastructure

test_base_memory_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_base_memory_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_base_memory_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_base_memory_SOURCES  = \
    supersonic/base/memory/arena_test.cc \
    supersonic/base/memory/memory_mocks.h \
    supersonic/base/memory/memory_mocks.cc \
    supersonic/base/memory/memory_test.cc
TESTS += test_base_memory

# test regexp
test_regexp_expressions_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_regexp_expressions_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_regexp_expressions_LDADD    = $(SUPERSONIC_TESTS_LD_ADD) $(RE2_LIBS) libsupersonic_regexp.la
test_regexp_expressions_SOURCES  = \
    supersonic/expression/core/regexp_bound_expressions_test.cc \
    supersonic/expression/core/regexp_expressions_test.cc
if USE_RE2
REGEXP_TEST=test_regexp_expressions
else
REGEXP_TEST=
endif
TESTS += $(REGEXP_TEST)

## benchmark
test_benchmark_dot_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_benchmark_dot_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_benchmark_dot_LDADD    = $(SUPERSONIC_TESTS_LD_ADD) libsupersonic_benchmark.la
test_benchmark_dot_SOURCES  = \
    supersonic/benchmark/dot/dot_drawer_test.cc
TESTS += test_benchmark_dot

test_benchmark_infrastructure_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_benchmark_infrastructure_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_benchmark_infrastructure_LDADD    = $(SUPERSONIC_TESTS_LD_ADD) libsupersonic_benchmark.la
test_benchmark_infrastructure_SOURCES  = \
    supersonic/benchmark/infrastructure/benchmark_listener_test.cc \
    supersonic/benchmark/infrastructure/benchmark_transformer_test.cc \
    supersonic/benchmark/infrastructure/node_test.cc
TESTS += test_benchmark_infrastructure

test_benchmark_manager_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_benchmark_manager_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_benchmark_manager_LDADD    = $(SUPERSONIC_TESTS_LD_ADD) libsupersonic_benchmark.la
test_benchmark_manager_SOURCES  = \
    supersonic/benchmark/manager/benchmark_manager_test.cc
TESTS += test_benchmark_manager

## cursor
test_cursor_base_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_cursor_base_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_cursor_base_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_cursor_base_SOURCES  = \
    supersonic/cursor/base/lookup_index_test.cc
TESTS += test_cursor_base

test_cursor_core_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_cursor_core_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_cursor_core_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_cursor_core_SOURCES  = \
    supersonic/cursor/core/aggregate_clusters_test.cc \
    supersonic/cursor/core/aggregate_groups_test.cc \
    supersonic/cursor/core/aggregate_scalar_test.cc \
    supersonic/cursor/core/coalesce_test.cc \
    supersonic/cursor/core/column_aggregator_test.cc \
    supersonic/cursor/core/compute_test.cc \
    supersonic/cursor/core/filter_test.cc \
    supersonic/cursor/core/foreign_filter_test.cc \
    supersonic/cursor/core/generate_test.cc \
    supersonic/cursor/core/hash_join_test.cc \
    supersonic/cursor/core/hybrid_aggregate_test.cc \
    supersonic/cursor/core/hybrid_aggregate_large_test.cc \
    supersonic/cursor/core/hybrid_group_utils_test.cc \
    supersonic/cursor/core/limit_test.cc \
    supersonic/cursor/core/merge_union_all_test.cc \
    supersonic/cursor/core/project_test.cc \
    supersonic/cursor/core/rowid_merge_join_test.cc \
    supersonic/cursor/core/scan_view_test.cc \
    supersonic/cursor/core/sort_test.cc \
    supersonic/cursor/core/specification_builder_test.cc \
    supersonic/cursor/core/splitter_test.cc
TESTS += test_cursor_core

test_cursor_infrastructure_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_cursor_infrastructure_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_cursor_infrastructure_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_cursor_infrastructure_SOURCES  = \
    supersonic/cursor/infrastructure/basic_operation_test.cc \
    supersonic/cursor/infrastructure/iterators_test.cc \
    supersonic/cursor/infrastructure/row_copier_test.cc \
    supersonic/cursor/infrastructure/row_hash_set_test.cc \
    supersonic/cursor/infrastructure/row_test.cc \
    supersonic/cursor/infrastructure/table_test.cc \
    supersonic/cursor/infrastructure/view_cursor_test.cc \
    supersonic/cursor/infrastructure/writer_test.cc
TESTS += test_cursor_infrastructure

## expression
test_expression_core_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_expression_core_LDFLAGS = $(SUPERSONIC_TESTS_LD_FLAGS)
test_expression_core_LDADD = $(SUPERSONIC_TESTS_LD_ADD)
test_expression_core_SOURCES = \
    supersonic/expression/core/arithmetic_bound_expressions_test.cc \
    supersonic/expression/core/arithmetic_expressions_test.cc \
    supersonic/expression/core/case_expression_test.cc \
    supersonic/expression/core/comparison_bound_expressions_test.cc \
    supersonic/expression/core/date_bound_expressions_test.cc \
    supersonic/expression/core/date_expressions_test.cc \
    supersonic/expression/core/elementary_bound_expressions_test.cc \
    supersonic/expression/core/elementary_expressions_test.cc \
    supersonic/expression/core/math_bound_expressions_test.cc \
    supersonic/expression/core/math_expressions_test.cc \
    supersonic/expression/core/projecting_bound_expressions_test.cc \
    supersonic/expression/core/projecting_expressions_test.cc \
    supersonic/expression/core/stateful_bound_expressions_test.cc \
    supersonic/expression/core/stateful_expressions_test.cc \
    supersonic/expression/core/string_bound_expressions_test.cc \
    supersonic/expression/core/string_expressions_test.cc
TESTS += test_expression_core

test_expression_infrastructure_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_expression_infrastructure_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_expression_infrastructure_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_expression_infrastructure_SOURCES  = \
    supersonic/expression/infrastructure/basic_bound_expression_test.cc \
    supersonic/expression/infrastructure/expression_utils_test.cc \
    supersonic/expression/infrastructure/terminal_expressions_test.cc
TESTS += test_expression_infrastructure

test_expression_templated_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_expression_templated_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_expression_templated_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_expression_templated_SOURCES  = \
    supersonic/expression/templated/cast_bound_expression_test.cc \
    supersonic/expression/templated/cast_expression_test.cc
TESTS += test_expression_templated

test_expression_vector_CPPFLAGS = $(SUPERSONIC_TESTS_FLAGS)
test_expression_vector_LDFLAGS  = $(SUPERSONIC_TESTS_LD_FLAGS)
test_expression_vector_LDADD    = $(SUPERSONIC_TESTS_LD_ADD)
test_expression_vector_SOURCES  = \
    supersonic/expression/vector/binary_column_computers_test.cc \
    supersonic/expression/vector/column_validity_checkers_test.cc \
    supersonic/expression/vector/ternary_column_computers_test.cc \
    supersonic/expression/vector/unary_column_computers_test.cc \
    supersonic/expression/vector/vector_logic_test.cc \
    supersonic/expression/vector/vector_primitives_test.cc
TESTS += test_expression_vector

# Benchmark utilities
lib_LTLIBRARIES += libsupersonic_benchmark.la

libsupersonic_benchmark_la_CXXFLAGS = $(COMPILER_FLAGS)
libsupersonic_benchmark_la_LDFLAGS =
libsupersonic_benchmark_la_LIBADD =

libsupersonic_benchmark_ladir = $(includedir)
nobase_libsupersonic_benchmark_la_HEADERS = \
    supersonic/benchmark/base/benchmark_types.h \
    supersonic/benchmark/dot/dot_drawer.h \
    supersonic/benchmark/examples/common_utils.h \
    supersonic/benchmark/infrastructure/benchmark_listener.h \
    supersonic/benchmark/infrastructure/benchmark_listener_mock.h \
    supersonic/benchmark/infrastructure/benchmark_transformer.h \
    supersonic/benchmark/infrastructure/cursor_statistics.h \
    supersonic/benchmark/infrastructure/cursor_statistics_mock.h \
    supersonic/benchmark/infrastructure/node.h \
    supersonic/benchmark/infrastructure/tree_builder.h \
    supersonic/benchmark/manager/benchmark_manager.h

libsupersonic_benchmark_la_SOURCES = \
    supersonic/benchmark/dot/dot_drawer.cc \
    supersonic/benchmark/examples/common_utils.cc \
    supersonic/benchmark/infrastructure/benchmark_listener.cc \
    supersonic/benchmark/infrastructure/benchmark_transformer.cc \
    supersonic/benchmark/infrastructure/cursor_statistics.cc \
    supersonic/benchmark/infrastructure/node.cc \
    supersonic/benchmark/infrastructure/tree_builder.cc \
    supersonic/benchmark/manager/benchmark_manager.cc

# Benchmark examples
BENCHMARK_EXAMPLES = operation_example

## simple operation example
operation_example_CPPFLAGS = $(COMPILER_FLAGS)
operation_example_LDFLAGS  = $(SUPERSONIC_LIB_LD_FLAGS)
operation_example_LDADD    = $(SUPERSONIC_LIB_LD_ADD) libsupersonic_benchmark.la libsupersonic_testutils.la
operation_example_SOURCES  = supersonic/benchmark/examples/operation_example.cc


check_PROGRAMS = $(TESTS)
noinst_PROGRAMS = $(TESTS) $(BENCHMARK_EXAMPLES)

# Headers exported from the library.
supersonic_includedir = $(includedir)

protos = \
    supersonic/utils/exception/stack_trace.proto \
    supersonic/utils/proto/expressions.proto \
    supersonic/utils/proto/types.proto \
    supersonic/benchmark/proto/benchmark.proto \
    supersonic/cursor/proto/cursors.proto \
    supersonic/expression/proto/operators.proto \
    supersonic/proto/specification.proto \
    supersonic/proto/supersonic.proto

protos_H = \
    supersonic/utils/exception/stack_trace.pb.h \
    supersonic/utils/proto/expressions.pb.h \
    supersonic/utils/proto/types.pb.h \
    supersonic/benchmark/proto/benchmark.pb.h \
    supersonic/cursor/proto/cursors.pb.h \
    supersonic/expression/proto/operators.pb.h \
    supersonic/proto/specification.pb.h \
    supersonic/proto/supersonic.pb.h

sources_H = supersonic/utils/algorithm.h \
    supersonic/utils/arm_instruction_set_select.h \
    supersonic/utils/basictypes.h \
    supersonic/utils/bits.h \
    supersonic/utils/charmap.h \
    supersonic/utils/container_literal.h \
    supersonic/utils/container_literal_generated.h \
    supersonic/utils/container_logging.h \
    supersonic/utils/cycleclock-inl.h \
    supersonic/utils/demangle.h \
    supersonic/utils/fixedarray.h \
    supersonic/utils/integral_types.h \
    supersonic/utils/iterator_adaptors.h \
    supersonic/utils/endian.h \
    supersonic/utils/casts.h \
    supersonic/utils/int128.h \
    supersonic/utils/linked_ptr.h \
    supersonic/utils/logging-inl.h \
    supersonic/utils/macros.h \
    supersonic/utils/manual_constructor.h \
    supersonic/utils/map_util.h \
    supersonic/utils/mathlimits.h \
    supersonic/utils/paranoid.h \
    supersonic/utils/pointer_vector.h \
    supersonic/utils/port.h \
    supersonic/utils/scoped_ptr.h \
    supersonic/utils/scoped_ptr_internals.h \
    supersonic/utils/sparsetable.h \
    supersonic/utils/std_namespace.h \
    supersonic/utils/strtoint.h \
    supersonic/utils/stl_util.h \
    supersonic/utils/stringprintf.h \
    supersonic/utils/template_util.h \
    supersonic/utils/valgrind.h \
    supersonic/utils/hash/hash.h \
    supersonic/utils/exception/coowned_pointer.h \
    supersonic/utils/exception/failureor.h \
    supersonic/utils/exception/stack_trace.h \
    supersonic/utils/strings/ascii_ctype.h \
    supersonic/utils/strings/case.h \
    supersonic/utils/strings/charset.h \
    supersonic/utils/strings/fastmem.h \
    supersonic/utils/strings/join.h \
    supersonic/utils/strings/join_internal.h \
    supersonic/utils/strings/escaping.h \
    supersonic/utils/strings/human_readable.h \
    supersonic/utils/strings/memutil.h \
    supersonic/utils/strings/numbers.h \
    supersonic/utils/strings/serialize.h \
    supersonic/utils/strings/split.h \
    supersonic/utils/strings/split_internal.h \
    supersonic/utils/strings/strcat.h \
    supersonic/utils/strings/stringpiece.h \
    supersonic/utils/strings/substitute.h \
    supersonic/utils/strings/strip.h \
    supersonic/utils/strings/util.h \
    supersonic/utils/utf/utfdef.h \
    supersonic/utils/timer.h \
    supersonic/utils/random.h \
    supersonic/utils/walltime.h \
    supersonic/utils/file.h \
    supersonic/utils/file_util.h \
    supersonic/base/exception/exception_macros.h \
    supersonic/base/exception/exception.h \
    supersonic/base/exception/result.h \
    supersonic/base/infrastructure/aggregation_operators.h \
    supersonic/base/infrastructure/bit_pointers.h \
    supersonic/base/infrastructure/block.h \
    supersonic/base/infrastructure/copy_column.h \
    supersonic/base/infrastructure/double_buffered_block.h \
    supersonic/base/infrastructure/init.h \
    supersonic/base/infrastructure/operators.h \
    supersonic/base/infrastructure/projector.h \
    supersonic/base/infrastructure/tuple_schema.h \
    supersonic/base/infrastructure/types_infrastructure.h \
    supersonic/base/infrastructure/types.h \
    supersonic/base/infrastructure/variant_pointer.h \
    supersonic/base/infrastructure/variant.h \
    supersonic/base/infrastructure/view_copier.h \
    supersonic/base/memory/arena.h \
    supersonic/base/memory/memory.h \
    supersonic/cursor/base/cursor.h \
    supersonic/cursor/base/lookup_index.h \
    supersonic/cursor/base/operation.h \
    supersonic/cursor/base/cursor_transformer.h \
    supersonic/cursor/core/aggregate.h \
    supersonic/cursor/core/aggregator.h \
    supersonic/cursor/core/benchmarks.h \
    supersonic/cursor/core/coalesce.h \
    supersonic/cursor/core/column_aggregator.h \
    supersonic/cursor/core/compute.h \
    supersonic/cursor/core/filter.h \
    supersonic/cursor/core/foreign_filter.h \
    supersonic/cursor/core/generate.h \
    supersonic/cursor/core/hash_join.h \
    supersonic/cursor/core/hybrid_group_utils.h \
    supersonic/cursor/core/limit.h \
    supersonic/cursor/core/merge_union_all.h \
    supersonic/cursor/core/ownership_taker.h \
    supersonic/cursor/core/project.h \
    supersonic/cursor/core/rowid_merge_join.h \
    supersonic/cursor/core/scan_view.h \
    supersonic/cursor/core/sort.h \
    supersonic/cursor/core/specification_builder.h \
    supersonic/cursor/core/splitter.h \
    supersonic/cursor/core/spy.h \
    supersonic/cursor/infrastructure/basic_cursor.h \
    supersonic/cursor/infrastructure/basic_operation.h \
    supersonic/cursor/infrastructure/file_io.h \
    supersonic/cursor/infrastructure/file_io-internal.h \
    supersonic/cursor/infrastructure/history_transformer.h \
    supersonic/cursor/infrastructure/iterators.h \
    supersonic/cursor/infrastructure/ordering.h \
    supersonic/cursor/infrastructure/ownership_revoker.h \
    supersonic/cursor/infrastructure/row_copier.h \
    supersonic/cursor/infrastructure/row.h \
    supersonic/cursor/infrastructure/row_hash_set.h \
    supersonic/cursor/infrastructure/table.h \
    supersonic/cursor/infrastructure/value_ref.h \
    supersonic/cursor/infrastructure/view_cursor.h \
    supersonic/cursor/infrastructure/view_printer.h \
    supersonic/expression/base/expression.h \
    supersonic/expression/core/arithmetic_bound_expressions.h \
    supersonic/expression/core/arithmetic_expressions.h \
    supersonic/expression/core/comparison_bound_expressions.h \
    supersonic/expression/core/comparison_expressions.h \
    supersonic/expression/core/date_bound_expressions.h \
    supersonic/expression/core/date_evaluators.h \
    supersonic/expression/core/date_expressions.h \
    supersonic/expression/core/elementary_bound_expressions.h \
    supersonic/expression/core/elementary_expressions.h \
    supersonic/expression/core/math_bound_expressions.h \
    supersonic/expression/core/math_evaluators.h \
    supersonic/expression/core/math_expressions.h \
    supersonic/expression/core/projecting_bound_expressions.h \
    supersonic/expression/core/projecting_expressions.h \
    supersonic/expression/core/stateful_bound_expressions.h \
    supersonic/expression/core/stateful_expressions.h \
    supersonic/expression/core/string_bound_expressions.h \
    supersonic/expression/core/string_evaluators.h \
    supersonic/expression/core/string_expressions.h \
    supersonic/expression/ext/hashing/hashing_bound_expressions.h \
    supersonic/expression/ext/hashing/hashing_evaluators.h \
    supersonic/expression/ext/hashing/hashing_expressions.h \
    supersonic/expression/infrastructure/basic_bound_expression.h \
    supersonic/expression/infrastructure/basic_expressions.h \
    supersonic/expression/infrastructure/bound_expression_creators.h \
    supersonic/expression/infrastructure/elementary_bound_const_expressions.h \
    supersonic/expression/infrastructure/elementary_const_expressions.h \
    supersonic/expression/infrastructure/expression_utils.h \
    supersonic/expression/infrastructure/terminal_bound_expressions.h \
    supersonic/expression/infrastructure/terminal_expressions.h \
    supersonic/expression/templated/abstract_bound_expressions.h \
    supersonic/expression/templated/abstract_expressions.h \
    supersonic/expression/templated/bound_expression_factory.h \
    supersonic/expression/templated/cast_bound_expression.h \
    supersonic/expression/templated/cast_expression.h \
    supersonic/expression/vector/binary_column_computers.h \
    supersonic/expression/vector/column_validity_checkers.h \
    supersonic/expression/vector/expression_evaluators.h \
    supersonic/expression/vector/expression_traits.h \
    supersonic/expression/vector/simd_operators.h \
    supersonic/expression/vector/ternary_column_computers.h \
    supersonic/expression/vector/unary_column_computers.h \
    supersonic/expression/vector/vector_logic.h \
    supersonic/expression/vector/vector_primitives.h \
    supersonic/cursor/infrastructure/writer.h \
    supersonic/utils/utf/utf.h \
    supersonic/supersonic.h \
    supersonic/supersonic-config.h

nobase_supersonic_include_HEADERS = $(protos_H) $(sources_H)

noinst_HEADERS = config.h

supersonic_S =

# cpp sources
# protoc output
protos_S = \
    supersonic/utils/exception/stack_trace.pb.cc \
    supersonic/utils/proto/expressions.pb.cc \
    supersonic/utils/proto/types.pb.cc \
    supersonic/benchmark/proto/benchmark.pb.cc \
    supersonic/cursor/proto/cursors.pb.cc \
    supersonic/expression/proto/operators.pb.cc \
    supersonic/proto/specification.pb.cc \
    supersonic/proto/supersonic.pb.cc

supersonic_S += $(protos_S)

supersonic_S += \
    supersonic/utils/bits.cc \
    supersonic/utils/demangle.cc \
    supersonic/utils/int128.cc \
    supersonic/utils/mathlimits.cc \
    supersonic/utils/strtoint.cc \
    supersonic/utils/stringprintf.cc \
    supersonic/utils/hash/hash.cc \
    supersonic/utils/exception/stack_trace.cc \
    supersonic/utils/strings/case.cc \
    supersonic/utils/strings/join.cc \
    supersonic/utils/strings/stringpiece.cc \
    supersonic/utils/strings/ascii_ctype.cc \
    supersonic/utils/strings/charset.cc \
    supersonic/utils/strings/escaping.cc \
    supersonic/utils/strings/human_readable.cc \
    supersonic/utils/strings/memutil.cc \
    supersonic/utils/strings/numbers.cc \
    supersonic/utils/strings/serialize.cc \
    supersonic/utils/strings/split.cc \
    supersonic/utils/strings/strcat.cc \
    supersonic/utils/strings/substitute.cc \
    supersonic/utils/strings/strip.cc \
    supersonic/utils/strings/util.cc \
    supersonic/utils/utf/rune.c

supersonic_S += supersonic/utils/random.cc \
    supersonic/utils/walltime.cc \
    supersonic/utils/file.cc \
    supersonic/utils/file_util.cc

# base
supersonic_S += \
    supersonic/base/infrastructure/bit_pointers.cc \
    supersonic/base/infrastructure/block.cc \
    supersonic/base/infrastructure/copy_column.cc \
    supersonic/base/infrastructure/double_buffered_block.cc \
    supersonic/base/infrastructure/projector.cc \
    supersonic/base/infrastructure/tuple_schema.cc \
    supersonic/base/infrastructure/types_infrastructure.cc \
    supersonic/base/infrastructure/types.cc \
    supersonic/base/infrastructure/view_copier.cc \
    supersonic/base/memory/arena.cc \
    supersonic/base/memory/memory.cc

# cursor
supersonic_S += supersonic/cursor/base/cursor.cc \
    supersonic/cursor/base/lookup_index.cc \
    supersonic/cursor/core/aggregate_clusters.cc \
    supersonic/cursor/core/aggregate_groups.cc \
    supersonic/cursor/core/aggregate_scalar.cc \
    supersonic/cursor/core/aggregator.cc \
    supersonic/cursor/core/benchmarks.cc \
    supersonic/cursor/core/coalesce.cc \
    supersonic/cursor/core/column_aggregator.cc \
    supersonic/cursor/core/compute.cc \
    supersonic/cursor/core/filter.cc \
    supersonic/cursor/core/foreign_filter.cc \
    supersonic/cursor/core/generate.cc \
    supersonic/cursor/core/hash_join.cc \
    supersonic/cursor/core/hybrid_group_utils.cc \
    supersonic/cursor/core/limit.cc \
    supersonic/cursor/core/merge_union_all.cc \
    supersonic/cursor/core/project.cc \
    supersonic/cursor/core/rowid_merge_join.cc \
    supersonic/cursor/core/scan_view.cc \
    supersonic/cursor/core/sort.cc \
    supersonic/cursor/core/specification_builder.cc \
    supersonic/cursor/core/splitter.cc \
    supersonic/cursor/core/spy.cc \
    supersonic/cursor/infrastructure/basic_cursor.cc \
    supersonic/cursor/infrastructure/basic_operation.cc \
    supersonic/cursor/infrastructure/file_io.cc \
    supersonic/cursor/infrastructure/iterators.cc \
    supersonic/cursor/infrastructure/ordering.cc \
    supersonic/cursor/infrastructure/row_hash_set.cc \
    supersonic/cursor/infrastructure/table.cc \
    supersonic/cursor/infrastructure/view_cursor.cc \
    supersonic/cursor/infrastructure/view_printer.cc \
    supersonic/cursor/infrastructure/writer.cc

# expression
supersonic_S += supersonic/expression/base/expression.cc \
    supersonic/expression/core/arithmetic_bound_expressions.cc \
    supersonic/expression/core/arithmetic_expressions.cc \
    supersonic/expression/core/comparison_bound_expressions.cc \
    supersonic/expression/core/comparison_expressions.cc \
    supersonic/expression/core/date_bound_expressions.cc \
    supersonic/expression/core/date_evaluators.cc \
    supersonic/expression/core/date_expressions.cc \
    supersonic/expression/core/elementary_bound_expressions.cc \
    supersonic/expression/core/elementary_expressions.cc \
    supersonic/expression/core/math_bound_expressions.cc \
    supersonic/expression/core/math_expressions.cc \
    supersonic/expression/core/projecting_bound_expressions.cc \
    supersonic/expression/core/projecting_expressions.cc \
    supersonic/expression/core/stateful_bound_expressions.cc \
    supersonic/expression/core/stateful_expressions.cc \
    supersonic/expression/core/string_bound_expressions.cc \
    supersonic/expression/core/string_expressions.cc \
    supersonic/expression/ext/hashing/hashing_bound_expressions.cc \
    supersonic/expression/ext/hashing/hashing_expressions.cc \
    supersonic/expression/infrastructure/basic_bound_expression.cc \
    supersonic/expression/infrastructure/basic_expressions.cc \
    supersonic/expression/infrastructure/expression_utils.cc \
    supersonic/expression/infrastructure/terminal_bound_expressions.cc \
    supersonic/expression/infrastructure/terminal_expressions.cc \
    supersonic/expression/templated/bound_expression_factory.cc \
    supersonic/expression/templated/cast_bound_expression.cc \
    supersonic/expression/templated/cast_expression.cc \
    supersonic/expression/vector/vector_logic.cc

# Source list
libsupersonic_la_SOURCES = $(nobase_supersonic_include_HEADERS) \
                           $(supersonic_S)

if USE_RE2
SUPERSONIC_REGEXP_LIB=libsupersonic_regexp.la
else
SUPERSONIC_REGEXP_LIB=
endif

# Regexp - re2 dependent implementation
lib_LTLIBRARIES += $(SUPERSONIC_REGEXP_LIB)

libsupersonic_regexp_la_CXXFLAGS = $(COMPILER_FLAGS) $(RE2_FLAGS)
libsupersonic_regexp_la_LDFLAGS =
libsupersonic_regexp_la_LIBADD = $(RE2_LIBS)

libsupersonic_regexp_ladir = $(includedir)
nobase_libsupersonic_regexp_la_HEADERS = \
    supersonic/expression/core/regexp_evaluators.h \
    supersonic/expression/core/regexp_expressions.h \
    supersonic/expression/core/regexp_bound_expressions.h \
    supersonic/expression/core/regexp_bound_expressions_internal.h

libsupersonic_regexp_la_SOURCES = \
    supersonic/expression/core/regexp_bound_expressions.cc \
    supersonic/expression/core/regexp_expressions.cc

# Serialization
lib_LTLIBRARIES += libsupersonic_serialization.la

libsupersonic_serialization_la_CXXFLAGS = $(COMPILER_FLAGS)
libsupersonic_serialization_la_LDFLAGS =
libsupersonic_serialization_la_LIBADD = $(RE2_LIBS) libsupersonic.la

libsupersonic_serialization_ladir = $(includedir)
nobase_libsupersonic_serialization_la_HEADERS = \
    supersonic/serialization/build_expression_from_proto.h

libsupersonic_serialization_la_SOURCES = \
    supersonic/serialization/build_expression_from_proto.cc

# Run libtool
libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(builddir) $<

dist_noinst_DATA = $(protos) install_supersonic.sh
BUILT_SOURCES = $(protos_H)

# Remove all proto-generated source and header files.
MOSTLYCLEANFILES = $(protos_H) $(protos_S)
