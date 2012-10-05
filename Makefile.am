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

BOOST_TIMER_LIB = -lboost_timer

RE2_LIBS = -lre2

# Library
lib_LTLIBRARIES = libsupersonic.la

libsupersonic_la_CXXFLAGS = $(PTHREAD_CFLAGS) \
    $(AM_CXXFLAGS) \
    $(GFLAGS_CFLAGS) \
    $(GLOG_CFLAGS) \
    $(PROTO_CFLAGS) \
    $(RE2_CFLAGS) \
    $(BOOST_CPPFLAGS)
libsupersonic_la_LDFLAGS =
libsupersonic_la_LIBADD =

# Unit tests and benchmarks.
supersonic_unittest_CPPFLAGS = $(AM_CPPFLAGS) \
    $(PTHREAD_CFLAGS) \
    -I$(top_srcdir)/gtest/include -I$(top_srcdir)/gtest \
    $(GFLAGS_CFLAGS) \
    $(GLOG_CFLAGS) \
    $(PROTO_CFLAGS) \
    $(RE2_CFLAGS) \
    $(BOOST_CPPFLAGS)
supersonic_unittest_LDFLAGS =
supersonic_unittest_LDADD = libsupersonic.la \
    $(BOOST_TIMER_LIB) \
    $(GFLAGS_LIBS) \
    $(GLOG_LIBS) \
    $(PROTO_LIBS) \
    $(RE2_LIBS)

supersonic_unittest_SOURCES = gtest/src/gtest-all.cc \
    test_utils/supersonic_test_main.cc \
    test/smoke_test.cc \
    test/guide/group_sort.cc \
    test/guide/join.cc \
    test/guide/primer.cc

TESTS = supersonic_unittest
check_PROGRAMS = $(TESTS)
noinst_PROGRAMS = $(TESTS)

EXTRA_DIST = $(TESTS)

# Headers exported from the library.
supersonic_includedir = $(includedir)

protos_H = \
    supersonic/utils/exception/stack_trace.pb.h \
    supersonic/utils/proto/expressions.pb.h \
    supersonic/utils/proto/types.pb.h \
    supersonic/cursor/proto/cursors.pb.h \
    supersonic/expression/proto/operators.pb.h \
    supersonic/proto/specification.pb.h \
    supersonic/proto/supersonic.pb.h

sources_H = supersonic/utils/algorithm.h \
    supersonic/utils/arm_instruction_set_select.h \
    supersonic/utils/atomicops.h \
    supersonic/utils/auxiliary/atomicops-internals-macosx.h \
    supersonic/utils/auxiliary/atomicops-internals-arm-v6plus.h \
    supersonic/utils/auxiliary/atomicops-internals-arm-generic.h \
    supersonic/utils/atomicops-internals-x86.h \
    supersonic/utils/atomicops-internals-powerpc.h \
    supersonic/utils/auxiliary/atomicops-internals-windows.h \
    supersonic/utils/atomic_refcount.h \
    supersonic/utils/basictypes.h \
    supersonic/utils/bits.h \
    supersonic/utils/charmap.h \
    supersonic/utils/container_literal.h \
    supersonic/utils/container_literal_generated.h \
    supersonic/utils/cycleclock-inl.h \
    supersonic/utils/demangle.h \
    supersonic/utils/dynamic_annotations.h \
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
    supersonic/utils/map-util.h \
    supersonic/utils/mathlimits.h \
    supersonic/utils/mutex.h \
    supersonic/utils/once.h \
    supersonic/utils/paranoid.h \
    supersonic/utils/pointer_vector.h \
    supersonic/utils/port.h \
    supersonic/utils/scoped_ptr.h \
    supersonic/utils/scoped_ptr_internals.h \
    supersonic/utils/singleton.h \
    supersonic/utils/shared_ptr.h \
    supersonic/utils/sparsetable.h \
    supersonic/utils/spinlock_linux-inl.h \
    supersonic/utils/spinlock_posix-inl.h \
    supersonic/utils/spinlock_win32-inl.h \
    supersonic/utils/spinlock_wait.h \
    supersonic/utils/strtoint.h \
    supersonic/utils/stl_util.h \
    supersonic/utils/stringprintf.h \
    supersonic/utils/template_util.h \
    supersonic/utils/thread_annotations.h \
    supersonic/utils/type_traits.h \
    supersonic/utils/valgrind.h \
    supersonic/utils/atomic/atomic.h \
    supersonic/utils/atomic/atomic_internal.h \
    supersonic/utils/atomic/atomic_std.h \
    supersonic/utils/atomic/forward.h \
    supersonic/utils/atomic/locking.h \
    supersonic/utils/atomic/tsan.h \
    supersonic/utils/atomic/x86-gcc.h \
    supersonic/utils/hash/builtin_type_hash.h \
    supersonic/utils/hash/city.h \
    supersonic/utils/hash/hash.h \
    supersonic/utils/hash/hash128to64.h \
    supersonic/utils/hash/jenkins.h \
    supersonic/utils/hash/jenkins_lookup2.h \
    supersonic/utils/hash/legacy_hash.h \
    supersonic/utils/hash/string_hash.h \
    supersonic/utils/exception/coowned_pointer.h \
    supersonic/utils/exception/failureor.h \
    supersonic/utils/exception/stack_trace.h \
    supersonic/utils/strings/fastmem.h \
    supersonic/utils/strings/join.h \
    supersonic/utils/strings/escaping.h \
    supersonic/utils/strings/memutil.h \
    supersonic/utils/strings/numbers.h \
    supersonic/utils/strings/split_internal.h \
    supersonic/utils/strings/strcat.h \
    supersonic/utils/strings/stringpiece.h \
    supersonic/utils/strings/substitute.h \
    supersonic/utils/strings/strip.h \
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
    supersonic/expression/core/string_bound_expressions_internal.h \
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
    supersonic/serialization/build_expression_from_proto.h \
    supersonic/expression/vector/vector_primitives.h \
    supersonic/cursor/infrastructure/writer.h \
    supersonic/utils/utf/utf.h \
    supersonic/supersonic.h \
    supersonic/supersonic-config.h

nobase_supersonic_include_HEADERS = $(protos_H) \
																		$(sources_H)

noinst_HEADERS = config.h

supersonic_S =

# cpp sources
# protoc output
protos_S = \
    supersonic/utils/exception/stack_trace.pb.cc \
    supersonic/utils/proto/expressions.pb.cc \
    supersonic/utils/proto/types.pb.cc \
    supersonic/cursor/proto/cursors.pb.cc \
    supersonic/expression/proto/operators.pb.cc \
    supersonic/proto/specification.pb.cc \
    supersonic/proto/supersonic.pb.cc

supersonic_S += $(protos_S)

supersonic_S += \
    supersonic/utils/atomicops-internals-x86.cc \
    supersonic/utils/bits.cc \
    supersonic/utils/demangle.cc \
    supersonic/utils/dynamic_annotations.c \
    supersonic/utils/int128.cc \
    supersonic/utils/mathlimits.cc \
    supersonic/utils/once.cc \
    supersonic/utils/spinlock_wait.cc \
    supersonic/utils/strtoint.cc \
    supersonic/utils/stringprintf.cc \
    supersonic/utils/atomic/locking.cc \
    supersonic/utils/atomic/x86-gcc.cc \
    supersonic/utils/hash/city.cc \
    supersonic/utils/hash/hash.cc \
    supersonic/utils/hash/jenkins.cc \
    supersonic/utils/exception/stack_trace.cc \
    supersonic/utils/strings/join.cc \
    supersonic/utils/strings/stringpiece.cc \
    supersonic/utils/strings/ascii_ctype.cc \
    supersonic/utils/strings/charset.cc \
    supersonic/utils/strings/escaping.cc \
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

# serialization
supersonic_S += supersonic/serialization/build_expression_from_proto.cc

# Source list
nodist_libsupersonic_la_SOURCES = $(nobase_supersonic_include_HEADERS) \
																	$(supersonic_S)

# Run libtool
libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

## Rules to compile protocol buffer sources
supersonic/utils/exception/stack_trace.pb.h supersonic/utils/exception/stack_trace.pb.cc: supersonic/utils/exception/stack_trace.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/expression/proto/operators.pb.h supersonic/expression/proto/operators.pb.cc: supersonic/expression/proto/operators.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/cursor/proto/cursors.pb.h supersonic/cursor/proto/cursors.pb.cc: supersonic/cursor/proto/cursors.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/proto/supersonic.pb.h supersonic/proto/supersonic.pb.cc: supersonic/proto/supersonic.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/proto/specification.pb.h supersonic/proto/specification.pb.cc: supersonic/proto/specification.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/utils/proto/types.pb.h supersonic/utils/proto/types.pb.cc: supersonic/utils/proto/types.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

supersonic/utils/proto/expressions.pb.h supersonic/utils/proto/expressions.pb.cc: supersonic/utils/proto/expressions.proto
	$(PROTOC) --proto_path=$(srcdir) --cpp_out=$(srcdir) $<

# Proto header dependencies
$(supersonic_S) $(sources_H): | $(protos_H)

# Remove all proto-generated source and header files.
CLEANFILES = $(protos_H) $(protos_S)
