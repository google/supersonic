m4_define([supersonic_major], [0])
m4_define([supersonic_minor], [9])
m4_define([supersonic_patch], [3])

AC_PREREQ([2.65])
AC_INIT([supersonic], [supersonic.major.supersonic_minor.supersonic_patch])
AC_CONFIG_SRCDIR([supersonic/base/exception/exception.h])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADER(config.h)

# Set forced failure checking to true.
AC_DEFINE([FORCE_FAILURE_CHECK], [1], [Force failure checking.])

# Additional (installable) config.h-like file with prefixed flags
# which makes it possible to make installation-dependent includes.
AX_PREFIX_CONFIG_H(supersonic/supersonic-config.h, SUPERSONIC)

# The gflags are used for automake, not GCC!
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

# Required by Automake 1.12, but not recognised in older versions.
m4_pattern_allow([AM_PROG_AR])
AM_PROG_AR

# Adding libtool support
LT_INIT
AC_PROG_LIBTOOL

AX_CHECK_COMPILE_FLAG([-funsigned-char], [CXXFLAGS="$CXXFLAGS -funsigned-char"])
AX_CHECK_COMPILE_FLAG([-O3], [CXXFLAGS="$CXXFLAGS -O3"])
AX_CHECK_COMPILE_FLAG([-mmmx], [CXXFLAGS="$CXXFLAGS -mmmx"])
AX_CHECK_COMPILE_FLAG([-msse], [CXXFLAGS="$CXXFLAGS -msse"])
AX_CHECK_COMPILE_FLAG([-msse2], [CXXFLAGS="$CXXFLAGS -msse2"])

# Checks for programs.
AC_LANG([C++])
AC_PROG_CXX
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CPP
AM_CONDITIONAL(GCC, test "$GCC" = yes)   # let the Makefile know if we're gcc

# Check for libraries.
PKG_CHECK_MODULES(GLOG, libglog)
PKG_CHECK_MODULES(GFLAGS, libgflags)
PKG_CHECK_MODULES(PROTO, protobuf)

# Check for boost.
AX_BOOST_BASE
AX_BOOST_SYSTEM
AX_BOOST_TIMER

# Checks for header files.
AC_CHECK_HEADERS([limits.h stddef.h stdint.h stdlib.h string.h string \
                  sys/stat.h sys/time.h])
AC_CHECK_HEADERS([glog/logging.h], [],
                 AC_MSG_ERROR([google-glog is required. See INSTALL file.]))
AC_CHECK_HEADERS([google/protobuf/text_format.h], [],
                 AC_MSG_ERROR([protobuf is required. See INSTALL file.]))
AC_CHECK_HEADERS([gflags/gflags.h], [],
                 AC_MSG_ERROR([gflags library is required. See INSTALL file.]))
AC_CHECK_HEADERS([re2/re2.h], [],
                 AC_MSG_ERROR([re2 library is required. See INSTALL file.]))

# A lot of the code in this directory depends on pthreads
ACX_PTHREAD

# We'd like to use read/write locks in several places in the code.
# See if our pthreads support extends to that.  Note: for linux, it
# does as long as you define _XOPEN_SOURCE appropriately.
AC_RWLOCK

# For mingw/cygwin, figure out if the mutex code needs to use
# 'volatile' in some places.  They differ from MSVC, and the API is
# unclear, so it's best just to check.
AC_INTERLOCKED_EXCHANGE_NONVOLATILE

# Check for protobuf compiler, but only if the path has not been specified
# manually.
if test -z $PROTOC;
then
  AC_PATH_PROG(PROTOC, protoc, [AC_MSG_ERROR([protoc was not found])])
fi

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([atexit floor localtime_r memset pow setenv sqrt strcasecmp \
strerror tzset])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
