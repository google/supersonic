#!/bin/bash
set -o errexit
set -o nounset

mkdir -p $1
PARAM_DIR=${1:-"/usr/local"}
mkdir -p ${PARAM_DIR}

# In this way we acquire the absolute path to the $PARAM_DIR.
TARGET_DIR=$(cd ${PARAM_DIR}; pwd)

#======================== Installing dependencies =============================

# Temporary directory where the Supersonic dependencies will be
# downloaded and build in.
DEPS_DIR=./dependencies
mkdir -p ${DEPS_DIR}
cd ${DEPS_DIR}

WITH_RE2=true

# There are problems with compiling and linking against RE2 on OSX.
if [[ `uname` == 'Darwin' ]]; then
echo "Disabling 're2' on OSX"
WITH_RE2=false
fi

# Supersonic requires C++0x compatible compiler, we build the dependencies in that
# mode as well.
export CXXFLAGS="--std=c++0x"

#
# a) glog
#

if [ ! -d "glog-0.3.3" ]; then
curl -O https://google-glog.googlecode.com/files/glog-0.3.3.tar.gz && \
tar zxf glog-0.3.3.tar.gz && \
rm glog-0.3.3.tar.gz
fi
(
cd glog-0.3.3
#   Some compilers does not provide ext/slist. We replace it with C++0x standarized
#   <forward_list>.
sed -i -e 's|include <ext/slist>|include <forward_list>|g' src/glog/stl_logging.h.in
sed -i -e 's|__gnu_cxx::slist|std::forward_list|g' src/glog/stl_logging.h.in
./configure --prefix=$TARGET_DIR
make -j 16
make install
)
#
# b) gflags
#
if [ ! -d "gflags-2.0" ]; then
curl -O http://gflags.googlecode.com/files/gflags-2.0.tar.gz && \
tar zxf gflags-2.0.tar.gz && \
rm gflags-2.0.tar.gz
fi
(
cd gflags-2.0
#   C++11 requires a space between literal and symbol for automatic
#   concatenation.
sed -i -e 's|"%"PRI|"%" PRI|g' src/gflags.cc
./configure --prefix=$TARGET_DIR
make -j 16
make install
)
#
# c) protobuf
#
if [ ! -d "protobuf-2.5.0" ]; then
curl -O http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz && \
tar zxf protobuf-2.5.0.tar.gz && \
rm protobuf-2.5.0.tar.gz
fi
(
cd protobuf-2.5.0
./configure --prefix=$TARGET_DIR
rm -rf src/*.la  # In those files might be stored path from the previous run
make -j 16
make install
)
#
# d) re2
#
if $WITH_RE2 ; then
if [ ! -d "re2" ]; then
curl -O https://re2.googlecode.com/files/re2-20131024.tgz && \
tar -xf re2-20131024.tgz && \
rm re2-20131024.tgz && \
cp re2/Makefile re2/Makefile.original
fi
(
cd re2
sed -e "s|prefix=/usr/local|prefix=$TARGET_DIR|g" Makefile.original \
| sed -e "s|\(CXXFLAGS=-Wall -O3 -g -pthread \)|\1 -std=c++0x -DUSE_CXX0X|g" > Makefile
make install
)
fi
#
#
# 3) Go back to supersonic
#
cd ..
#
#
# 4) Export environment variables before configuring the package (only for
# downloaded packages).
#
export CPPFLAGS="${CPPFLAGS:-} -I${TARGET_DIR}/include"
OPTS=""

# a) glog
#
export GLOG_CFLAGS="-I${TARGET_DIR}/include"
export GLOG_LIBS="-L${TARGET_DIR}/lib -lglog"
#
# b) gflags
#
export GFLAGS_CFLAGS="-I${TARGET_DIR}/include"
export GFLAGS_LIBS="-L${TARGET_DIR}/lib -lgflags"
#
# c) protobuf
#
export PROTO_CFLAGS="-I${TARGET_DIR}/include"
export PROTO_LIBS="-L/${TARGET_DIR}/lib -lprotobuf"
export PROTOC="${TARGET_DIR}/bin/protoc"
#
# d) re2
#
if $WITH_RE2 ; then
export LDFLAGS="${LDFLAGS:-} -L${TARGET_DIR}/lib -Wl,-rpath -Wl,${TARGET_DIR}/lib"
export CPPFLAGS="${CPPFLAGS} -I${TARGET_DIR}/include"
else
OPTS="${OPTS} --without-re2"
fi
#
#
# 5) And finally...
#

./configure ${OPTS} --prefix=${TARGET_DIR} && make clean && make -j 16 && make check && make install
