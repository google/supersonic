#! /bin/sh -e

# Check if gtest is available in main directory,
# otherwise attempt to download it.
if test ! -e gtest; then
  echo "Google Test not present.  Fetching gtest-1.6.0 from the web..."
  wget http://googletest.googlecode.com/files/gtest-1.6.0.zip
  unzip gtest-1.6.0.zip
  rm gtest-1.6.0.zip
  mv gtest-1.6.0 gtest
fi

rm -rf autom4te.cache

aclocal -I m4
libtoolize --copy
autoconf
autoheader
automake --copy --add-missing

rm -rf autom4te.cache
