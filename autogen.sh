#! /bin/sh -e

# Check if gmock is available in main directory,
# otherwise attempt to download it.
if test ! -e gmock; then
  echo "Google Mock not present.  Fetching gmock-1.6.0 from the web..."
  echo "Google Test version 1.6.0 will be there too."
  wget http://googlemock.googlecode.com/files/gmock-1.6.0.zip
  unzip gmock-1.6.0.zip
  rm gmock-1.6.0.zip
  mv gmock-1.6.0 gmock
fi

rm -rf autom4te.cache

aclocal -I m4
libtoolize --copy
autoconf
autoheader
automake --copy --add-missing

rm -rf autom4te.cache
