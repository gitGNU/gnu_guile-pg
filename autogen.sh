#!/bin/sh
# Usage: sh -x autogen.sh [--libtoolize]
#
# Bootstrap the development environment.  Optional arg "--libtoolize" means
# also run "libtoolize --force".  The distribution was made w/ these tools:
# - libtool 1.4.3
# - autoconf 2.53
# - automake 1.6.3

test x"$1" = x--libtoolize && libtoolize --force
test -f ltmain.sh || libtoolize --force
aclocal
autoheader
autoconf
automake --add-missing --force

# autogen.sh ends here
