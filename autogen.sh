#!/bin/sh
# Usage: sh -x autogen.sh
#
# The distribution was made w/ these tools:
#  - autoconf (GNU Autoconf) 2.68
#  - automake (GNU automake) 1.11.1
#  - libtool (GNU libtool) 2.4
#  - guile (Unofficial Guile) 1.4.1.122
#  - guile-baux-tool (Guile-BAUX) 20110515.1158.8d8c7ae

set -e

#############################################################################
# Guile-BAUX

guile-baux-tool import \
    c2x \
    tsar \
    c-tsar \
    tsin \
    gen-scheme-wrapper \
    punify \
    gbaux-do

#############################################################################
# Autotools

autoreconf --verbose --force --install --symlink --warnings=all

######################################################################
# Done.

: Now run configure and make.
: You must pass the "--enable-maintainer-mode" option to configure.

# autogen.sh ends here
