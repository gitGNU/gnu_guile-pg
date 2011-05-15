#!/bin/sh
# Usage: sh -x autogen.sh [--libtoolize]
#
# Bootstrap the development environment.  Optional arg "--libtoolize" means
# also run "libtoolize --force".  The distribution was made w/ these tools:
# - autoconf (GNU Autoconf) 2.65
# - automake (GNU automake) 1.11.1
# - ltmain.sh (GNU libtool) 2.2.6b
# - Guile 1.4.1.119

######################################################################
# Local.

for f in sofix sofix.m4 ; do
    g=../.common/$f
    test -f $g || { echo ERROR: No such file: $g ; exit 1 ; }
    ( cd build-aux ; ln -sf ../$g $f )
done

#############################################################################
# Guile-BAUX

guile-baux-tool import \
    c2x \
    tsar \
    c-tsar \
    tsin \
    gen-scheme-wrapper \
    gbaux-do

#############################################################################
# Autotools

test x"$1" = x--libtoolize && libtoolize --force
test -r build-aux/ltmain.sh || libtoolize --force

aclocal -I build-aux
autoheader
autoconf
automake --add-missing --force -Wall

######################################################################
# Done.

: Now run configure and make.
: You must pass the "--enable-maintainer-mode" option to configure.

# autogen.sh ends here
