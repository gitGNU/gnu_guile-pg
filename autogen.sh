#! /bin/sh
# $Id$
aclocal -I.
libtoolize --copy --automake
autoheader
autoconf
automake --add-missing

