#!/bin/sh
# Usage: sh -x autogen.sh [--libtoolize]
#
# Bootstrap the development environment.  Optional arg "--libtoolize" means
# also run "libtoolize --force".  The distribution was made w/ these tools:
# - libtool 1.5.6 (1.1220.2.94 2004/04/10 16:27:27)
# - autoconf 2.59
# - automake 1.7.6

#############################################################################
# Autotools

test x"$1" = x--libtoolize && libtoolize --force
test -f ltmain.sh || libtoolize --force
aclocal -I `guile-config info datadir`/aclocal
( echo ; echo 'AC_DEFUN([_LT_AC_TAGCONFIG],[])' ) >> aclocal.m4
autoheader
autoconf
# prep automake
if [ ! -f doc/guile-pg.texi ] ; then
    echo '@setfilename guile-pg.info' > doc/guile-pg.texi
    touch -m -t 01010000 doc/guile-pg.texi
fi
automake --add-missing --force

#############################################################################
# Make local automake frags.

for script in `find . -name .make-automake-frags` ; do
    ( cd `dirname $script` ; ./.make-automake-frags )
done

######################################################################
# Header: <guile/modsup.h>

ln -sf `guile-config info includedir`/guile/modsup.h

######################################################################
# Self knowledge.

if [ -d CVS ] ; then
    # release tags all look like v-X-Y
    cvs log -h autogen.sh \
        | sed -n '/^[^a-zA-Z]v-/{s/^.//;s/:.*//;p;q;}' \
        > .last-release
fi

######################################################################
# Done.

: Now run configure and make.
: You must pass the "--enable-maintainer-mode" option to configure.

# autogen.sh ends here
