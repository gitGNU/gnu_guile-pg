#!/bin/sh
# Usage: sh -x autogen.sh [--libtoolize]
#
# Bootstrap the development environment.  Optional arg "--libtoolize" means
# also run "libtoolize --force".  The distribution was made w/ these tools:
# - libtool 1.5.6 (1.1220.2.94 2004/04/10 16:27:27)
# - autoconf 2.59
# - automake 1.7.6
# - guile 1.4.1.98

#############################################################################
# Autotools

test x"$1" = x--libtoolize && libtoolize --force
test -f ltmain.sh || libtoolize --force

# guile.m4.snap is from unreleased guile 1.4.1.99, so for now we use it
# explicitly; after guile 1.4.1.99 is released, this part should be rewritten
# in similar style to the modsup.h wrangling below.
fresh_guile_m4="guile.m4.snap"
ln -sf $fresh_guile_m4 guile.m4
aclocal -I .
# voodoo to avoid ridiculous C++ and FORTRAN probing
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

fresh_modsup_h="`guile-config info includedir`/guile/modsup.h"
test -f $fresh_modsup_h || fresh_modsup_h="modsup.h.snap"
ln -sf $fresh_modsup_h modsup.h
diff -u modsup.h.snap modsup.h > TMP 2>&1
if [ -s TMP ] ; then
    echo "WARNING: modsup.h.snap out of date, diff follows:"
    cat TMP
fi
rm -f TMP

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
