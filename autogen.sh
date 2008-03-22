#!/bin/sh
# Usage: sh -x autogen.sh [--libtoolize]
#
# Bootstrap the development environment.  Optional arg "--libtoolize" means
# also run "libtoolize --force".  The distribution was made w/ these tools:
# - GNU Libtool 1.5.24 (1.1220.2.455 2007/06/24 02:13:29)
# - GNU Autoconf 2.61
# - GNU Automake 1.9.6
# - (Unofficial) Guile 1.4.1.112

######################################################################
# Local.

for f in sofix sofix.m4 ; do
    g=../.common/$f
    test -f $g || { echo ERROR: No such file: $g ; exit 1 ; }
    ( cd build-aux ; ln -sf ../$g $f )
done

#############################################################################
# Autotools (except automake)

test x"$1" = x--libtoolize && libtoolize --force
test -f ltmain.sh || libtoolize --force

fresh_guile_m4="`guile-config info datadir`/aclocal/guile.m4"
cd build-aux
test -f $fresh_guile_m4 || fresh_guile_m4="guile.m4.snap"
ln -sf $fresh_guile_m4 guile.m4
diff -u guile.m4.snap guile.m4 > TMP 2>&1
if [ -s TMP ] ; then
    echo "WARNING: guile.m4.snap out of date, diff follows:"
    cat TMP
fi
rm -f TMP
cd ..

aclocal -I build-aux --output=- | sed '$rbuild-aux/aclocal-suffix' > aclocal.m4

autoheader
autoconf

#############################################################################
# Automake

# Make local automake frags and do other automake prep.
for script in `find . -name .make-automake-frags` ; do
    ( cd `dirname $script` ; ./.make-automake-frags )
done

# Do it.
ln -sf ../.common/GPLv3 COPYING
automake --add-missing --force

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
# Done.

: Now run configure and make.
: You must pass the "--enable-maintainer-mode" option to configure.

# autogen.sh ends here
