dnl configure.ac
dnl
dnl Copyright (C) 2002, 2003, 2004, 2005,
dnl   2006, 2007, 2008, 2009, 2010 Thien-Thi Nguyen
dnl Portions Copyright (C) 1998 Ian Grant
dnl
dnl This file is part of Guile-PG.
dnl
dnl Guile-PG is free software; you can redistribute it and/or modify it
dnl under the terms of the GNU General Public License as published by
dnl the Free Software Foundation; either version 3, or (at your
dnl option) any later version.
dnl
dnl Guile-PG is distributed in the hope that it will be useful, but
dnl WITHOUT ANY WARRANTY; without even the implied warranty of
dnl MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
dnl General Public License for more details.
dnl
dnl You should have received a copy of the GNU General Public License
dnl along with Guile-PG; see the file COPYING.  If not, write to the
dnl Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
dnl Boston, MA  02110-1301  USA

AC_INIT([Guile-PG],[0.39],[ttn@gnuvola.org])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([1.11 -Wall])

AM_CONFIG_HEADER([src/config.h])

AM_MAINTAINER_MODE

LT_PREREQ([2.2.6])
LT_INIT([dlopen disable-static]) dnl module only

AC_PROG_CC

AC_STDC_HEADERS

AC_HAVE_HEADERS(string.h stdio.h stdlib.h malloc.h unistd.h)

##----------------------------------------------------------------------------
## Guile

AC_MSG_NOTICE([checking Guile particulars])

GUILE_PROGS
GUILE_FLAGS

GUILE_SITE_DIR
GUILE_LIBSITE_DIR

GUILE_TOOLS_PROG([mkpimmc],[mkpimmc])
GUILE_TOOLS_PROG([mkmodcat],[make-module-catalog])

saved_CPPFLAGS="$CPPFLAGS"	# restoration below
saved_LDFLAGS="$LDFLAGS"
CPPFLAGS="$GUILE_CFLAGS $CPPFLAGS"
LDFLAGS="$GUILE_LDFLAGS $LDFLAGS"

AC_GUILE_PG_BCOMPAT
AC_GUILE_PG_FCOMPAT

CPPFLAGS="$saved_CPPFLAGS"
LDFLAGS="$saved_LDFLAGS"

##----------------------------------------------------------------------------
## PostgreSQL

AC_MSG_NOTICE([checking PostgreSQL particulars])

## This little hack allows the tests for libpq etc. to be overridden by
## setting the environment variables PQ_CPPFLAGS and PQ_LDFLAGS.  This is
## useful e.g. when running 'Make distcheck', which runs ./configure.

if test "x$PQ_LDFLAGS" = "x" && test "x$PQ_CPPFLAGS" = "x" ; then
PQ_FLAGS
fi

saved_CPPFLAGS="$CPPFLAGS"                  # restoration below
saved_LDFLAGS="$LDFLAGS"
CPPFLAGS="$PQ_CPPFLAGS $CPPFLAGS"
LDFLAGS="$PQ_LDFLAGS $LDFLAGS"

AC_CHECK_DECLS([pg_encoding_to_char, pg_char_to_encoding])

# 8.0
AC_CHECK_FUNCS([PQserverVersion])

CPPFLAGS="$saved_CPPFLAGS"
LDFLAGS="$saved_LDFLAGS"

##----------------------------------------------------------------------------
## Compose CPPFLAGS, LDFLAGS

if test x"$PQ_CPPFLAGS" = x"$GUILE_CFLAGS" ; then
  CPPFLAGS="$GUILE_CFLAGS"
else
  CPPFLAGS="$PQ_CPPFLAGS $GUILE_CFLAGS"
fi
LDFLAGS="$PQ_LDFLAGS $GUILE_LDFLAGS"

AC_MSG_NOTICE([NOTE: preprocessor and linker flags (see README if wrong):
 CPPFLAGS: $CPPFLAGS
  LDFLAGS: $LDFLAGS])

##----------------------------------------------------------------------------
## Etc

SET_SOFIXFLAGS
AC_PROG_INSTALL
AC_PROG_LN_S

## If we're using GCC, ask for aggressive warnings.
if test x"$GCC" = xyes ; then
  AGGRESSIVE_WARNINGS="-std=gnu99 -pedantic"
  for x in all extra float-equal declaration-after-statement \
           undef shadow pointer-arith cast-qual cast-align aggregate-return \
           old-style-definition no-missing-field-initializers \
           nested-externs inline volatile-register-var disabled-optimization
    do AGGRESSIVE_WARNINGS="$AGGRESSIVE_WARNINGS -W$x"
  done
fi
AC_SUBST([AGGRESSIVE_WARNINGS])

AC_CONFIG_FILES([
  Makefile
  src/Makefile
  doc/Makefile
  test/Makefile
])

AC_GUILE_PG_CONFIG_SCRIPT([test/runtest])

AC_CONFIG_COMMANDS([twerp-prep],[
  if echo "$ac_cs_version" | grep -q options.*--enable-maintainer-mode ; then
    if guile-tools | grep -q prep-Ptexi ; then
      guile-tools prep-Ptexi doc/Makefile `cat $srcdir/doc/.twerp-list`
    else
      AC_MSG_WARN([[automatic doc rebuild not supported; you need to]])
      AC_MSG_WARN([[edit doc/guile-pg.texi manually if source changes]])
    fi
  fi
])

GUILE_MODULE_CATALOG_PREP

AC_OUTPUT

dnl configure.ac ends here