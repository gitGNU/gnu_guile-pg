dnl configure.ac
dnl
dnl Copyright (C) 2002, 2003, 2004, 2005, 2006,
dnl   2007, 2008, 2009, 2010, 2011 Thien-Thi Nguyen
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

AC_CONFIG_HEADERS([src/config.h])

AM_MAINTAINER_MODE

AC_ARG_ENABLE([cmod],[AS_HELP_STRING([--disable-cmod],
 [do not install (database postgres) as a shared object library;
  use a Scheme wrapper that does the dynamic-link "manually"
  (default: install the module as a shared object library,
  but only if Guile seems capable of loading it directly)])],[],[
enable_cmod=yes
])

AC_ARG_ENABLE([pq-rpath],[AS_HELP_STRING([--enable-pq-rpath],
 [arrange to use "-R" when linking libpq])])

AC_DEFUN([PG_CHECK_DIR],
[AS_IF([test yes = $withval],
       [AC_MSG_ERROR([--with-$1 requires argument DIR])])])

AC_ARG_WITH([libpq],[AS_HELP_STRING([--with-libpq=DIR],
 [look for libpq headers in DIR/include and libpq.a in DIR/lib
  @<:@default=$prefix@:>@; see also --with-libpq-includes and
  --with-libpq-lib below])],[PG_CHECK_DIR([libpq])])

AC_ARG_WITH([libpq-includes],[AS_HELP_STRING([--with-libpq-includes=DIR],
 [look for libpq includes in DIR])],[PG_CHECK_DIR([libpq-includes])])

AC_ARG_WITH([libpq-lib],[AS_HELP_STRING([--with-libpq-lib=DIR],
 [look for libpq libraries in DIR])],[PG_CHECK_DIR([libpq-lib])])

LT_PREREQ([2.2.6])
LT_INIT([dlopen disable-static]) dnl module only

AC_PROG_CC

AC_HEADER_STDC

AC_CHECK_HEADERS([string.h stdio.h stdlib.h malloc.h unistd.h])

##----------------------------------------------------------------------------
## Guile

AC_MSG_NOTICE([checking Guile particulars])

GUILE_PROGS
GUILE_FLAGS
GUILE_LIBSITE_DIR

AS_IF([test xyes = x$enable_cmod],[
AC_CACHE_CHECK([for "guile-tools make-module-catalog"],[pg_cv_mkmodcat],[
if $GUILE_TOOLS make-module-catalog --version >/dev/null 2>&1
then pg_cv_mkmodcat=yes
else pg_cv_mkmodcat=no
fi
])])
AM_CONDITIONAL([USE_CMOD],
[test yes = $enable_cmod && test xyes = x"$pg_cv_mkmodcat"])

AM_COND_IF([USE_CMOD],[
  AC_DEFINE([USE_CMOD], 1,
     [Define to 1 if Guile can load C modules.])
  AC_MSG_NOTICE([will install C module directly])
],[
  AC_MSG_NOTICE([will install Scheme wrapper for shared object library])
])

saved_CPPFLAGS="$CPPFLAGS"	# restoration below
saved_LDFLAGS="$LDFLAGS"
CPPFLAGS="$GUILE_CFLAGS $CPPFLAGS"
LDFLAGS="$GUILE_LDFLAGS $LDFLAGS"

dnl Ancient guiles know ‘scm_terminating’ as simply ‘terminating’
dnl and don't even declare it.  So, we check for both.

AC_CHECK_DECLS([scm_terminating],,,[[#include <libguile.h>]])
AS_IF([test no = $ac_cv_have_decl_scm_terminating],[
 AC_CHECK_LIB([guile], [terminating],
 [AC_DEFINE([HAVE_LIBGUILE_TERMINATING], 1,
            [Define if libguile defines terminating.])])
])

AC_DEFUN([PG_CHECK_GUILE_HEADER],[
  AC_CHECK_HEADERS([guile/$1.h],,,[[
#include <libguile.h>
]$2])])

PG_CHECK_GUILE_HEADER([gh])
PG_CHECK_GUILE_HEADER([modsup],[[
#if HAVE_GUILE_GH_H
# include <guile/gh.h>
#endif
]])

CPPFLAGS="$saved_CPPFLAGS"
LDFLAGS="$saved_LDFLAGS"

##----------------------------------------------------------------------------
## PostgreSQL

AC_MSG_NOTICE([checking PostgreSQL particulars])

## This little hack allows the tests for libpq etc. to be overridden by
## setting the environment variables PQ_CPPFLAGS and PQ_LDFLAGS.  This is
## useful e.g. when running 'Make distcheck', which runs ./configure.

AC_DEFUN([PG_PQ_NOT_YET],
[AS_IF([test -z "$PQ_CPPFLAGS" && test -z "$PQ_LDFLAGS"],$1)])

PG_PQ_NOT_YET([AS_IF([test set = "${with_libpq+set}"],[
AC_CACHE_CHECK([for libpq-fe.h and libpq.a under $with_libpq],[pg_cv_w],
[AS_IF([test -f "${with_libpq}/include/libpq-fe.h" &&
        test -f "${with_libpq}/lib/libpq.a"],
[pg_cv_w=yes],
[pg_cv_w=no])])
AS_IF([test yes = $pg_cv_w],[
PQ_CPPFLAGS="-I${with_libpq}/include"
PQ_LDFLAGS="-L${with_libpq}/lib"
])])])

PG_PQ_NOT_YET([AS_IF([test set = ${with_libpq_includes+set} &&
                      test set = ${with_libpq_lib+set}],[
AC_CACHE_CHECK([for ${with_libpq_includes}/libpq-fe.h],[pg_cv_wi],
[AS_IF([test -f "${with_libpq_includes}/libpq-fe.h"],
[pg_cv_wi=yes],
[pg_cv_wi=no])])
AC_CACHE_CHECK([for ${with_libpq_lib}/libpq.a],[pg_cv_wl],
[AS_IF([test -f "${with_libpq_lib}/libpq.a"],
[pg_cv_wl=yes],
[pg_cv_wl=no])])
AS_IF([test yes = $pg_cv_wi && test yes = $pg_cv_wl],[
PQ_CPPFLAGS="-I${with_libpq_includes}"
PQ_LDFLAGS="-L${with_libpq_lib}"
])])])

PG_PQ_NOT_YET([
AC_MSG_FAILURE([Cannot find libpq header and lib; see README])
])

# Add rpath to link flags if requested by ‘--enable-pq-rpath’.
AS_IF([test yes = "$enable_pq_rpath"],[
more=`echo $PQ_LDFLAGS | sed s/^-L/-R/`
PQ_LDFLAGS="$more $PQ_LDFLAGS"
AS_UNSET([more])
])

# Finally!
PQ_LDFLAGS="$PQ_LDFLAGS -lpq"

saved_CPPFLAGS="$CPPFLAGS"                  # restoration below
saved_LDFLAGS="$LDFLAGS"
CPPFLAGS="$PQ_CPPFLAGS $CPPFLAGS"
LDFLAGS="$PQ_LDFLAGS $LDFLAGS"

AC_CHECK_DECLS([pg_encoding_to_char, pg_char_to_encoding],,,[
#include <libpq-fe.h>
])

# 8.0
AC_CHECK_FUNCS([PQserverVersion])

CPPFLAGS="$saved_CPPFLAGS"
LDFLAGS="$saved_LDFLAGS"

##----------------------------------------------------------------------------
## Compose ‘SUBCPPFLAGS’ and ‘SUBLDFLAGS’, whose names are deliberately
## without underscore (info "(automake) Flag Variables Ordering").

AS_IF([test "$PQ_CPPFLAGS" = "$GUILE_CFLAGS"],
      [SUBCPPFLAGS="$GUILE_CFLAGS"],
      [SUBCPPFLAGS="$PQ_CPPFLAGS $GUILE_CFLAGS"])
SUBLDFLAGS="$PQ_LDFLAGS $GUILE_LDFLAGS"
AC_SUBST([SUBCPPFLAGS])
AC_SUBST([SUBLDFLAGS])

AC_MSG_NOTICE([NOTE: combined cpp and linker flags (see README if wrong):
 SUBCPPFLAGS: $SUBCPPFLAGS
  SUBLDFLAGS: $SUBLDFLAGS])

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
  test/runtest
  test/show-platform
])

AC_OUTPUT

dnl configure.ac ends here
