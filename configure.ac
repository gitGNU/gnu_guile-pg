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

GUILE_SITE_DIR
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

AC_CACHE_CHECK([for fill_input in struct scm_ptob_descriptor],
               [ac_cv_struct_fill_input],[
  AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <libguile.h>
]],[[struct scm_ptob_descriptor s; s.fill_input;]])],
                 [ac_cv_struct_fill_input=yes],
                 [ac_cv_struct_fill_input=no])
])
if test $ac_cv_struct_fill_input = no; then
  AC_MSG_ERROR([guile struct scm_ptob_descriptor lacks member fill_input])
fi

## Older guiles know "scm_terminating" as simply "terminating" and don't
## declare it.  So, we check for both.  See libpostgres_lo.c comments.

AC_CHECK_DECL([scm_terminating],[
                AC_DEFINE(HAVE_SCM_TERMINATING, 1,
                          [Define if libguile.h declares scm_terminating.])
              ],,[[#include "libguile.h"]])

AC_CHECK_LIB(guile, terminating, [
  AC_DEFINE(HAVE_LIBGUILE_TERMINATING, 1,
            [Define if libguile defines terminating.])
])

GUILE_MODSUP_H

AC_CHECK_DECL([scm_gc_protect_object],[
 AC_DEFINE([HAVE_SCM_GC_PROTECT_OBJECT], [1],
   [Define if libguile provides the scm_gc_protect_object function.])
],,[#include "libguile.h"])

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

AC_GUILE_PG_CONFIG_SCRIPT([test/show-platform])
AC_GUILE_PG_CONFIG_SCRIPT([test/runtest])

AC_OUTPUT

dnl configure.ac ends here
