dnl   -*- autoconf -*- macros for Guile-PG
dnl
dnl	Copyright (C) 1998 Ian Grant
dnl	Copyright (C) 2002, 2003, 2004 Thien-Thi Nguyen
dnl
dnl   This program is free software; you can redistribute it and/or modify
dnl   it under the terms of the GNU General Public License as published by
dnl   the Free Software Foundation; either version 2, or (at your option)
dnl   any later version.
dnl
dnl   This program is distributed in the hope that it will be useful,
dnl   but WITHOUT ANY WARRANTY; without even the implied warranty of
dnl   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
dnl   GNU General Public License for more details.
dnl
dnl   You should have received a copy of the GNU General Public License
dnl   along with this software; see the file COPYING.  If not, write to
dnl   the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
dnl   Boston, MA 02111-1307 USA
dnl

# PQ_FLAGS --- set flags for compiling and linking with libpq
#
# This macro looks for the libpq-fe.h and libpq.a files installed
# with PostgreSQL.  It sets two variables, marked for substitution,
# as by AC_SUBST.
#
#   PQ_CFLAGS --- flags to pass to a C or C++ compiler to build
#                 code that uses libpq header files.  This is almost
#                 always just a -I flag.
#
#   PQ_LDFLAGS --- flags to pass to the linker to link a
#                  program against libpq.  This includes `-lpq' for
#                  the pq library itself. It may also include a -L
#                  flag to tell the compiler where to find the libraries
#
AC_DEFUN([PQ_FLAGS],[
  AC_ARG_WITH(libpq,
[  --with-libpq=DIR        look for libpq includes in DIR/include and
                          libpq.a in DIR/lib (default=/usr/local)
                             See also --with-libpq-includes and
                          --with-libpq-lib below],
   [AC_MSG_CHECKING([$withval/include/libpq-fe.h exists])
    if test ! -f "$withval/include/libpq-fe.h" ; then
      AC_MSG_RESULT(no)
      AC_ERROR([$withval/include/libpq-fe.h does not exist.])
    else
      AC_MSG_RESULT(yes)
    fi
    AC_MSG_CHECKING([$withval/lib/libpq.a exists])
    if test ! -f "$withval/lib/libpq.a" ; then
      AC_MSG_RESULT(no)
      AC_ERROR([$withval/include/libpq.a does not exist.])
    else
      AC_MSG_RESULT(yes)
    fi
    PQ_CFLAGS="-I$withval/include"
    PQ_LDFLAGS="-L$withval/lib -lpq"
   ],[
    AC_ARG_WITH(libpq-includes,
[  --with-libpq-includes=DIR    look for libpq includes in DIR ],[
       if test ! -f "$withval/libpq-fe.h" ; then
        AC_ERROR([$withval/libpq-fe.h does not exist.])
       fi
       PQ_CFLAGS="-I$withval"],[
       AC_CHECK_HEADERS(libpq-fe.h)
       if test ! "$ac_cv_header_libpq_fe_h" = yes ; then
         AC_ERROR([Cannot find libpq-fe.h; try "--with-libpq-includes=DIR"])
       fi
    ])
    AC_ARG_WITH(libpq-lib,
[  --with-libpq-lib=DIR    look for libpq libraries in DIR ],[
       AC_MSG_CHECKING([$withval/libpq.a exists])
       if test ! -f "$withval/libpq.a" ; then
        AC_MSG_RESULT(no)
        AC_ERROR([$withval/libpq.a does not exist.])
       else
        AC_MSG_RESULT(yes)
       fi
       PQ_LDFLAGS="-L$withval -lpq"],[
       AC_CHECK_LIB(pq,PQsetdbLogin)
       if test ! "$ac_cv_lib_pq_PQsetdbLogin" = yes ; then
         AC_ERROR([Cannot find libpq; try "--with-libpq-lib=DIR"])
       fi
       PQ_LDFLAGS="-lpq"
    ])
  ])
  AC_CHECK_LIB(crypt, crypt)
  if test "$ac_cv_lib_crypt" = yes ; then
     PQ_LDFLAGS="$PQ_LDFLAGS -lcrypt"
  fi
  AC_SUBST(PQ_CFLAGS)
  AC_SUBST(PQ_LDFLAGS)
])


# AC_GUILE_PG_FCOMPAT --- figure out some "forward compatability" cruft
#
AC_DEFUN([AC_GUILE_PG_FCOMPAT],[

AC_CHECK_DECL([scm_gc_protect_object],[
 AC_DEFINE([HAVE_SCM_GC_PROTECT_OBJECT], [1],
   [Define if libguile provides the scm_gc_protect_object function.])
],,[#include "libguile.h"])

])

dnl acinclude.m4 ends here
