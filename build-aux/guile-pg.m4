# guile-pg.m4 --- some -*-autoconf-*- macros for Guile-PG
#
# Copyright (C) 2002, 2003, 2004, 2005, 2006,
#   2008, 2009, 2011 Thien-Thi Nguyen
# Portions Copyright (C) 1998 Ian Grant
#
# This file is part of Guile-PG.
#
# Guile-PG is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3, or (at your option)
# any later version.
#
# Guile-PG is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Guile-PG; see the file COPYING.  If not, write to
# the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301  USA
##

# PQ_FLAGS --- set flags for compiling and linking with libpq
#
# This macro looks for the libpq-fe.h and libpq.a files installed
# with PostgreSQL.  It sets two variables, marked for substitution,
# as by AC_SUBST.
#
#   PQ_CPPFLAGS --- flags to pass to a C or C++ compiler to build
#                   code that uses libpq header files.  This is almost
#                   always just a -I flag.
#
#   PQ_LDFLAGS --- flags to pass to the linker to link a
#                  program against libpq.  This includes ‘-lpq’ for
#                  the pq library itself. It may also include a -L
#                  flag to tell the compiler where to find the libraries
#
# This whole approach is rather ungainly and needs to be rethought. --ttn
#
AC_DEFUN([PQ_FLAGS],[
  AC_ARG_ENABLE([pq-rpath], AS_HELP_STRING([--enable-pq-rpath],
                              [arrange to use "-R" when linking libpq]))
  AC_ARG_WITH([libpq], AS_HELP_STRING([--with-libpq=DIR],
   [look for libpq headers in DIR/include and libpq.a in DIR/lib
    @<:@default=$prefix@:>@; see also --with-libpq-includes and
    --with-libpq-lib below]),
   [AC_MSG_CHECKING([$withval/include/libpq-fe.h exists])
    if test ! -f "$withval/include/libpq-fe.h" ; then
      AC_MSG_RESULT(no)
      AC_MSG_ERROR([$withval/include/libpq-fe.h does not exist.])
    else
      AC_MSG_RESULT(yes)
    fi
    AC_MSG_CHECKING([$withval/lib/libpq.a exists])
    if test ! -f "$withval/lib/libpq.a" ; then
      AC_MSG_RESULT(no)
      AC_MSG_ERROR([$withval/include/libpq.a does not exist.])
    else
      AC_MSG_RESULT(yes)
    fi
    PQ_CPPFLAGS="-I$withval/include"
    PQ_LDFLAGS="-L$withval/lib -lpq"
   ],[
    AC_ARG_WITH([libpq-includes], AS_HELP_STRING([--with-libpq-includes=DIR],
                                    [look for libpq includes in DIR]),[
       if test ! -f "$withval/libpq-fe.h" ; then
         AC_MSG_ERROR([$withval/libpq-fe.h does not exist.])
       fi
       PQ_CPPFLAGS="-I$withval"],[
       saved_CPPFLAGS="$CPPFLAGS"
       CPPFLAGS="-I$prefix/include"
       AC_CHECK_HEADERS(libpq-fe.h)
       CPPFLAGS="$saved_CPPFLAGS"
       if test ! "$ac_cv_header_libpq_fe_h" = yes ; then
         AC_MSG_ERROR([Cannot find libpq-fe.h; try "--with-libpq-includes=DIR"])
       fi
       PQ_CPPFLAGS="-I$prefix/include"
    ])
    AC_ARG_WITH([libpq-lib], AS_HELP_STRING([--with-libpq-lib=DIR],
                               [look for libpq libraries in DIR]),[
       AC_MSG_CHECKING([$withval/libpq.a exists])
       if test ! -f "$withval/libpq.a" ; then
         AC_MSG_RESULT(no)
         AC_MSG_ERROR([$withval/libpq.a does not exist.])
       else
         AC_MSG_RESULT(yes)
       fi
       PQ_LDFLAGS="-L$withval -lpq"],[
       saved_LDFLAGS="$LDFLAGS"
       LDFLAGS="-L$prefix/lib"
       AC_CHECK_LIB([pq],[PQprotocolVersion])
       LDFLAGS="$saved_LDFLAGS"
       if test ! "$ac_cv_lib_pq_PQprotocolVersion" = yes ; then
         AC_MSG_ERROR([Cannot find libpq that supports Protocol 3.0 or later;
                       try "--with-libpq-lib=DIR"])
       fi
       PQ_LDFLAGS="-L$prefix/lib -lpq"
    ])
  ])
  # add rpath to link flags if requested by --enable-pq-rpath;
  # note: the regexp passed to sed relies on a trailing space,
  # which means that removing "-lpq" from PQ_LDFLAGS will break it
  if test "$enable_pq_rpath" = yes ; then
    PQ_LDFLAGS="`echo $PQ_LDFLAGS | sed 's/-L\(@<:@^ @:>@* \)/-R\1-L\1/'`"
  fi
  AC_SUBST(PQ_CPPFLAGS)
  AC_SUBST(PQ_LDFLAGS)
])

# guile-pg.m4 ends here
