dnl $Id$
dnl   Automake macros for working with PostgreSQL libPQ.
dnl   
dnl   	Copyright (C) 1998 Free Software Foundation, Inc.
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

dnl   PQ_FLAGS --- set flags for compiling and linking with libpq
dnl 
dnl   This macro looks for the libpq-fe.h and libpq.a files installed
dnl   with PostgreSQL.  It sets three variables, marked for substitution, as
dnl   by AC_SUBST.
dnl
dnl	PQ_CFLAGS --- flags to pass to a C or C++ compiler to build
dnl		code that uses libpq header files.  This is almost
dnl		always just a -I flag.
dnl
dnl     PQ_LDFLAGS --- flags to pass to the linker to link a
dnl		program against libpq.  This includes `-lpq' for
dnl		the pq library itself. It may also include a -L 
dnl             flag to tell the compiler where to find the libraries

AC_DEFUN([PQ_FLAGS],[
  AC_ARG_WITH(libpq,
[  --with-libpq=DIR        look for libpq includes in DIR/include and
                          libpq.a in DIR/lib (default=/usr/local)
                             See also --with-libpq-includes and 
                          --with-libpq-lib below],
   [AC_MSG_CHECKING("$withval/include/libpq-fe.h exists")
    if test ! -f "$withval/include/libpq-fe.h" ; then
      AC_MSG_RESULT(no)
      AC_ERROR("$withval/include/libpq-fe.h does not exist.")
    else
      AC_MSG_RESULT(yes)
    fi
    AC_MSG_CHECKING("$withval/lib/libpq.a exists")
    if test ! -f "$withval/lib/libpq.a" ; then
      AC_MSG_RESULT(no)
      AC_ERROR("$withval/include/libpq.a does not exist.")
    else
      AC_MSG_RESULT(yes)
    fi
    PQ_CFLAGS="-I$withval/include"
    PQ_LDFLAGS="-L$withval/lib -lpq"
   ],[
    AC_ARG_WITH(libpq-includes,
[  --with-libpq-includes=DIR    look for libpq includes in DIR ],[
       if test ! -f "$withval/libpq-fe.h" ; then
        AC_ERROR("$withval/libpq-fe.h does not exist.")
       fi
       PQ_CFLAGS="-I$withval"],[
       AC_CHECK_HEADERS(libpq-fe.h)
       if test ! "$ac_cv_header_libpq_fe_h" = yes ; then
         AC_ERROR("Cannot build without libpq-fe.h. Use --with-libpq-includes ?")
       fi
    ])
    AC_ARG_WITH(libpq-lib,
[  --with-libpq-lib=DIR    look for libpq libraries in DIR ],[
       AC_MSG_CHECKING("$withval/libpq.a exists")
       if test ! -f "$withval/libpq.a" ; then
        AC_MSG_RESULT(no)
        AC_ERROR("$withval/libpq.a does not exist.")
       else
        AC_MSG_RESULT(yes)
       fi
       PQ_LDFLAGS="-L$withval -lpq"],[
       AC_CHECK_LIB(pq,PQsetdbLogin)
       if test ! "$ac_cv_lib_pq_PQsetdbLogin" = yes ; then
         AC_ERROR("Cannot build without libpq. Use --with-libpq-lib ?")
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


dnl   Automake macros for working with Guile.
dnl   
dnl   	Copyright (C) 1998 Free Software Foundation, Inc.
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
dnl   As a special exception, the Free Software Foundation gives permission
dnl   for additional uses of the text contained in its release of GUILE.
dnl   
dnl   The exception is that, if you link the GUILE library with other files
dnl   to produce an executable, this does not by itself cause the
dnl   resulting executable to be covered by the GNU General Public License.
dnl   Your use of that executable is in no way restricted on account of
dnl   linking the GUILE library code into it.
dnl   
dnl   This exception does not however invalidate any other reasons why
dnl   the executable file might be covered by the GNU General Public License.
dnl   
dnl   This exception applies only to the code released by the
dnl   Free Software Foundation under the name GUILE.  If you copy
dnl   code from other Free Software Foundation releases into a copy of
dnl   GUILE, as the General Public License permits, the exception does
dnl   not apply to the code that you add in this way.  To avoid misleading
dnl   anyone as to the status of such modified files, you must delete
dnl   this exception notice from them.
dnl   
dnl   If you write modifications of your own for GUILE, it is your choice
dnl   whether to permit this exception to apply to your modifications.
dnl   If you do not wish that, delete this exception notice.


dnl   GUILE_MODULE --- set flags for compiling and linking with Guile
dnl                    and set libdir and datadir to point to the place
dnl                    at which to install shared libraries and scheme
dnl                    modules.
dnl 
dnl   This macro runs the `guile-config' script, installed with Guile,
dnl   to find out where Guile's header files and libraries are
dnl   installed.  It sets two variables, marked for substitution, as
dnl   by AC_SUBST.
dnl
dnl	GUILE_CFLAGS --- flags to pass to a C or C++ compiler to build
dnl		code that uses Guile header files.  This is almost
dnl		always just a -I flag.
dnl
dnl     GUILE_LDFLAGS --- flags to pass to the linker to link a
dnl		program against Guile.  This includes `-lguile' for
dnl		the Guile library itself, any libraries that Guile
dnl		itself requires (like -lqthreads), and so on.  It may
dnl		also include a -L flag to tell the compiler where to
dnl		find the libraries.
dnl
dnl     datadir --- The path to the data directory in which scheme
dnl                 modules live
dnl
dnl     libdir --- The path to the directory in which libguile lives

AC_DEFUN([GUILE_MODULE],[
  ## First, let's just see if we can find Guile at all.
  AC_CACHE_CHECK("for guile and guile-config", guile_cv_installed_guile,
  [guile_cv_installed_guile=no
  guile-config link > /dev/null 2>&1 && guile_cv_installed_guile=yes])
  if test $guile_cv_installed_guile = no ; then
    AC_MSG_ERROR(cannot find guile-config. Is Guile 1.3 or later installed?)
  fi
  AC_CACHE_CHECK("Guile cc flags", guile_cv_flags_cflags,
        guile_cv_flags_cflags="`guile-config compile`")
  GUILE_CFLAGS="$guile_cv_flags_cflags"
  AC_SUBST(GUILE_CFLAGS)
  AC_CACHE_CHECK("Guile ld flags", guile_cv_flags_ldflags,
        guile_cv_flags_ldflags="`guile-config link`")
  GUILE_LDFLAGS="$guile_cv_flags_ldflags"
  AC_SUBST(GUILE_LDFLAGS)
  if test "x$exec_prefix" = xNONE -a "x$prefix" = xNONE ; then
      AC_CACHE_CHECK("Guile lib directory", guile_cv_path_libdir,
                      [guile_cv_path_libdir="`guile-config info libdir`"])
      libdir="$guile_cv_path_libdir"
  fi
  if test "x$prefix" = xNONE ; then
    AC_CACHE_CHECK("Guile module directory", guile_cv_path_datadir,
                   [guile_cv_path_datadir="`guile-config info datadir`"])
    datadir="$guile_cv_path_datadir"
  fi
])
