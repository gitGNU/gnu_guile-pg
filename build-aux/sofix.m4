## sofix.m4                                             -*-autoconf-*-

## Copyright (C) 2007 Thien-Thi Nguyen
##
## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 3, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public
## License along with this program; see the file COPYING.  If not,
## write to the Free Software Foundation, Inc., 51 Franklin
## Street, Fifth Floor, Boston, MA 02110-1301 USA

AC_DEFUN([SET_SOFIXFLAGS],[
# If Guile supports module catalogs, arrange
# to tidy up the compiled modules installation.
AC_MSG_CHECKING([sofix flags])
if $GUILE_TOOLS | grep -q make-module-catalog
then SOFIXFLAGS=no-la,no-symlinks
else SOFIXFLAGS=ln-s-lib
fi
AC_MSG_RESULT([$SOFIXFLAGS])
AC_SUBST(SOFIXFLAGS)
])

## sofix.m4 ends here
