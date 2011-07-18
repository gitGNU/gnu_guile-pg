## guile-tools-existsp.m4
##
## Copyright (C) 2011 Thien-Thi Nguyen
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

dnl GUILE_TOOLS_EXISTSP(CACHE-VAR,PROGRAM)
dnl
dnl Check if "guile-tools" lists PROGRAM.  If so, set
dnl shell variable CACHE-VAR to "yes", otherwise "no".
dnl
AC_DEFUN([GUILE_TOOLS_EXISTSP],[
AC_REQUIRE([GUILE_PROGS])
AC_CACHE_CHECK([for "guile-tools $2"],[$1],
[AS_IF([$GUILE_TOOLS | grep "^$2$" 1>/dev/null 2>&1],[$1=yes],[$1=no])])
])])

## guile-tools-existsp.m4 ends here
