## determine-guile-libsite-dir.m4
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

dnl DETERMINE_GUILE_LIBSITE_DIR(CACHE-VAR-PREFIX)
dnl
dnl Use Guile-BAUX program ‘re-prefixed-site-dirs’ to set shell-variable
dnl CACHE-VAR-PREFIX_cv_minstroot, which is subsequently also copied to
dnl var ‘GUILE_LIBSITE’, marked for substitution, as by ‘AC_SUBST’.
dnl
AC_DEFUN([DETERMINE_GUILE_LIBSITE_DIR],[
AC_REQUIRE([GUILE_PROGS])
AC_CACHE_CHECK([module installation root dir],[$1_cv_minstroot],[
eval `GUILE="$GUILE" \
      $ac_aux_dir/guile-baux/gbaux-do \
      re-prefixed-site-dirs "$GUILE_CONFIG" $1`
])
GUILE_LIBSITE="$][$1_cv_minstroot"
AC_SUBST([GUILE_LIBSITE])
])])

## determine-guile-libsite-dir.m4 ends here
