## Process this file with Automake to create Makefile.in
##
##	Copyright (C) 1998, 2002, 2003 Free Software Foundation, Inc.
##
##   This file is part of GUILE-PG.
##
##   GUILE-PG is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as
##   published by the Free Software Foundation; either version 2, or
##   (at your option) any later version.
##
##   GUILE is distributed in the hope that it will be useful, but
##   WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public
##   License along with GUILE-PG; see the file COPYING.  If not, write
##   to the Free Software Foundation, Inc., 59 Temple Place, Suite
##   330, Boston, MA 02111-1307 USA

TESTS_ENVIRONMENT =                             \
  top_builddir=$(top_builddir_absolute)         \
  top_srcdir=$(top_srcdir)                      \
  srcdir=$(srcdir)

TESTS = alive.sh                                \
        basic-tests.sh                          \
        lo-tests.sh                             \
        abstraction-scm-tests.sh

CLEANFILES = basic-tests.log                    \
             lo-tests.log                       \
             abstraction-scm-tests.log          \
             lo-tests-data-1                    \
             lo-tests-data-2                    \
             test-lo-seek.log

EXTRA_DIST = $(TESTS)                           \
             testing.scm                        \
             guile-pg-basic-tests.scm           \
             guile-pg-lo-tests.scm              \
             guile-pg-abstraction-scm-tests.scm \
             show-platform                      \
             cov

dist-hook:
	if [ -x $(srcdir)/cov ] ; then \
	  ( cd $(srcdir) && ./cov ) > $(distdir)/test.coverage ; fi

## Makefile.am ends here
