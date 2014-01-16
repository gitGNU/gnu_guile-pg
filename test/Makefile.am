# Makefile.am
#
# Copyright (C) 2002, 2003, 2004, 2005, 2006,
#   2008, 2009, 2011, 2012, 2014 Thien-Thi Nguyen
# Portions Copyright (C) 1998 Ian Grant
#
# This file is part of Guile-PG.
#
# Guile-PG is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3, or
# (at your option) any later version.
#
# Guile-PG is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Guile-PG.  If not, see <http://www.gnu.org/licenses/>.

CTL = $(srcdir)/fake-cluster-control

TESTS_ENVIRONMENT = sh runtest

TESTS = alive.scm basic.scm large.scm types-table.scm meta.scm

XFAIL_TESTS =

CLEANFILES = $(TESTS:.scm=.log)                 \
             lo-tests-data-1                    \
             lo-tests-data-2                    \
             test-lo-seek.log

EXTRA_DIST = $(TESTS)                           \
             OK                                 \
             testing.scm                        \
             show-platform

check_DATA = fake-install

fake-install: ../src/stamp-all
	top=`pwd`/fake-install ;		\
	rm -rf $$top ;				\
	cd ../src ;				\
	$(MAKE) $(AM_MAKEFLAGS) install		\
	  FOR_MAKE_CHECK=1			\
	  prefix="$$top"

EXTRA_DIST += fake-cluster-control
check_DATA += fake-cluster/postmaster.pid
CLEANFILES += fake-cluster.log

fake-cluster/postmaster.pid:
	@$(CTL) 1
	@if [ x1 = x'$(DEBUG)' ] ; then $(CTL) q ; fi

check-local:
	@test x1 = 'x$(KEEPD)' || $(CTL) 0

kill-daemon:
	$(CTL) 0

delete-cluster: kill-daemon
	rm -rf fake-cluster

clean-local: delete-cluster
	-rm -rf fake-install

installcheck-local:
	$(MAKE) check prefix='$(prefix)'

# Makefile.am ends here
