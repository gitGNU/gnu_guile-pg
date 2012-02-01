# Makefile.am
#
# Copyright (C) 2002, 2003, 2004, 2005, 2006,
#   2008, 2009, 2011 Thien-Thi Nguyen
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
# You should have received a copy of the GNU General Public
# License along with Guile-PG; see the file COPYING.  If not,
# write to the Free Software Foundation, Inc., 51 Franklin Street,
# Fifth Floor, Boston, MA  02110-1301  USA

CTL = $(srcdir)/fake-cluster-control

TESTS_ENVIRONMENT = sh runtest

TESTS = alive.scm basic.scm large.scm types-table.scm meta.scm

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
	@$(CTL) 0

kill-daemon:
	$(CTL) z

clean-local: kill-daemon
	rm -rf fake-cluster
	-rm -rf fake-install

# Makefile.am ends here
