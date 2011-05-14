# Makefile.am
#
# Copyright (C) 2002, 2003, 2004, 2005,
#   2006, 2007, 2008, 2009 Thien-Thi Nguyen
# Portions Copyright (C) 1998,1999,2000 Ian Grant
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

SUBDIRS = src test doc

EXTRA_DIST = build-aux/sofix build-aux/guile-baux

dist-hook:
	sed -i 's/RECIP/$(PACKAGE_BUGREPORT)/;s/VERSION/$(VERSION)/' \
	  $(distdir)/test/show-platform

# Makefile.am ends here
