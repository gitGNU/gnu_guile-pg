# Makefile.am
#
# Copyright (C) 2008, 2009, 2010, 2011 Thien-Thi Nguyen
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

include $(top_srcdir)/build-aux/common.mk

schemefrags = resdisp types col-defs resx qcons gxrepl table meta
schemestems = $(schemefrags:%=postgres-%)

BUILT_SOURCES = libpq.x

AM_CFLAGS = $(AGGRESSIVE_WARNINGS) $(MORE_AGGRESSIVE_WARNINGS)

ddir = $(GUILE_LIBSITE)/database

dist_d_DATA = $(schemestems:=.scm)

d_LTLIBRARIES = postgres.la
postgres_la_SOURCES = libpq.c gi.h
postgres_la_LDFLAGS = -export-dynamic -module -version-info 0:0:0

snarfcppopts = $(DEFS) $(DEFAULT_INCLUDES) $(CPPFLAGS)

.c.x:
	$(gx) c2x -o $@ $< -- $(snarfcppopts) $(CFLAGS)

if MAINTAINER_MODE

noinst_DATA = all.tsar

all.tsar: libpq.tsar scheme.tsar
	$(gx) tsar -f $@ -c utf-8 concat $^

libpq.tsar: libpq.c
	$(gx) c-tsar -f $@ -c utf-8 update $^ -- $(snarfcppopts)

scheme.tsar: $(schemestems:=.scm)
	$(gx) tsar -f $@ -c utf-8 update $^

endif # MAINTAINER_MODE

install-data-hook:
	$(top_srcdir)/build-aux/sofix $(SOFIXFLAGS) \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)
	$(mkmodcat) '$(GUILE_LIBSITE)'

EXTRA_DIST = modsup.h

DISTCLEANFILES = *.x

CLEANFILES = *.x

# Makefile.am ends here
