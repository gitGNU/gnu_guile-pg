## Process this file with automake to produce Makefile.in
##
##   Copyright (C) 2008 Thien-Thi Nguyen
##
##   This file is part of Guile-PG.
##
##   Guile-PG is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as
##   published by the Free Software Foundation; either version 3, or
##   (at your option) any later version.
##
##   Guile-PG is distributed in the hope that it will be useful, but
##   WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public
##   License along with Guile-PG; see the file COPYING.  If not,
##   write to the Free Software Foundation, Inc., 51 Franklin Street,
##   Fifth Floor, Boston, MA  02110-1301  USA

BUILT_SOURCES = libpq.x libpq.doc

AM_CPPFLAGS = -I. -I$(srcdir) $(GUILE_CFLAGS) $(PQ_CPPFLAGS)

ddir = $(GUILE_LIBSITE)/database
d_LTLIBRARIES = postgres.la
postgres_la_SOURCES = libpq.c tmpfile.c bcruft.h fcruft.h
postgres_la_LDFLAGS = -export-dynamic -module -version-info 0:0:0 \
				$(LDFLAGS) $(PQ_LDFLAGS)

SUFFIXES = .x .doc

snarfcppopts = $(DEFS) $(AM_CPPFLAGS) $(CPPFLAGS)

.c.x:
	$(c2x) -o $@ $< $(snarfcppopts) $(CFLAGS)
.c.doc:
	$(GUILE_TOOLS) c2doc -o $@ $< -- $(snarfcppopts)

install-data-hook:
	rm -f $(ddir)/postgres-sup.*
	$(top_srcdir)/build-aux/sofix $(SOFIXFLAGS) \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)

EXTRA_DIST = modsup.h

DISTCLEANFILES = *.x *.doc

CLEANFILES = *.x

## Makefile.am ends here
