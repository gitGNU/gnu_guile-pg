# Makefile.am
#
# Copyright (C) 2008, 2009 Thien-Thi Nguyen
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

schemefrags = resdisp types col-defs resx qcons gxrepl table meta
schemestems = $(schemefrags:%=postgres-%)

dotdocfiles = libpq.doc $(schemestems:%=%.doc)

BUILT_SOURCES = libpq.x

AM_CFLAGS = $(AGGRESSIVE_WARNINGS) $(MORE_AGGRESSIVE_WARNINGS)

ddir = $(GUILE_LIBSITE)/database

dist_d_DATA = $(schemestems:=.scm)

d_LTLIBRARIES = postgres.la
postgres_la_SOURCES = libpq.c gi.h
postgres_la_LDFLAGS = -export-dynamic -module -version-info 0:0:0

SUFFIXES = .x .doc

snarfcppopts = $(DEFS) $(DEFAULT_INCLUDES) $(CPPFLAGS)

.c.x:
	$(c2x) -o $@ $< $(snarfcppopts) $(CFLAGS)
.c.doc:
	$(GUILE_TOOLS) c2doc -o $@ $< -- $(snarfcppopts)
.scm.doc:
	$(GUILE_TOOLS) doc-snarf -D -o $@ $<

if MAINTAINER_MODE

BUILT_SOURCES += $(dotdocfiles)

.doc-index: $(dotdocfiles)
	$(GUILE_TOOLS) make-twerp2texi-index -o $@ $(dotdocfiles)

noinst_DATA = .doc-index

endif # MAINTAINER_MODE

retired = $(GUILE_SITE)/database

install-data-hook:
	rm -f $(ddir)/postgres-sup.*
	$(top_srcdir)/build-aux/sofix $(SOFIXFLAGS) \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)
	if [ -d "$(retired)" ] ; then		\
	  cd "$(retired)" ;			\
	  rm -f postgres.scm $(dist_d_DATA) ;	\
	  if [ "`ls 2>/dev/null`" = "" ] ; then	\
	    cd .. ; rmdir "$(retired)" ; fi ;	\
	  $(mkmodcat) "$(GUILE_SITE)" ;		\
	fi

EXTRA_DIST = modsup.h

DISTCLEANFILES = *.x *.doc

CLEANFILES = *.x

# Makefile.am ends here
