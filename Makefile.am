## Process this file with automake to produce Makefile.in
##
##      Copyright (C) 1998-2000, 2002, 2003 Free Software Foundation, Inc.
##
##   This file is part of GUILE-PG.
##
##   GUILE-PG is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as
##   published by the Free Software Foundation; either version 2, or
##   (at your option) any later version.
##
##   GUILE-PG is distributed in the hope that it will be useful, but
##   WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public
##   License along with GUILE-PG; see the file COPYING.  If not, write
##   to the Free Software Foundation, Inc., 59 Temple Place, Suite
##   330, Boston, MA 02111-1307 USA

SUBDIRS = scm test doc

EXTRA_DIST = THANKS contrib acinclude.m4 TODO HACKING autogen.sh

BUILT_SOURCES = libpostgres.x libpostgres_lo.x

INCLUDES = -I. $(GUILE_CFLAGS) $(PQ_CFLAGS)

pkglibversdir = $(pkglibdir)/$(VERSION)

pkglibvers_LTLIBRARIES = libpostgres.la
libpostgres_la_SOURCES = libpostgres.c libpostgres_lo.c libpostgres.h \
                         libpostgres.x libpostgres_lo.x
libpostgres_la_LDFLAGS = -export-dynamic -version-info 4:0:2 \
                         @LDFLAGS@ @PQ_LDFLAGS@

SUFFIXES = .x .doc

snarfcppopts = $(DEFS) $(INCLUDES) $(CPPFLAGS)

.c.x:
	guile-snarf -o $@ $< $(snarfcppopts) $(CFLAGS)
.c.doc:
	$(GUILE_TOOLS) c2doc -o $@ $< -- $(snarfcppopts)
.scm.doc:
	$(GUILE_TOOLS) doc-snarf -D -o $@ $<

## Add -MG to make the .x magic work with auto-dep code.
MKDEP = gcc -M -MG $(snarfcppopts) $(CFLAGS)

DISTCLEANFILES = *.x *.doc

CLEANFILES = *.x

dist-hook:
	rm -rf `find $(distdir)/contrib -name CVS`

if MAINTAINER_MODE

DOTDOCFILES = libpostgres.doc                   \
              libpostgres_lo.doc                \
              scm/postgres-types.doc            \
              scm/postgres-col-defs.doc         \
              scm/postgres-resx.doc             \
              scm/postgres-table.doc            \
              scm/postgres-meta.doc

.doc-index: $(DOTDOCFILES)
	guile-tools twerp2texi --make-index -o $@ $(DOTDOCFILES)

endif # MAINTAINER_MODE

## Makefile.am ends here
