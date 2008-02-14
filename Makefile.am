## Process this file with automake to produce Makefile.in
##
##   Copyright (C) 2002,2003,2004,2005,2006,2007,2008 Thien-Thi Nguyen
##   Portions Copyright (C) 1998,1999,2000 Ian Grant
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

SUBDIRS = scm test doc

EXTRA_DIST = THANKS TODO HACKING \
		build-aux/sofix \
		modsup.h autogen.sh

BUILT_SOURCES = libpostgres.x libpostgres_lo.x

AM_CPPFLAGS = -I. -I$(srcdir) $(GUILE_CFLAGS) $(PQ_CPPFLAGS)

ddir = $(GUILE_LIBSITE)/database
d_LTLIBRARIES = postgres.la
postgres_la_SOURCES = libpostgres.c libpostgres_lo.c libpostgres.h \
				tmpfile.c bcruft.h fcruft.h
postgres_la_LDFLAGS = -export-dynamic -module -version-info 0:0:0 \
				$(LDFLAGS) $(PQ_LDFLAGS)

noinst_DATA = .module-catalog

.module-catalog:
	if guile-tools | grep -q mkpimmc ; then			\
	  guile-tools mkpimmc -d $(DEPDIR) $@ "$(srcdir)"	\
	    -- -d in,lo,o -X '~$$' -X '[.]git' -X doc ;		\
	else echo '()' > $@ ; fi

SUFFIXES = .x .doc

snarfcppopts = $(DEFS) $(AM_CPPFLAGS) $(CPPFLAGS)

.c.x:
	$(c2x) -o $@ $< $(snarfcppopts) $(CFLAGS)
.c.doc:
	guile-tools c2doc -o $@ $< -- $(snarfcppopts)
.scm.doc:
	guile-tools doc-snarf -D -o $@ $<

install-data-hook:
	rm -f $(ddir)/postgres-sup.*
	$(srcdir)/build-aux/sofix $(SOFIXFLAGS) \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)
	if guile-tools | grep -q make-module-catalog ; then \
	   guile-tools make-module-catalog $(GUILE_LIBSITE) ; fi

DISTCLEANFILES = *.x *.doc

CLEANFILES = *.x $(noinst_DATA)

if MAINTAINER_MODE

DOTDOCFILES = libpostgres.doc                   \
              libpostgres_lo.doc                \
              scm/postgres-resdisp.doc	        \
              scm/postgres-types.doc            \
              scm/postgres-col-defs.doc         \
              scm/postgres-resx.doc             \
              scm/postgres-qcons.doc            \
              scm/postgres-gxrepl.doc           \
              scm/postgres-table.doc            \
              scm/postgres-meta.doc

.doc-index: $(DOTDOCFILES)
	guile-tools make-twerp2texi-index -o $@ $(DOTDOCFILES)

endif # MAINTAINER_MODE

## bonus target
destroy-nonmodule-guile-pg-prior-install:
	dir="$(libdir)/guile-pg" ;				\
	if test -d $$dir ; then					\
	  echo NOTE: deleting $$dir ;				\
	  rm -rf $$dir ; fi ;					\
	dir="$(datadir)/guile/database" ;			\
	if test -d $$dir ; then cd $$dir ;			\
	  files="`ls postgres*.scm 2>/dev/null`" ;		\
	  if test -n "$$files" ; then				\
	    for f in $$files ; do				\
	      echo NOTE: deleting $$dir/$$f ;			\
	      rm -f $$f ; done ; fi ;				\
	  if test "`ls`" = "" ; then cd .. ;			\
	    echo NOTE: deleting $$dir ;				\
	    rmdir database ; fi ; fi ;				\
	dir="$(datadir)/guile" ; cat=".module-catalog" ;	\
	if test -f $$dir/$$cat ; then cd $$dir ;		\
	  echo NOTE: modifying $$dir/$$cat ;			\
	  sed '/database postgres/d' $$cat > TMP ;		\
	  mv TMP $$cat ; fi

## Makefile.am ends here
