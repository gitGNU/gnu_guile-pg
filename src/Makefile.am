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
schemesource = $(schemestems:=.scm)

EXTRA_DIST = $(schemesource)
BUILT_SOURCES = libpq.x

AM_CPPFLAGS = $(SUBCPPFLAGS)
AM_CFLAGS = $(AGGRESSIVE_WARNINGS) $(MORE_AGGRESSIVE_WARNINGS)
AM_LDFLAGS = $(SUBLDFLAGS)

ddir = $(GUILE_LIBSITE)/database
d_DATA = $(schemestems)

stamp-all: $(schemestems) $(d_LTLIBRARIES) $(pkglib_LTLIBRARIES)
	@touch $@
noinst_DATA = stamp-all

SUFFIXES = .scm

if USE_CMOD
d_LTLIBRARIES = postgres.la
else
pkglib_LTLIBRARIES = postgres.la
endif
postgres_la_SOURCES = libpq.c gi.h
postgres_la_LDFLAGS = -export-dynamic -module -version-info 0:0:0
postgres_la_LDFLAGS += $(AM_LDFLAGS)

if !USE_CMOD
d_DATA += postgres

postgres.exports: $(postgres_la_SOURCES)
	> $@ \
	$(SED) '/^(pg_/!d;s/^.*"\(.*\)".*/\1/' $^

postgres: postgres.la postgres.exports
	$(gx) gen-scheme-wrapper \
	  -o $@ $< -g database -t guile_pg_init_~A_module \
	  -i '$(pkglibdir)'
endif !USE_CMOD

snarfcppopts = $(DEFS) $(DEFAULT_INCLUDES) $(AM_CPPFLAGS) $(CPPFLAGS)

.c.x:
	$(gx) c2x -o $@ $< -- $(snarfcppopts) $(CFLAGS)

.scm:
	$(gx) punify -n $< > $@

if MAINTAINER_MODE

noinst_DATA += all.tsar

all.tsar: libpq.tsar scheme.tsar
	$(gx) tsar -f $@ -c utf-8 concat $^

libpq.tsar: libpq.c
	$(gx) c-tsar -f $@ -c utf-8 update $^ -- $(snarfcppopts)

scheme.tsar: $(schemesource)
	$(gx) tsar -f $@ -c utf-8 update $^

endif # MAINTAINER_MODE

update-libsite-module-catalog:
	$(GUILE_TOOLS) make-module-catalog \
	  -t '$(DESTDIR)' '$(DESTDIR)$(GUILE_LIBSITE)'

install-data-hook:
	@for f in $(schemestems) ; do					  \
	  g="$(ddir)/$$f" ;						  \
	  if [ -f "$(DESTDIR)$$g.scm" ] ; then				  \
	    echo "WARNING: previously installed $$g.scm may shadow $$f" ; \
	  fi ;								  \
	done
if USE_CMOD
	-rmdir '$(DESTDIR)$(pkglibdir)'
	$(top_srcdir)/build-aux/sofix $(SOFIXFLAGS) \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)
	$(MAKE) $(AM_MAKEFLAGS) update-libsite-module-catalog
else !USE_CMOD
	@if [ 1 = '$(FOR_MAKE_CHECK)' ] ; then		\
	  mv -f postgres postgres.STASH ;		\
	  $(MAKE) $(AM_MAKEFLAGS) postgres ;		\
	  $(INSTALL_DATA) postgres '$(ddir)/postgres' ;	\
	  mv -f postgres.STASH postgres ;		\
	fi
endif !USE_CMOD

uninstall-hook:
if USE_CMOD
	$(top_srcdir)/build-aux/uninstall-sofixed \
	  $(DESTDIR)$(ddir) $(d_LTLIBRARIES)
	$(MAKE) $(AM_MAKEFLAGS) update-libsite-module-catalog
endif USE_CMOD
	-rmdir $(ddir)
	-rmdir $(pkglibdir)

DISTCLEANFILES = *.x postgres.exports

CLEANFILES = *.x postgres stamp-all $(schemestems)

# Makefile.am ends here
