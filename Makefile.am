## Process this file with automake to produce Makefile.in
##
##      Copyright (C) 1998-2000, 2002 Free Software Foundation, Inc.
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

SUBDIRS = scm doc test

EXTRA_DIST = acinclude.m4 TODO HACKING autogen.sh

BUILT_SOURCES = libpostgres.x libpostgres_lo.x

INCLUDES = -I. $(GUILE_CFLAGS) $(PQ_CFLAGS)

pkglibversdir = $(pkglibdir)/$(VERSION)

pkglibvers_LTLIBRARIES = libpostgres.la
libpostgres_la_SOURCES = libpostgres.c libpostgres_lo.c libpostgres.h \
                         libpostgres.x libpostgres_lo.x
libpostgres_la_LDFLAGS = -export-dynamic -version-info 2:0:0 \
                         @LDFLAGS@ @PQ_LDFLAGS@

SUFFIXES = .x
.c.x:
	guile-snarf -o $@ $< $(DEFS) $(INCLUDES) $(CPPFLAGS) $(CFLAGS)

## Add -MG to make the .x magic work with auto-dep code.
MKDEP = gcc -M -MG $(DEFS) $(INCLUDES) $(CPPFLAGS) $(CFLAGS)

DISTCLEANFILES = *.x

CLEANFILES = *.x

## Makefile.am ends here
