## $Id$
## Process this file with automake to produce Makefile.in
##
##      Copyright (C) 1998-2000 Free Software Foundation, Inc.
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

EXTRA_DIST = acinclude.m4 TODO HACKING

BUILT_SOURCES = libpostgres.x libpostgres_lo.x

bin_PROGRAMS = guile-pg

guile_pg_SOURCES = guile-pg.c guile-pg.h
guile_pg_LDADD = libpostgres.la
guile_pg_CFLAGS = @CFLAGS@ @GUILE_CFLAGS@ @PQ_CFLAGS@
guile_pg_LDFLAGS = @LDFLAGS@ @GUILE_LDFLAGS@ @PQ_LDFLAGS@ -static

INCLUDES = $(GUILE_CFLAGS) $(PQ_CFLAGS)

lib_LTLIBRARIES = libpostgres.la
libpostgres_la_SOURCES = libpostgres.c libpostgres_lo.c libpostgres.h \
                         libpostgres.x libpostgres_lo.x
libpostgres_la_LDFLAGS = -export-dynamic -version-info 1:0:0 \
                         @LDFLAGS@ @PQ_LDFLAGS@

SUFFIXES = .x
.c.x:
	guile-snarf $(DEFS) $(INCLUDES) $(CPPFLAGS) $(CFLAGS) $< > $@

## Add -MG to make the .x magic work with auto-dep code.
MKDEP = gcc -M -MG $(DEFS) $(INCLUDES) $(CPPFLAGS) $(CFLAGS)

DISTCLEANFILES = *.x
