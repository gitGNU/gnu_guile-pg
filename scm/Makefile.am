## Process this file with automake to produce Makefile.in.
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
##
##   guile-pg was written by Ian grant <Ian.Grant@cl.cam.ac.uk>
##
AUTOMAKE_OPTIONS = foreign

# These should be installed and distributed.
guile_postgres_sources =

# These should be installed, but not distributed.
guile_postgres_generated = postgres.scm

subpkgdatadir = $(datadir)/guile/database
subpkgdata_DATA = $(guile_postgres_sources) $(guile_postgres_generated)
ETAGS_ARGS = $(subpkgdata_DATA)

## guile-pg-test.scm and testing.scm are not currently installed.
EXTRA_DIST = postgres.scm.in

