## Process this file with automake to produce Makefile.in.

##      Copyright (C) 1998-2000,02,03,2004 Free Software Foundation, Inc.
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

##   Author: Ian grant <Ian.Grant@cl.cam.ac.uk>

AUTOMAKE_OPTIONS = foreign

# These files should be installed and distributed.
guile_postgres_sources = 			\
  postgres-types.scm 				\
  postgres-col-defs.scm				\
  postgres-resx.scm				\
  postgres-table.scm				\
  postgres-meta.scm

# These should be installed, but not distributed.
guile_postgres_generated = postgres.scm

ddir = $(GUILE_SITE)/database
d_DATA = $(guile_postgres_sources) $(guile_postgres_generated)

ETAGS_ARGS = $(subpkgdata_DATA)

EXTRA_DIST = postgres.scm.in $(guile_postgres_sources)

install-data-hook:
	if guile-tools | grep -q make-module-catalog ; then \
	   guile-tools make-module-catalog $(GUILE_SITE) ; fi

## Makefile.am ends here
