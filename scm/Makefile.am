## Process this file with automake to produce Makefile.in.

##	Copyright (C) 1998,1999,2000 Ian Grant
##	Copyright (C) 2002,2003,2004 Thien-Thi Nguyen
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

AUTOMAKE_OPTIONS = foreign

guile_postgres_sources = 			\
  postgres-types.scm 				\
  postgres-col-defs.scm				\
  postgres-resx.scm				\
  postgres-table.scm				\
  postgres-meta.scm

ddir = $(GUILE_LIBSITE)/database
d_DATA = $(guile_postgres_sources)

ETAGS_ARGS = $(subpkgdata_DATA)

EXTRA_DIST = $(guile_postgres_sources)

retired = $(GUILE_SITE)/database

install-data-hook:
	if [ -f $(retired)/postgres.scm ] ; then \
	  rm -f $(retired)/postgres.scm ; fi
	for f in $(guile_postgres_sources) ; do	\
	  if [ -f $(retired)/$$f ] ; then	\
	    rm -f $(retired)/$$f ; fi ; done
	if [ -d $(retired) -a "`ls $(retired) 2>/dev/null`" = "" ] ; then \
	  rmdir $(retired) ; fi
	if guile-tools | grep -q make-module-catalog ; then \
	   guile-tools make-module-catalog $(GUILE_SITE) ; fi

## Makefile.am ends here
