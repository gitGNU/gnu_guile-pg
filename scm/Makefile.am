## Process this file with automake to produce Makefile.in.

##   Copyright (C) 2002,2003,2004,2005,2006,2008 Thien-Thi Nguyen
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

ddir = $(GUILE_LIBSITE)/database
dist_d_DATA =					\
  postgres-resdisp.scm				\
  postgres-types.scm				\
  postgres-col-defs.scm				\
  postgres-resx.scm				\
  postgres-qcons.scm				\
  postgres-gxrepl.scm				\
  postgres-table.scm				\
  postgres-meta.scm

retired = $(GUILE_SITE)/database

install-data-hook:
	if [ -d "$(retired)" ] ; then					\
	  cd "$(retired)" ;						\
	  rm -f postgres.scm $(dist_d_DATA) ;				\
	  if [ "`ls 2>/dev/null`" = "" ] ; then				\
	    cd .. ; rmdir "$(retired)" ; fi ;				\
	  if $(GUILE_TOOLS) | grep -q make-module-catalog ; then	\
	    $(GUILE_TOOLS) make-module-catalog "$(GUILE_SITE)" ; fi ;	\
	fi

## Makefile.am ends here
