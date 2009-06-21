## Process this file with Automake to create Makefile.in
##
##   Copyright (C) 2003,2004,2005,2006,2007,2008 Thien-Thi Nguyen
##   Portions Copyright (C) 1998 Ian Grant
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

info_TEXINFOS = guile-pg.texi
guile_pg_TEXINFOS = guile-pg.texi relinfo.texi qcons-tables.texi

relinfo.texi: $(top_srcdir)/configure
	@echo Creating $@
	@echo "This manual documents $(PACKAGE_STRING)" > $@
	@echo "(released `date +%Y-%m-%d -r $<`)." >> $@

qcons-tables.texi: $(top_srcdir)/src/postgres-qcons.scm
	$(srcdir)/mkinc $< > $@

DISTCLEANFILES = guile-pg.info

EXTRA_DIST = guile-pg.twerp mkinc guile-pg.html

if MAINTAINER_MODE

.twerp.texi:
	$(GUILE_TOOLS) twerp2texi	\
	  --scheme-implied		\
	  -I ../src/.doc-index		\
	  -d .deps/$*.Ptexi		\
	  -o $(srcdir)/$*.texi		\
	  $<

include twerp.mk-am-frag

endif # MAINTAINER_MODE

## Makefile.am ends here
