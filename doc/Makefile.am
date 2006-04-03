## Process this file with Automake to create Makefile.in
##
##   Copyright (C) 2003,2004,2005,2006 Thien-Thi Nguyen
##   Portions Copyright (C) 1998 Ian Grant
##
##   This file is part of Guile-PG.
##
##   Guile-PG is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as
##   published by the Free Software Foundation; either version 2, or
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
guile_pg_TEXINFOS = guile-pg.texi qcons-tables.texi

qcons-tables.texi: $(top_srcdir)/scm/postgres-qcons.scm
	$(srcdir)/mkinc $< > $@

CLEANFILES = guile-pg.info guile-pg.html

EXTRA_DIST = cov guile-pg.twerp twerp.mk-am-frag mkinc

dist-hook:
	if [ -x $(srcdir)/cov ] ; then \
	  ( cd $(srcdir) && ./cov ) > $(distdir)/doc.coverage ; fi

if MAINTAINER_MODE

BUILT_SOURCES = doc-index-prep

doc-index-prep:
	( cd .. ; $(MAKE) .doc-index )

.twerp.texi:
	guile-tools twerp2texi		\
	  -I $(top_builddir)/.doc-index	\
	  -d .deps/$*.Ptexi		\
	  -o $(srcdir)/$*.texi		\
	  $<

include twerp.mk-am-frag

html: guile-pg.texi
	$(MAKEINFO) --html $< -o $@

endif # MAINTAINER_MODE

## Makefile.am ends here
