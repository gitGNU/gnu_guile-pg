## Process this file with Automake to create Makefile.in
##
##  	Copyright (C) 1998 Free Software Foundation, Inc.
##
##   This file is part of GUILE-PG.
##
##   GUILE-PG is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as
##   published by the Free Software Foundation; either version 2, or
##   (at your option) any later version.
##
##   GUILE is distributed in the hope that it will be useful, but
##   WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public
##   License along with GUILE-PG; see the file COPYING.  If not, write
##   to the Free Software Foundation, Inc., 59 Temple Place, Suite
##   330, Boston, MA 02111-1307 USA

info_TEXINFOS = guile-pg.texi
guile_pg_TEXINFOS = version.texi

CLEANFILES = guile-pg.info guile-pg.dvi guile-pg_toc.html guile-pg.html

EXTRA_DIST = cov guile-pg.twerp

dist-hook:
	if [ -x $(srcdir)/cov ] ; then \
	  ( cd $(srcdir) && ./cov ) > $(distdir)/doc.coverage ; fi

if MAINTAINER_MODE

BUILT_SOURCES = doc-index-prep

doc-index-prep:
	( cd .. ; $(MAKE) .doc-index )

.twerp.texi:
	$(GUILE_TOOLS) twerp2texi	\
	  -I $(top_builddir)/.doc-index	\
	  -d .deps/$*.Ptexi		\
	  -o $(srcdir)/$*.texi		\
	  $<

include twerp.mk-am-frag

endif # MAINTAINER_MODE

## Makefile.am ends here
