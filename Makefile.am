## Process this file with automake to produce Makefile.in
##
##   Copyright (C) 2002,2003,2004,2005,2006,2007,2008 Thien-Thi Nguyen
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

SUBDIRS = src test doc

EXTRA_DIST = THANKS TODO HACKING \
		build-aux/sofix

noinst_DATA = .module-catalog

.module-catalog:
	$(mkpimmc) -d $(DEPDIR) $@ "$(srcdir)"	\
	    -- -d in,lo,o -X '~$$' -X '[.]git' -X doc

install-data-hook:
	$(mkmodcat) "$(GUILE_LIBSITE)"

CLEANFILES = $(noinst_DATA)

dist-hook:
	sed -i 's/VERSION/$(VERSION)/' $(distdir)/test/show-platform

## bonus target
destroy-nonmodule-guile-pg-prior-install:
	dir="$(libdir)/guile-pg" ;				\
	if test -d $$dir ; then					\
	  echo NOTE: deleting $$dir ;				\
	  rm -rf $$dir ; fi ;					\
	dir="$(datadir)/guile/database" ;			\
	if test -d $$dir ; then cd $$dir ;			\
	  files="`ls postgres*.scm 2>/dev/null`" ;		\
	  if test -n "$$files" ; then				\
	    for f in $$files ; do				\
	      echo NOTE: deleting $$dir/$$f ;			\
	      rm -f $$f ; done ; fi ;				\
	  if test "`ls`" = "" ; then cd .. ;			\
	    echo NOTE: deleting $$dir ;				\
	    rmdir database ; fi ; fi ;				\
	dir="$(datadir)/guile" ; cat=".module-catalog" ;	\
	if test -f $$dir/$$cat ; then cd $$dir ;		\
	  echo NOTE: modifying $$dir/$$cat ;			\
	  sed '/database postgres/d' $$cat > TMP ;		\
	  mv TMP $$cat ; fi

## Makefile.am ends here
