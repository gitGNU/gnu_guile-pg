#!/bin/sh

if [ -z "$top_builddir" ] ; then echo $0: error: Bad env. ; exit 1 ; fi

${GUILE-guile} -l $top_builddir/scm/postgres.scm -c '(quit)'
rv=$?

exit $rv
