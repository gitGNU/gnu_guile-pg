#!/bin/sh

if [ -z "$top_builddir" ] ; then echo $0: error: Bad env. ; exit 1 ; fi

if [ x"$DEBUG" = x ] ; then debug= ; else debug='--debug' ; fi

${GUILE-guile} $debug -l $top_builddir/scm/postgres.scm -c '(quit)'
rv=$?

exit $rv
