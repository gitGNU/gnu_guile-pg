#! /bin/sh

if [ -z "$top_builddir" ] ; then echo $0: error: Bad env. ; exit 1 ; fi

drop=$top_builddir/test/drop.sh
create=$top_builddir/test/create.sh

$drop
$create || exit 1

if [ x"$DEBUG" = x ] ; then debug= ; else debug='--debug' ; fi

${GUILE-guile} $debug \
               -l $top_srcdir/scm/postgres-types.scm \
               -l $top_srcdir/scm/postgres-col-defs.scm \
               -l $top_srcdir/scm/postgres-resx.scm \
               -l $top_srcdir/scm/postgres-table.scm \
               -l config.scm \
               -s $srcdir/guile-pg-abstraction-scm-tests.scm
rv=$?

$drop

exit $rv

# lo-tests.sh ends here
