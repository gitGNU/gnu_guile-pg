#! /bin/sh

if [ -z "$top_builddir" ] ; then echo $0: error: Bad env. ; exit 1 ; fi

drop=$top_builddir/test/drop.sh
create=$top_builddir/test/create.sh

$drop
$create || exit 1

${GUILE-guile} -l $top_builddir/scm/postgres.scm \
               -l config.scm \
               -s guile-pg-basic-tests.scm
rv=$?

$drop

exit $rv

# basic-tests.sh ends here
