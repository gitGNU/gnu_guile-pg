#! /bin/sh

if type dropdb >/dev/null 2>&1 ; then
    dropdb=dropdb
else
    dropdb=destroydb # old
fi

$dropdb guile_pg_test >/dev/null 2>&1
createdb guile_pg_test || {
   echo test.sh: error: createdb failed. Giving up. 1>&2
   exit 1
}
PGDATABASE=guile_pg_test
export PGDATABASE

${GUILE-guile} -l $top_builddir/scm/postgres.scm -s guile-pg-basic-tests.scm

$dropdb guile_pg_test

# basic-tests.sh ends here
