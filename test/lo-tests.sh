#! /bin/sh

destroydb guile_pg_test >/dev/null 2>&1
createdb guile_pg_test || {
   echo test.sh: error: createdb failed. Giving up. 1>&2
   exit 1
}
export PGDATABASE=guile_pg_test
../guile-pg -s $srcdir/guile-pg-lo-tests.scm
