#! /bin/sh

destroydb guile_pg_test >/dev/null 2>&1
createdb guile_pg_test || {
   echo test.sh: error: createdb failed. Giving up. 1>&2
   exit 1
}
PGDATABASE=guile_pg_test
GUILE_LOAD_PATH=$srcdir
export PGDATABASE GUILE_LOAD_PATH
../guile-pg -s guile-pg-lo-tests.scm
