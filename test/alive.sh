#!/bin/sh

${GUILE-guile} -l $top_builddir/scm/postgres.scm -c '(quit)'
