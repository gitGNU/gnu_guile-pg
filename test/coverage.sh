#!/bin/sh

if [ -z "$srcdir" ] ; then echo $0: error: Bad env. ; exit 1 ; fi

cd $srcdir
./cov | sed '/^yes/d' > UNTESTED

if [ -s UNTESTED ] ; then
    echo INFO: untested procedures "($0)"
    sed 's/^ no/INFO:/g' UNTESTED
    rv=1
else
    rv=0
fi

rm -f UNTESTED
exit $rv

# coverage.sh ends here
