#!/bin/sh

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
