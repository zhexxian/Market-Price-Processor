#!/bin/sh
cd $(dirname $0)

cd ..

mvn clean package
ret=$?
if [ $ret -eq 0 ]; then
  exit $ret
fi
rm -rf target

mvn clean compile
ret=$?
if [ $ret -eq 0 ]; then
exit $ret
fi
rm -rf target

exit
