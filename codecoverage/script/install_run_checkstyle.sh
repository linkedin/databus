#!/bin/sh
rm -rf checkstyle-5.3
curl -L http://downloads.sourceforge.net/project/checkstyle/checkstyle/5.3/checkstyle-5.3-bin.zip > checkstyle-5.3-bin.zip
unzip checkstyle-5.3-bin.zip
cd src
ant checkstyle -Dcheckstyle.home=`pwd`/../checkstyle-5.3
