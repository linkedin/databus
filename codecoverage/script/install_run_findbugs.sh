#!/bin/sh
FINDBUGS_ARCHIVE=findbugs-1.3.9.tar.gz
curl -L http://downloads.sourceforge.net/project/findbugs/findbugs/1.3.9/$FINDBUGS_ARCHIVE > $FINDBUGS_ARCHIVE
tar zxvf $FINDBUGS_ARCHIVE
cd src
ant findbugs -Dfindbugs.home=`pwd`/../findbugs-1.3.9
