#!/bin/sh
for dir in $@
do 
  for x in `find $dir -name "*\.java"`
  do 
  echo '|' $x '|' `svn log $x| egrep "^r[0-9]" | cut -d "|" -f 2 | awk '{total[$1]++; total_commits++} END { for (i in total) { print total[i]*100.0/total_commits, i;} }'  | sort -nr | head -n 2 | sed -e :a -e '$!N; s/\n/|/; ta'` '|'
  done
done
