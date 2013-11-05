#!/bin/bash
# merge-source-trees.sh:  Merge all databus* subprojects into unified source
#   tree for code-coverage tests.  (No need for integration-test or perf trees
#   at this point.)
#
# Typical directory structure looks like this:
#
#   databus2-relay
#   └── databus2-relay-impl
#       └── src
#           ├── main
#           │   └── java
#           │       └── com
#           │           └── linkedin
#           │               ├── databus
#           │               │   ├── container
#           │               │   │   ├── netty
#           │               │   │   └── request
#           │               │   ├── core
#           │               │   │   └── util
#           │               │   └── monitoring
#           │               │       ├── events
#           │               │       └── mbean
#           │               └── databus2
#           │                   ├── producers
#           │                   │   └── db
#           │                   └── relay
#           │                       └── config
#           └── test
#               └── java
#                   └── com
#                .      └── linkedin
#               /|\         ├── databus
#                |          │   ├── container
#                |          │   │   └── netty
#                |          │   └── core
#                |          │       └── test
#                |          │           └── netty
#                |          └── databus2
#                |              └── relay
#                |
# We want to merge all src/main and src/test subtrees into a single pair of
# uber-combo src/main and src/test trees, e.g.:
#
#   code-coverage-all
#   └── src
#       ├── main
#       │   └── java
#       │       └── com
#       │           └── linkedin
#       │               ├── ...
#       └── test
#           └── java
#               └── com
#                   └── linkedin
#                       ├── ...
#

top="./"
# this is relative to $top :
mergedir="code-coverage-all"

cd $top

#if [ -e "$mergedir" ]; then
#  echo "error:  $mergedir subdirectory already exists; bailing"
#  exit 1
#fi

echo "Creating $mergedir/src for unified tree."
mkdir -p "$mergedir"/src || exit 2

# databus-events/databus-events-* dirs are older duplicates of content under
# databus-events/databus-events/ (179 duplicate files), so we exclude them for
# performance/redundancy reasons:
allfiles=`find . -type f -print | sed 's#^./##' | grep '^databus' | fgrep -v '/bin/' | grep '/.*/' | egrep '/src/main/|/src/test/' | fgrep -v 'databus-events/databus-events-'`

echo "Copying files."
for file in $allfiles; do
  # get rid of the databus*/databus*/src/ prefix:
  relative_path=`echo "$file" | sed 's/^[^/]*\/[^/]*\/src\///'`
  # lop off the filename:
  target_dir=`echo "$relative_path" | sed 's/[^/]*$//'`
  # create the target directory:
  mkdir -p "$mergedir"/src/"$target_dir" || exit 2
  # copy (or hard-link) the file into the target directory:
  cp -p "$file" "$mergedir"/src/"$target_dir"/. || exit 3
done

echo "Done."
exit 0
