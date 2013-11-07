root_dir=${script_dir}/..
lib_dir="${root_dir}/lib"
logs_dir="${root_dir}/logs"
conf_dir="${root_dir}/conf"

cp="."
for f in ${lib_dir}/*.jar ; do
  cp="${cp}:${f}"
done

if [ ! -e $logs_dir ] ; then
  mkdir ${logs_dir}
fi

# guess JAVA_HOME
if [ -z "$JAVA_HOME" ] ; then
  if [ -d /Library/Java/Home ] ; then
    export JAVA_HOME=/Library/Java/Home
  elif [ -d /export/apps/jdk/latest ] ; then
    export JAVA_HOME=/export/apps/jdk/latest
  elif [ -d /export/apps/java/latest ] ; then
    export JAVA_HOME=/export/apps/java/latest
  elif [ -d /export/apps/jdk ] ; then
    declare -a a=($(ls -rd /export/apps/jdk/JDK*))
    JAVA_HOME_CANDIDATE=${a[0]}
    if [ -f $JAVA_HOME_CANDIDATE/bin/java ] ; then
      export JAVA_HOME=$JAVA_HOME_CANDIDATE
    elif [ -f $JAVA_HOME_CANDIDATE/usr/java/bin/java ] ; then
      export JAVA_HOME=$JAVA_HOME_CANDIDATE/usr/java
    fi
  elif which java >& /dev/null ; then
    export JAVA_HOME=$(dirname $(dirname $(which java)))
  fi
  #echo "Using JAVA_HOME=$JAVA_HOME"
fi

export PATH=$JAVA_HOME/bin:$PATH