lib_dir="$script_dir/../lib"
logs_dir="${script_dir}/../logs"
conf_dir="$script_dir/../conf"

cp="."
for f in ${lib_dir}/*.jar ; do
  cp="${cp}:${f}"
done

if [ ! -e $logs_dir ] ; then
  mkdir ${logs_dir}
fi

espresso_client_pid_file=${logs_dir}/espresso-client.pid
espresso_client_out_file=${logs_dir}/espresso-client.out