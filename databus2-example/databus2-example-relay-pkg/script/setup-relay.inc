var_dir=${script_dir}/../var

if [ ! -d ${var_dir} ] ; then
  mkdir -p ${var_dir}
fi

relay_pid_file=${var_dir}/databus2-relay-${source_name}.pid
relay_out_file=${logs_dir}/databus2-relay-${source_name}.out
