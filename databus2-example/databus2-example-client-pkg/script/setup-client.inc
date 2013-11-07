var_dir=${script_dir}/../var

if [ ! -d ${var_dir} ] ; then
  mkdir -p ${var_dir}
fi

client_pid_file=${var_dir}/databus2-client-${source_name}.pid
client_out_file=${logs_dir}/databus2-client-${source_name}.out
