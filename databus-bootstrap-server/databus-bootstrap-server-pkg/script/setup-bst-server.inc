var_dir=${script_dir}/../var

if [ ! -d ${var_dir} ] ; then
  mkdir -p ${var_dir}
fi

pid_file=${var_dir}/databus-bst-server.pid
out_file=${logs_dir}/databus-bst-server.out
jvm_gc_log=${logs_dir}/databus-bst-server_gc.log


# Custom memory configuration
heap_size=
max_heap_size=${heap_size:-}
new_size=
max_new_size=${new_size:-}
direct_mem_size=
