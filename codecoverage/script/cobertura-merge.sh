lib_dir=`dirname $0`/../lib/cobertura/1.9.4.1
java -cp $lib_dir/cobertura.jar:$lib_dir/asm-3.0.jar:$lib_dir/asm-tree-3.0.jar:$lib_dir/log4j-1.2.9.jar:$lib_dir/jakarta-oro-2.0.8.jar net.sourceforge.cobertura.merge.Main $*
