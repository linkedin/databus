lib_dir="$script_dir/../lib"

cp="."
for f in ${lib_dir}/*.jar ; do
  cp="${cp}:${f}"
done
