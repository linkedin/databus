# this is for hudson run
export PATH=$PATH:export/home/tester/bin/cobertura
cd li_databus2_trunk/databus2
SCRIPT_DIR=`dirname $0`
SCRIPT_DIR=codecoverage/script
SCRIPT_DIR=`cd $SCRIPT_DIR; pwd`
ROOT_DIR=$SCRIPT_DIR/../../
VIEW_ROOT=`cd ${ROOT_DIR}; pwd`
cd ${VIEW_ROOT}
COVERAGE_DIR=${VIEW_ROOT}/build/cobertura

#   -. Gather src/main into build/coverage/src_main
find . -name "*.java" | grep "src/main" | egrep -v "(perf-test-impl|schemas_registry-static)" | while read f; do d=`dirname $f`; destd=${COVERAGE_DIR}/src_main/`echo $d|awk '{print $NF}' FS=main/`; mkdir -p $destd; cp $f $destd; done


$SCRIPT_DIR/cobertura-merge.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser `find . -iname "cobertura*.ser" | tr '\n' ' '`

#   -. generate report
$SCRIPT_DIR/cobertura-report.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser --destination ${COVERAGE_DIR}/report --format html ${COVERAGE_DIR}/src_main/java
$SCRIPT_DIR/cobertura-report.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser --destination ${COVERAGE_DIR}/report --format xml ${COVERAGE_DIR}/src_main/java



## -- generate the top offenders --
topN=10
topNplus1=$(($topN+1))
#   -. generate list of Top N classes under target percentage, sort by percentage coverage, complexity
REGEX=`cat $SCRIPT_DIR/whitelist.txt | sed 's/\#.*$//' | sed 's/^[  ]*//' | sed 's/[  ]*$//' | sed '/^$/d' | tr "\\n" "|" | sed 's/^\(.*\).$/\1/'`
echo "Applying this filter to egrep" $REGEX
ret_txt="coverage.sh: Generate coverage_parsed.log"
cat ${COVERAGE_DIR}/report/coverage.xml | grep "class name" | grep -v "\\$" | sed 's/^.*name=\"\(.*\)\".*filename.*line-rate=\"\(.*\)\" branch-rate.*complexity=\"\(.*\)\".*$/\2 \3 \1/' | egrep -v $REGEX | awk '{print 1.0-$1, $2, $3}' | sort -k 1,1nr -k 2,2nr | awk 'BEGIN{ printf "%12s %12s %100s\n", "Coverage", "Complexity", "Class"} // {printf "%10.2f %10.2f %100s\n" , 100.0*(1.0-$1) , $2 , $3}' > ${COVERAGE_DIR}/coverage_parsed.log

ret_txt="coverage.sh: generate outliers_top${topN}.log"
cat ${COVERAGE_DIR}/coverage_parsed.log | head -n $topNplus1 > ${COVERAGE_DIR}/outliers_top$topN.log 2>&1


find ${COVERAGE_DIR}/src_main/java -name "*.java" | xargs grep 'import.*com\.linkedin\.databus' | sed -e 's/.*\(com.linkedin.databus.*\);.*/\1/' | sort | uniq -c | sort -nr | head -50 > ${COVERAGE_DIR}/coverage_tmp2.log

while read  p; do
    echo $p
    q=`echo $p | awk '{print $2}'`
    r=`echo $p | awk '{print $1}'`
    echo $r
    grep -w ${q} ${COVERAGE_DIR}/coverage_parsed.log  >> ${COVERAGE_DIR}/coverage_tmp3.log
done < ${COVERAGE_DIR}/coverage_tmp2.log

paste ${COVERAGE_DIR}/coverage_tmp2.log ${COVERAGE_DIR}/coverage_tmp3.log > ${COVERAGE_DIR}/coverage_tmp4

cat ${COVERAGE_DIR}/coverage_tmp4 | awk 'BEGIN {printf "%12s %100s\n" ,"Coverage" , "Class"} //{printf "%10.2f %100s\n" , $3 , $5}' > ${COVERAGE_DIR}/coverage_tmp5

echo "*******************************************************************************************" > ${COVERAGE_DIR}/final_email
echo "                        TOP 10 WORST PEFROMING CLASSES                                     " >> ${COVERAGE_DIR}/final_email
echo "*******************************************************************************************" >> ${COVERAGE_DIR}/final_email
cat ${COVERAGE_DIR}/outliers_top$topN.log >> ${COVERAGE_DIR}/final_email

echo "" >> ${COVERAGE_DIR}/final_email
echo "" >> ${COVERAGE_DIR}/final_email
echo "" >> ${COVERAGE_DIR}/final_email

echo "*******************************************************************************************" >> ${COVERAGE_DIR}/final_email
echo "                        COVERAGE FOR TOP 50 USED CLASSES                                   " >> ${COVERAGE_DIR}/final_email
echo "*******************************************************************************************" >> ${COVERAGE_DIR}/final_email
cat ${COVERAGE_DIR}/coverage_tmp5 >> ${COVERAGE_DIR}/final_email


mail -s "Nightly Code Coverage Build - FILTERED DATA" stopiwal@linkedin.com pganti@linkedin.com cbotev@linkedin.com ksurlaker@linkedin.com bshkolnik@linkedin.com ssubrama@linkedin.com groelofs@linkedin.com bvaradar@linkedin.com snagaraj@linkedin.com nsomasun@linkedin.com mgandhi@linkedin.com <  ./final_email

