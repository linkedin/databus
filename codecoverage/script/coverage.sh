#!/bin/bash
#
# Generate code coverage for espresso trunk
#  The steps are
#   -. Compile all 
#   -. Gather the classes/main into build/coverage/instrumented_classes
#   -. Gather src/main into build/coverage/src_main
#   -. Instructment the classes   (some of the class will not generated instrumented classes, that's why we need to gather the classes first)
#   -. link the classes/main to the instrumented_classes
#   -. run test with Cobertura on, (set env COBERTURA_RUN=1, for integration test, set -Pcobertura=true for unit test)
#   -. restore classes/main
#   -. Merge the cobertura.ser in subproject (e.g. espresso-client/espresso-client-impl/cobertura.ser)
#   -. generate report

#   
#  Input: test tasks (e.g. espresso-client:espresso-client-impl:test), default all the tests 
#  Example
#    codecoverage/script/coverage.sh espresso-client:espresso-client-impl:test

#if [ "$1" == "" ]; then
TEST_TASK=test
#else
# TEST_TASK=$1
#fi

# this is for hudson run
export PATH=$PATH:export/home/tester/bin/cobertura:$HOME/hudson/data/tools/GRADLE_1_0_M3/gradle-1.0-milestone-3/bin

SCRIPT_DIR=`dirname $0`
SCRIPT_DIR=codecoverage/script
SCRIPT_DIR=`cd $SCRIPT_DIR; pwd`
   
ROOT_DIR=$SCRIPT_DIR/../../
VIEW_ROOT=`cd ${ROOT_DIR}; pwd`

RUN_INTEGRATION_TEST=FALSE

for i in $*
do
	case $i in
    	--run-integration-test=*)
		# Read value from command line and convert to all-upper case
		RUN_INTEGRATION_TEST=`echo $i | sed 's/[-a-zA-Z0-9]*=//' | tr "[:lower:]" "[:upper:]"`
		echo RUN_INTEGRATION_TEST=$RUN_INTEGRATION_TEST
		;;
    	*)
                # unknown option
		;;
  	esac
done

# clean up
cd ${VIEW_ROOT}
COVERAGE_DIR=${VIEW_ROOT}/build/cobertura
rm -rf $COVERAGE_DIR
mkdir -p $COVERAGE_DIR
find . -iname "cobertura*.ser" | xargs rm -f

#   -. Compile all and pull in dependencies
ligradle clean compileJava compileTestJava assemble

#   -. Gather the classes/main into build/coverage/instrumented_classes
find build -name "*.class" | grep "classes/main" | egrep -v "(perf-test-impl|schemas_registry-static)"| while read f; do d=`dirname $f`; destd=${COVERAGE_DIR}/instrumented_classes/`echo $d|awk '{print $NF}' FS=main/`; mkdir -p $destd; cp $f $destd; done

#   -. Gather src/main into build/coverage/src_main
find . -name "*.java" | grep "src/main" | egrep -v "(perf-test-impl|schemas_registry-static)" | while read f; do d=`dirname $f`; destd=${COVERAGE_DIR}/src_main/`echo $d|awk '{print $NF}' FS=main/`; mkdir -p $destd; cp $f $destd; done

#   -. Instructment the classes   (some of the class will not generated instrumented classes, that's why we need to gather the classes first)
# $SCRIPT_DIR/cobertura-instrument.sh --datafile ${COVERAGE_DIR}/cobertura.ser --ignore *test* --ignore *tools* ${COVERAGE_DIR}/instrumented_classes
$SCRIPT_DIR/cobertura-instrument.sh --datafile ${COVERAGE_DIR}/cobertura.ser --ignore com.linkedin.espresso.smoketest* --ignore com.linkedin.espresso.tools* --ignore com.yahoo.ycsb* ${COVERAGE_DIR}/instrumented_classes

#   -. link the classes/main to the instrumented_classes
for class_dir in `find build -name classes | tr '\n' ' '`; do mv ${class_dir}/main ${class_dir}/main_backup; ln -s ${COVERAGE_DIR}/instrumented_classes ${class_dir}/main; done

#   -. run test with Cobertura on, (set env COBERTURA_RUN=1, for integration test, set -Pcobertura=true for unit test)
#gradle -Pcobertura=true -x compileJava -x compileTestJava espresso-client:espresso-client-impl:test 
#export COBERTURA_RUN=true  # set for integration test 
export CODE_COVERAGE=true  # set for integration test 
#gradle -Pcobertura=true -x compileJava -x compileTestJava ${TEST_TASK}
ligradle -Pcobertura=true -PallTests=true -x compileJava -x compileTestJava ${TEST_TASK}

#   -. Merge the cobertura.ser in subproject (e.g. espresso-client/espresso-client-impl/cobertura.ser)
#rm ${COVERAGE_DIR}/cobertura.ser  # remove this as they will be created in the project dir
$SCRIPT_DIR/cobertura-merge.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser `find . -iname "cobertura*.ser" | tr '\n' ' '`

#   -. run integration tests
if [ "$RUN_INTEGRATION_TEST" == "TRUE" ]; then
  echo Running Integration Tests ...
  ligradle -Pinteg.test=true -PallTests=true :integration-test:integration-test-integ:test
fi

#   -. restore classes/main
for class_dir in `find build -name classes | tr '\n' ' '`; do rm -f ${class_dir}/main; mv ${class_dir}/main_backup ${class_dir}/main; done

#   -. generate report
$SCRIPT_DIR/cobertura-report.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser --destination ${COVERAGE_DIR}/report --format html ${COVERAGE_DIR}/src_main/java
$SCRIPT_DIR/cobertura-report.sh --datafile ${COVERAGE_DIR}/cobertura_merged.ser --destination ${COVERAGE_DIR}/report --format xml ${COVERAGE_DIR}/src_main/java

