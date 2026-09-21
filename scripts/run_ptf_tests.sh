export SDE=~/bf-sde-9.9.0
export SDE_INSTALL=$SDE/install
export PATH=$SDE_INSTALL/bin:$PATH

$SDE/run_p4_tests.sh -p splitter -t path/to/splitter-switch/ptf-tests/ -s count-windows-tumbling
# $SDE/run_p4_tests.sh -p splitter -t path/to/splitter-switch/ptf-tests/ -s count-windows-sliding
# $SDE/run_p4_tests.sh -p splitter -t path/to/splitter-switch/ptf-tests/ -s time-windows-tumbling
# $SDE/run_p4_tests.sh -p splitter -t path/to/splitter-switch/ptf-tests/ -s time-windows-sliding