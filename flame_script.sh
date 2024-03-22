#!/bin/bash

################################################## Variables ##################################################

FLAMEDIR="/home/fbc/FlameGraph"
LOCAL="192.168.100.1:12345"
HOMEDIR="/home/fbc"
OUTPUTDIR="/home/fbc"
TIMEOUT=30
SERVER_ARGS="8 sqrt"
SERVER_CPUS="1,3,5,7,9,11,13,15,17"
SERVER_LCORES=`echo ${SERVER_CPUS} | sed 's/,/:/g'`
ARGS=(--server ${LOCAL} ${SERVER_LCORES} ${SERVER_ARGS})
COMMAND="taskset -c ${SERVER_CPUS} sudo HOME=${HOMEDIR} LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu make test-system-rust LIBOS=catnip TEST=tcp-echo-multiflow"

################################################## Capture perf.data ##################################################
echo 0 | sudo tee /proc/sys/kernel/nmi_watchdog
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
taskset -c 1 sudo perf record -o ${FLAMEDIR}/perf.data -F 999 -C ${SERVER_CPUS} --no-buffering --call-graph dwarf,16384 -- $COMMAND ARGS="--server $LOCAL ${SERVER_LCORES} ${SERVER_ARGS}"
echo 1 | sudo tee /proc/sys/kernel/nmi_watchdog
################################################## Plot perf.data ##################################################

## You need to clone FlameGraph repo and set ${FLAMEDIR} variable
# git clone https://github.com/brendangregg/FlameGraph

cd ${FLAMEDIR}
sudo perf script 2>/dev/null | ./stackcollapse-perf.pl > out.perf-folded
./flamegraph.pl out.perf-folded > perf.svg
sudo rm -f perf.data
cd -
