#!/bin/bash

LOCAL='192.168.100.1:12345'
HOMEDIR='/home/fbc'

if [ "$#" -ne 4 ]; then
	echo "Illegal number of parameters"
	exit
fi

DELAY=1000 #in ms

SERVER_CPUS=$1
SERVER_ARGS=$2
TIMEOUT=$3
USE_PERF=$4

SERVER_LCORES=`echo ${SERVER_CPUS} | sed 's/,/:/g'`

ARGS="--server ${LOCAL} ${SERVER_LCORES} ${SERVER_ARGS}"

if [ ${USE_PERF} -eq 1 ]; then
    echo 0 | sudo tee /proc/sys/kernel/nmi_watchdog
    echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid

    SERVER_LCORES_FOR_PERF=`echo ${SERVER_CPUS} | cut -d, -f2-`

    taskset -c ${SERVER_CPUS} sudo HOME=${HOMEDIR} LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu perf stat -C ${SERVER_LCORES_FOR_PERF} -A -I ${DELAY} -d -d -d -x'|' -o output.perf make test-system-rust CONFIG_PATH=${HOME}/config.yaml.novo LIBOS=catnip TEST=server_db ARGS="${ARGS}" TIMEOUT=$TIMEOUT
    #taskset -c ${SERVER_CPUS} sudo HOME=${HOMEDIR} LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu perf record -e page-faults make test-system-rust CONFIG_PATH=${HOME}/config.yaml.novo LIBOS=catnip TEST=server_db ARGS="${ARGS}" TIMEOUT=$TIMEOUT

    echo 1 | sudo tee /proc/sys/kernel/nmi_watchdog
else
    taskset -c ${SERVER_CPUS} sudo HOME=${HOMEDIR} LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu make test-system-rust CONFIG_PATH=${HOME}/config.yaml.novo LIBOS=catnip TEST=server_dbARGS="${ARGS}" TIMEOUT=$TIMEOUT
    #taskset -c ${SERVER_CPUS} sudo HOME=${HOMEDIR} LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu make test-system-rust CONFIG_PATH=${HOME}/config.yaml.novo LIBOS=catnip TEST=server_db ARGS="${ARGS}" TIMEOUT=$TIMEOUT RUST_LOG=trace
fi

## Example to run:
# ./run_server.sh "1,3,5,7,9,11,13,15,17" "8 sqrt" "30"
