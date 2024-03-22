#!/bin/bash

## Rangers
HOMEDIR='/home/fbc'

## Prometheus
#HOMEDIR='/home/t-fabriciob'

## CloudLab (Clemson)
#HOMEDIR='/users/fabricio'

# ARGS="--calibrate sqrt"			                # Get latency (in ns) for 1 instruction
#ARGS="--calibrate sqrt 500"			            # Get instructions for XXX latency (in ns)		CALIBRATION:175.0
#ARGS="--calibrate sqrt 50000"					# Get instructions for XXX latency (in ns)	CALIBRATION:18287.0

#ARGS="--calibrate sqrt 1000"			            # Get instructions for XXX latency (in ns)		CALIBRATION:350.0
#ARGS="--calibrate sqrt 100000"					# Get instructions for XXX latency (in ns)	CALIBRATION:36574.0

ARGS="--calibrate randmem:11534336 1000"           # Get latency (in ns) for 1 instruction

# ARGS="--calibrate stridedmem:11534336:63"	        # Get latency (in ns) for 1 instruction
# ARGS="--calibrate stridedmem:25600:63 1000"       # Get instructions for XXX latency (in ns) #25KB == 80% 32KB    (L1)
# ARGS="--calibrate stridedmem:838656:63 1000"      # Get instructions for XXX latency (in ns) #819KB = 80% 1 MB    (L2)


#ARGS="--calibrate stridedmem:11534336:63 1000"      # Get instructions for XXX latency (in ns) #11MB == 80% 13.75MB (L3)	CALIBRATION:244.0	

# ARGS="--calibrate randmem:11534336"	        # Get latency (in ns) for 1 instruction
# ARGS="--calibrate randmem:1024 1000"       # Get instructions for XXX latency (in ns)
# ARGS="--calibrate randmem:25600 1000"       # Get instructions for XXX latency (in ns) #25KB == 80% 32KB    (L1)
# ARGS="--calibrate randmem:838656 1000"      # Get instructions for XXX latency (in ns) #819KB = 80% 1 MB    (L2)
# ARGS="--calibrate randmem:11534336 1000"      # Get instructions for XXX latency (in ns) #11MB == 80% 13.75MB (L3)

TIMEOUT=240

sudo HOME=$HOMEDIR LD_LIBRARY_PATH=$HOME/lib/x86_64-linux-gnu make test-system-rust TIMEOUT=${TIMEOUT} LIBOS=catnip TEST=tcp-echo-multiflow ARGS="${ARGS}"
