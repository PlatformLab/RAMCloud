#!/bin/bash

../../scripts/cluster.py --coordinatorArgs "--reset" --masterArgs \
                         "--totalMasterMemory=50%" --client \
                         "./runclient.sh" -s 5 -r 3 --disjunct --verbose
