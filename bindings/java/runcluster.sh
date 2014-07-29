#!/bin/bash

../../scripts/cluster.py --coordinatorArgs "--reset" --masterArgs \
                         "--totalMasterMemory=50%" --client \
                         "echo Done." -s 5 -r 3 --debug --disjunct --verbose
