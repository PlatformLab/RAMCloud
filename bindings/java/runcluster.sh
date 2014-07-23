#!/bin/bash

../../scripts/cluster.py --coordinatorArgs "--reset" --masterArgs "--totalMasterMemory=80%" --client "echo Done." -s 5 -r 3 --disjunct --debug --verbose
