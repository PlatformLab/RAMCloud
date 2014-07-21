#!/bin/bash

../../scripts/cluster.py --coordinatorArgs "--reset" --client "echo Done." -s 5 -r 3 --disjunct --debug --verbose
