#!/bin/bash

cd ${0%/*}
STR="$2"
build/install/ramcloud/bin/ramcloud $STR
