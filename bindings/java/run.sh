#!/bin/bash

java -Xmx256M -Xms256M -cp bin -Djava.library.path=lib edu.stanford.ramcloud.JRamCloud "$@"
