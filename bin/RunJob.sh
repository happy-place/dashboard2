#!/bin/sh

env=$1
debug=$2
script=$3

./job-tree -env $env -debug $debug -sql $script && \
./job-file -env $env -debug $debug -sql $script && \
./job-script -env $env -debug $debug -sql $script