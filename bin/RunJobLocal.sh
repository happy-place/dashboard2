#!/bin/sh

env=$1
debug=$2
script=$3
start_dt=$4
end_dt=$5

go run ./bin/tree/RunTreeJob.go -env $env -debug $debug -sql $script && \
go run ./bin/files/RunFileJob.go -env $env -debug $debug -sql $script && \
go run ./bin/script/RunScriptJob.go -env $env -debug $debug -sql $script -start $start_dt -end $end_dt