#!/bin/sh

me=`whoami`
dest=/Users/$me/Desktop

rm -rf $dest/job-file
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $dest/job-file ./files/RunFileJob.go
rm -rf $dest/job-script
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $dest/job-script ./script/RunScriptJob.go
rm -rf $dest/job-tree
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $dest/job-tree ./tree/RunTreeJob.go

rm -rf $dest/dev.json
cp ../dev.json $dest/

rm -rf $dest/saas.json
cp ../saas.json $dest/

rm -rf $dest/script.tar.gz
tar -zcvf $dest/script.tar.gz ../script
