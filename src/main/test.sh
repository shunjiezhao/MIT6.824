#!/bin/bash
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt &
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so &
go run -race mrworker.go wc.so 
