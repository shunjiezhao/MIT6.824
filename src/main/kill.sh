#!/bin/bash
pids=`ps | grep 'mr*' | awk '{print $1}' | xargs`
echo 'get all worker pid '$pids
kill -9 $pids
