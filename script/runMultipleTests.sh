#!/bin/bash

#FirstNode="172.31.52.119"
#AllNodes="172.31.52.119 172.31.48.15 172.31.59.7 172.31.59.197 172.31.59.198"
#RestNodes="172.31.52.119 172.31.48.15 172.31.59.7 172.31.59.197 172.31.59.198"
FirstNode="172.31.23.49"
AllNodes="172.31.23.49 172.31.30.71"
RestNodes="172.31.23.49 172.31.30.71"
#AllNodes="172.31.52.119 10.20.0.78 10.20.0.79"
#RestNodes="172.31.52.119 10.20.0.78"
Cookie="antidote"
Mode="erl"
#./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 1
./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 2 
#./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 3 
#/script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 4 


Mode="pb"
#./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 1
./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 2 
#./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 3 
#./script/runMultiDCBenchmark.sh "$AllNodes" "$RestNodes" antidote 1 $Mode 4 