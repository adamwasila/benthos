#!/bin/sh

rm ./output.txt

./benthos -c cassandra.yaml
