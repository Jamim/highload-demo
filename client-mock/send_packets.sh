#!/bin/bash

cd test_data/packets

for filename in $(ls *.json); do
    echo "${filename}"
    cat "${filename}" > /dev/udp/127.0.0.1/51273
done
