#!/bin/bash
mkdir -p data
dd if=/dev/urandom of=data/horizontal_string_random ibs=1000 count=100
dd if=/dev/urandom of=data/vertical_string_random ibs=1000 count=100