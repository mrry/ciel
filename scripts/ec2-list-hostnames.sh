#!/bin/bash

ec2-describe-instances | gawk '$1 == "INSTANCE" { print $4; }'