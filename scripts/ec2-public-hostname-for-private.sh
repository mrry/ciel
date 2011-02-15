#!/bin/bash

ec2-describe-instances | gawk '$5 == "'$1'" { print $4; }'