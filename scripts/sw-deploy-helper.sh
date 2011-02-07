#!/bin/bash
# Copyright (c) 2010 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# ----
#
# Skywriting/Ciel deployment helper script - usually not called directly, 
# but via sw-deploy-multi. Runs sw-deploy-local.sh on the cluster machines 
# after copying it over.
#
# usage: sw-deploy.sh hostname privkey [sw-user] [sw-user-pw]

TARGETHOST=$1
PRIVKEY=$2
#SRG_ROOT_PW= # set this in environment variable
BOOTSTRAP_SCRIPT="sw-deploy-local.sh"

if [[ $1 == '' || $2 == '' ]]; then
    echo "usage sw-deploy.sh hostname privkey [sw-user] [sw-user-pw]"
    exit 0
fi

if [[ $3 == '' ]]; then
    SWUSER='root'
elif [[ $2 == 'scc' ]]; then
    SWUSER='root'
    BOOTSTRAP_SCRIPT="sw-deploy-local-scc.sh"
else
    SWUSER=$3
fi

if [[ $4 == '' ]]; then
    SWROOT='/opt/skywriting'
else
    SWROOT=$4
fi

if [[ $5 == '' ]]; then
    GITUSER='mrry'
else
    GITUSER=$5
fi

if [[ $6 == '' ]]; then
    SWUSERPW=$SRG_ROOT_PW
else
    SWUSERPW=$6
fi

# output
echo "Deploying to $TARGETHOST..."

# install private key for non-EC2 case
# this requires some awkward putty hackery as ssh does not
# allow password-based login from non-interactive terminals
if [[ $3 == 'scc' ]]; then
    sleep 1
elif [[ $3 == 'ec2' ]]; then
    # EC2 case - just log in as "ubuntu" user and copy authorized_keys
    USER="ubuntu"
    ssh -o StrictHostKeyChecking=no -f -i $PRIVKEY $USER@$TARGETHOST "sudo cp ~$USER/.ssh/authorized_keys /root/.ssh/"
    SWUSER="root"
    sleep 5
else
    echo y | plink -pw $SWUSERPW $SWUSER@$TARGETHOST 'mkdir -p .ssh'
    pscp -q -pw $SWUSERPW $PRIVKEY.pub $SWUSER@$TARGETHOST:.ssh/authorized_keys
fi

# run remote deployment script
scp -o StrictHostKeyChecking=no -q -i $PRIVKEY $BOOTSTRAP_SCRIPT $SWUSER@$TARGETHOST:
ssh -o StrictHostKeyChecking=no -f -i $PRIVKEY $SWUSER@$TARGETHOST "~$SWUSER/$BOOTSTRAP_SCRIPT $SWROOT $GITUSER 1>&2 2>/dev/null"

# output
echo "done!"

