#!/bin/bash
# Script for remote file open
date
export XRD_LOGLEVEL=Debug
lsetup 'root pilot-default'
echo LSETUP_COMPLETED
date
python3 REPLACE_ME_FOR_CMD
echo "Script execution completed."
exit $?
