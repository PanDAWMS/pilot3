#!/bin/bash
# Script for remote file open
date
export XRD_LOGLEVEL=Debug
lsetup 'root pilot-default'
echo LSETUP_COMPLETED
date
python3 REPLACE_ME_FOR_CMD
export PYTHON_EC=$?
echo "Script execution completed."
echo PYTHON_COMPLETED $PYTHON_EC
exit $PYTHON_EC
