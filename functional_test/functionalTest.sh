#!/bin/sh

# The functional test has been run before, lets reset everything
if [ -f testDB.sqlite ]
then
    cp L0/* incoming/
    rm -rf L0 L1 L2
    rm -f testDB.sqlite
fi

~/dbUtils/CreateDB.py testDB.sqlite
~/dbUtils/addFromConfig.py -m testDB.sqlite config/testDB.conf
~/dbUtils/ProcessQueue.py -i -m testDB.sqlite
~/dbUtils/ProcessQueue.py -p --num-proc 1 -m testDB.sqlite