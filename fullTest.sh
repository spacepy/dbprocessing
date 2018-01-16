#!/bin/sh

# This script runs the function test from start to finish to create a sqlite 
# database, and then runs the unit tests against that. This allows someone to 
# thoroughly test everything.

cd functional_test
bash ./functionalTest.sh
cd ..
python unit_tests/test_all.py