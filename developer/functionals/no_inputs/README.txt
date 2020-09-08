Functional test setup for processes that don't require inputs.

It's recommended to make a copy of this directory before messing with it,
and be careful not to commit scratch files...

Setup:
CreateDB.py no_inputs.sqlite
addFromConfig.py -m no_inputs.sqlite ./no_inputs.conf
mkdir root/data root/dbp_incoming root/dbp_codes/errors
chmod u+x root/dbp_codes/scripts/inputless_v1.0.0/inputless_v1.0.0.py

Run to just make files in the local directory:
DBRunner.py -m ./no_inputs.sqlite -s 20100101 -e 20100102 1

Try same thing but process name:
DBRunner.py -m ./no_inputs.sqlite -s 20100101 -e 20100102 inputless

Run so as to ingest them:
DBRunner.py -m ./no_inputs.sqlite -s 20100101 -e 20100102 1 -i -u
ls root/data/ # Should be files there
printProcessQueue.py ./no_inputs.sqlite # Should be two on queue
ProcessQueue.py -p -m ./no_inputs.sqlite # Nothing to do

Make sure don't run again; they're up to date:
DBRunner.py -m ./no_inputs.sqlite -s 20100101 -e 20100102 1 -u

Bump the code version:
python ./inc_code_version.py

Make sure get new versions of files:
DBRunner.py -m ./no_inputs.sqlite -s 20100101 -e 20100102 1 -u -i
