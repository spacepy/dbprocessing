#Copy back the starting DB and rerun the injest and process
cp ../testDB.sqlite.bak ../testDB.sqlite
./ProcessQueue.py -i -m ../testDB.sqlite
./ProcessQueue.py -p -m ../testDB.sqlite
