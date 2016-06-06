rm ../testDB.sqlite
./CreateDB.py ../testDB.sqlite
./addFromConfig.py -m ../testDB.sqlite ../config/testDB.conf
./ProcessQueue.py -i -m ../testDB.sqlite
./ProcessQueue.py -p -m ../testDB.sqlite
