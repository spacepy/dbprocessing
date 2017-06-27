rm -f ../testDB.sqlite
~/dbUtils/CreateDB.py ../testDB.sqlite
~/dbUtils/addFromConfig.py -m ../testDB.sqlite ../config/testDB.conf
~/dbUtils/ProcessQueue.py -i -m ../testDB.sqlite
~/dbUtils/ProcessQueue.py -p -m ../testDB.sqlite
