#If this is first run, make the starting DB
if [ ! -f ../testDB.sqlite.bak ]; then
	./CreateDB.py ../testDB.sqlite.bak
	./addFromConfig.py -m ../testDB.sqlite.bak ../config/testDB.conf
fi

#Copy back the starting DB and rerun the injest and process
cp ../testDB.sqlite.bak ../testDB.sqlite
./ProcessQueue.py -i -m ../testDB.sqlite
./ProcessQueue.py -p -m ../testDB.sqlite
