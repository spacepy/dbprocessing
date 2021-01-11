Until we have the unit tests (and automatic functional) set up to
support either sqlite or postgresql, this procedure was used to test
the database creation under Postgres.

$ psql -U postgres
# CREATE DATABASE dbprocessing_test;
# \q
(Note this basically ignores everything regarding roles, authentication, etc.)
$ PGUSER=postgres CreateDB.py -d postgresql dbprocessing_test
$ PGUSER=postgres printInfo.py dbprocessing_test Mission

This raises an error because there are no entries in the mission
table, but confirms that database access works.

addFromConfig doesn't currently work, but once it does, can do:

$ PGUSER=postgres addFromConfig.py -m dbprocessing_test functional_test/config/testDB.conf

Clean up:
$ psql -U postgres
# DROP DATABASE dbprocessing_test;
# \q
