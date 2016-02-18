Scripts
=======

CreateDB.py
-----------

Create an empty sqlite database for use in dbprocessing.
(currently creates a RBSP database, this should be updated as an option).

This is the first step in the setup of a new processing chain.

.. option:: dbname The name of the database to create


addFromConfig.py
----------------

Adds data to a database from a config file. This is the second step in
setting up new processing chain.

See :doc:`ConfigurationFiles` for a full description of the config file
format and capability.

.. option:: config_file The name of the config file to ingest
.. option:: -m <dbname>, --mission <dbname> The database to apply the config file to
.. option:: -v, --verify  Verify the config file then stop


addVerboseProvenance.py
-----------------------

go into the database and get the verbose provencoe for a file
then add that to the global attrs for the file
either putout to the same file or a different file

This code has not been fully tested or used.

clearProcessingFlag.py
----------------------

Clear a processing flag (lock) on a database that has crashed.

.. option:: database The name of the database to unlock
.. option:: message Log message to insert into the database

configFromDB.py
---------------

Build a config file from an existing database. This is untested and not
fully useful yet.

.. option:: The filename to save the config
.. option:: -m,--mission The basebase to to connect to
.. option:: -f,--force Force the creation of the config file, allows overwrite
.. option:: -s,--satellite The name of the satellite for the config file
.. option:: -i,--instrument The name of the instrument for the config file
.. option:: -c,--nocomments Make the config file without a comment header block on top



