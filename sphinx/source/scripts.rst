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


