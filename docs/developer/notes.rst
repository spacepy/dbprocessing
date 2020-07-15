=====
Notes
=====

Ingest and Process steps
========================

Ingest(-i)
----------
1. checkIncoming - Gets all files in incoming directory, and adds them to 'queue'(removes duplicate files)
2. importFromIncoming - Pops files off 'queue', checks that they don't exist in the db already runs figureProduct() on them, then calls diskfileToDB().
3. figureProduct - runs every inspector on the files, stops and returns the diskfile that is created by the inspector when one matches.
4. diskfileToDB - Enters file into DB, moves the file to the its correct home, sets files in the db of the same product and same utc_file_date to not be newest version, adds to processqueue for later processing, and returns file_id

Process(-p)
-----------
1. _processqueueClean - Go through the process queue and clear out lower versions of the same files. Then, sort on dates, then sort on level. (some half baked newest_version stuff in here)
2. buildChilden - Called on every item in the processqueue. Calculates all possible children products
3. runMe.runner - Created for every item on the runme_list. Handles all the magic of running codes.

	A. Build up the command line and store in a commands list
	B. Loop over the commands

		a. Start up to MAX_PROC processes with subprocess.Popen
		b. Poll if they are done or not, and if they finished successfully
			i. Success: Move output file to incoming dir, run all inspectors on it to see what product a file is(why?), diskfileToDB is run(see -i section)
			ii. Failure: move stdout and stderr to errors

