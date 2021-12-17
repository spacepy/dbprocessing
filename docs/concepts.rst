********
Concepts
********

Understanding some key concepts about how dbprocessing views the world
makes it easier to deploy on a project. The treatment here is fairly
abstract but links to the concrete representations in Python code and
SQL objects.

.. contents::
   :depth: 1
   :local:

.. _concepts_files:

Files
=====
dbprocessing exists to manage the production of files from other
files. Conceptually, the treatment of a "file" in dbprocessing maps
directly to a single file on disk. Data are stored entirely in files;
the database contains metadata only.

Metadata about a file can be represented in several different ways:

   * As a record in the :sql:table:`file` table.
   * As an instance of :class:`~dbprocessing.Diskfile.Diskfile`, containing
     solely information about the file on disk, or of
     :class:`~dbprocessing.DBfile.DBfile`, which interacts with the database
     representation.

A file has certain properties:

   file date
      The "characteristic date" of the data contained within the file.
      For a daily file, the expectation is that most of the data in
      the file are timestamped with times within that day. But the
      file date is treated distinctly from the actual first and last
      timestamp, because a "daily" file might include a small amount
      of data timestamped on the previous or next day, depending on
      the needs of the mission. Thus dbprocessing needs to be aware
      both of "the date" of the file and the actual timestamps, so it
      can gather all data timestamped on a particular date. Usually
      the file date is reflected in the filename in some way.

   version
      The version of the file itself. Each file has a unique version
      that relates to its production history, but different files
      (e.g. for different days of data) with identical versions are
      not guaranteed to have had the same history. Version treatments
      are consistent across dbprocessing; see :ref:`concepts_versions`.

Files that have the same structure and are considered part of the same
data set; they are described as having the same :ref:`product
<concepts_products>`. Again this is frequently reflected in the filename.

The combination of file date, product, and version is considered to be
unique: only one file of a particular date, product, and version can
exist within the database.

.. note::
   It is important to distinguish between the "date" of a file in the
   sense of the timestamps on the data it contains, and the "date" in
   the sense of the timestamp on the file itself in the filesystem.
   Unless qualified, a file's "date" in dbprocessing always refers to
   the former, which is more important in processing data.

Files are largely treated the same whether they are created by
processes controlled by dbprocessing, or if they are created by other
means and then brought into the dbprocessing environment. Regardless
of where files are created, metadata are populated by a process called
:ref:`ingestion <concepts_ingest>`.

dbprocessing itself does not create data files; that is the
responsibility of data processing codes.
      
.. _concepts_codes:

Codes
=====
A data processing code, or simply "code", produces an output data file
from one ore more inputs. There are several requirements on a code:

   * It must be callable from the command line.
   * It must accept one or more input files and produce a single
     output file.
   * It can accept any combination of arguments, as long as the
     arguments are followed by the list of all input files, and
     finally the path of the output file.

Codes are represented in the :sql:table:`code` table.

Given a set of files and codes, dbprocessing's task is to call the
appropriate codes to generate all possible derived files. The
relationships that allow this are described at a higher level, through
:ref:`products <concepts_products>` and
:ref:`processes <concepts_processes>`.

There are two exceptions to the many-in, one-out concept:

   * :ref:`scripts_DBRunner` allows for the execution of codes with no
     inputs.
   * :ref:`concepts_Processes` with a ``RUN`` timebase do not produce outputs.

.. _concepts_products:

Products
========
A product is a *generalization of a file*. For instance, "HOPE-A level
3 pitch angle-resolved" is an example of a product.
``rbspa_rel04_ect-hope-PA-L3_20150102_v7.1.0.cdf`` is a file which is
an instance of this product, specifically with version 7.1.0 and containing
data for 2015-01-02.

Two properties of a product are of particular relevance:

   format
      The product's format describes how to build and parse the filename
      for files of that product. It includes the filename only, no directory,
      and may include wildcards to be filled by metadata. See
      :ref:`substitutions <concepts_substitutions>`.

   relative_path
      Path to the directory containing files of this product, relative to
      the mission's :sql:column:`~mission.rootdir`.

Determining the product of a file, among other metadata, is a task for
an :ref:`inspector <concepts_inspectors>`.

Products are represented in the :sql:table:`product` table.

.. _concepts_processes:

Processes
=========
As :ref:`products <concepts_products>` generalize files, so a process is
*a generalization of a code*. Processes describe the relationship between
any number (usually one or more) of input products, and usually one output
product (but sometimes zero).

Input products to a process may be optional, in which case a process
can execute without them. The input specification may also include a
request to include multiple days of input.

There are two other major properties of a process:

   output_product
      The product produced by this process (i.e., the type of file
      created by codes which implement this process.) This is optional
      for processes which produce no output.
   
   output_timebase
      The amount of data included in each file produced by this process.
      Currently the implemented timebases are ``DAILY``, to produce files
      with one day worth of data, ``RUN``, for processes that produce
      no output, and ``FILE``, for processes that map the time period of
      their input directly to the output. The timebase specification
      allows dbprocessing to find the appropriate set of inputs; ``DAILY``
      is almost always the correct choice. (``FILE`` rarely is, even for
      processes that take single-day input and produce single-day output).

Processes are represented in the :sql:table:`process` table; the
connection to input products is in :sql:table:`productprocesslink`.

.. _concepts_versions:

Versions
========
dbprocessing treats versions as a triplet of major.minor.subminor. These are
called, respectively, the interface, quality, and revision versions. The
versions are dot-separated numbers, not decimals: 1.1.0 and 1.10.0 are
different versions.

The *interface* version indicates compatibility. Changes in a file's
interface suggest a change to file structure; changes in a code's
interface usually suggests a change in the input or output files. For this
reason, it is recommnded that the interface version of a code be incremented
whenever the interface version of its output or any inputs is incremented.

A change to the *quality* version suggests a change where a user of the data
would generally care. This might be an improvement in processing or merely
the incorporation of additional data. Quality changes are the most common.

Changes to the *revision* version indicate very minor changes that a data
user may not find important. This may mean, for instance, small metadata
changes.

The enforced rules are:
   * The version of a *code* is set directly in the database.
   * The *interface* version of a file is usually determined by the
     :sql:column:`code.output_interface_version` of the code that makes it.
   * The first time a file of a given product, date, and interface version
     is created, it has version X.0.0 (where X is the interface version.)
   * If a new version of a file for a given product and date is created,
     its quality version is incremented if the quality *or* interface version
     of any of its inputs (any input files or code) are incremented.
   * A file's revision version is incremented if its quality version has not
     been incremented and the revision version of any of its inputs are
     incremented.

.. seealso::
   :class:`~dbprocessing.Version.Version`

.. _concepts_inspectors:

Inspectors
==========
dbprocessing does not interpret the contents of any data files. The bridge
between the generic handling of dbprocessing and the specific file format
is a small piece of code called an *inspector*. Every
:ref:`product <concepts_products>` has an associated inspector, which has
two tasks:

   1. Verifying a file is an instance of the product associated with this
      inspector.
   2. Extracting certain metadata from the file for use in dbprocessing.

The product match is a yes/no question: an inspector does not *choose* a
product, but verifies if a file matches the product. Keyword arguments can
be used to specify the product if the same piece of inspector code is used
for multiple products.

.. seealso::
   :sql:table:`inspector` table, :mod:`~dbprocessing.inspector` module

.. index:: ingest

.. _concepts_ingest:

Ingestion
=========
Bringing new files into the database is called "ingesting." New files are
searched for in the "incoming directory" (:sql:column:`mission.incoming_dir`)
and:

   1. The product is identified by calling
      :ref:`inspectors <concepts_inspectors>`.
   2. A :sql:table:`file` record is created, including metadata from the
      inspector.
   3. The file is moved to the appropriate directory based on its product.
   4. The file is added to the :ref:`concepts_process_queue` for
      consideration in future processing.

The ingestion process is run via :option:`ProcessQueue.py -i`.

.. _concepts_process_queue:

Process Queue
=============
The process queue is a list of files to evaluate as potential *inputs* to
new processing. It is implemented as table :sql:table:`processqueue`.

This is not the same as the :class:`~dbprocessing.dbprocessing.ProcessQueue`
class, which implements most of the logic of handing the processing queue
(and ingestion), or the :ref:`scripts_ProcessQueue_py` script, which is
the front-end for this processing.

.. _concepts_processing:

Processing
==========
"Processing" is the consideration of every file in the process queue as
a potential input for processing. For every file in the queue, this
procedure:

   1. Considers the file's product and date.
   2. Finds all processes which can be run with that product as input
   3. For each process:

      a. Consider all possible output files that can be made with the file's
	 date of input.
      b. Consider all inputs (not just the relevant file's) resulting in those
	 files.
      c. Compare the inputs against all existing outputs
      d. If *any* input (not just the file from the process queue) is newer
	 than the output under consideration, execute a code associated with
	 that process, with all the newest inputs, to make a new ouput.
      e. Ingest the new outputs into the database.

	 i. The product is known, so the inspector is only used to verify it.
	 ii. Verbose provenance is known and populated.
	 iii. The newly created file is appended to the process queue.

This may sometimes result in counterintuitive effects. For instance,
if version 1.1.0 of a file is on the process queue but 1.2.0 exists,
new files will be made with 1.2.0, not 1.1.0. In practice there is
filtering to, for instance, avoid adding 1.1.0 and 1.2.0 to the queue
at the same time.

Processing is executed via :option:`ProcessQueue.py -p`.

.. _concepts_missions:

Missions
========
Most of the automation in dbprocessing happens at the level of
products and processes (with their associated files and
codes). However, it is convenient (e.g. in considering reprocessing)
to group products together. Products may be associated with
instruments, instruments with satellites, and satellites with
missions. There is some support for interacting with database
components (e.g. adding files to reprocess, or displaying product
information) by instrument, for convenience.

The mission has one other major function: all filesystem structure
(including data product locations but also incoming directory,
processing codes, etc.) is determined by mission.

In practice, there is one mission per database.

.. seealso::
   :sql:table:`mission` table

.. _concepts_substitutions:

Substitutions
=============
dbprocessing supports Python format-style substitutions in most database
fields that refer to files and directories. These substitutions are also
applied to command line arguments. Where a value is known (such as in
calculating the filename for a new file), the value is directly substituted;
where it is not, a matching regular expression may be used.

Fields are wrapped in ``{}``. A double-brace can be used to avoid expansion,
although avoiding braces is preferred. For instance, ``{Y}`` in the
:sql:column:`~product.format` of a product will correspond to the year
of a file in its filename, but the :sql:column:`~product.relative_path` may
also contain ``{Y}`` to allow files of a product to be separated by year.

The following fields are based on the :sql:column:`~file.utc_file_date` of
a file. All numbers are zero-padded.

   Y
      Four-digit year
   m
      Two-digit month
   b
      Three-character month abbreviation, English (e.g. "Jan")
   d
      Two-digit day
   y
      Two-digit year (not recommended)
   j
      Three-digit day of year
   H
      Two-digit hour (24-hour)
   M
      Two-digit minute
   S
      Two-digit second
   MILLI
      Three-digit millisecond
   MICRO
      Three-digit microsecond
   DATE
      Full date as YYYYMMDD
   datetime
      Full date as YYYYMMDD

The following fields are based on other characteristics of a :sql:table:`file`:

   VERSION
      Version, x.y.z

The following fields are supported but must be carried through by an
inspector; see :sql:column:`file.process_keywords`.

   QACODE
      QA code, from the QA loop, ``ok``, ``ignore``, ``problem``.

   mday
      Mission day, decimal number

   APID
      Application ID, hex number

   ??
      Any two-character string

   ???
      Any three-character string

   ``????``
      Any four-character string

   nn
      Any two-digit decimal number, in practice sometimes used for a version
      on files that do not follow the dbprocessing
      :ref:`versioning <concepts_versions>` scheme.

   nnn
      Any three-digit decimal number.

   nnnn
      Any four-digit decimal number.

The following fields are based primarily on the properties of a code or
mission; they are handled somewhat differently from the above.

   CODEDIR
      Directory containing a code; mostly used if a command line argument
      to a code needs its full path. This is assembled from the component
      parts (:sql:column:`mission.codedir` and
      :sql:column:`code.relative_path`).

   CODEVERSION
      Version of a code as x.y.z from :sql:table:`code`; mostly used in
      specifying the path to a code if the version is desired to be in
      the path without having to update it with new versions.

   ROOTDIR
      Root data directory of a mission, i.e. :sql:column:`mission.rootdir`.
      Because most paths specified in the database are relative, this is
      primarily useful if specifying additional command line arguments.

.. seealso::
   :class:`~dbprocessing.DBstrings.DBformatter`

QA Loop
=======
The QA loop was designed for RBSP-ECT to permit e.g. the validation of
level 1 files before generating level 2. It was not used in production,
but may eventually be documented and tested for other use.
