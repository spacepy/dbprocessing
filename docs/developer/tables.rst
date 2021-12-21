******
Tables
******

..
   Much of this file is auto-generated

   For the skeleton with just the table and column names, see
   developer/scripts/table_docs.py

   For the relationship diagram, see developer/scripts/table_graph.py

The SQL database tables used in dbprocessing, and their columns, are
summarized here. :py:meth:`~dbprocessing.DButils.DButils.getEntry` will,
in most cases, return a record from any table.

================================== =============================================
:sql:table:`code`                  Executable data-producing codes
:sql:table:`file`                  A single data file
:sql:table:`filecodelink`          Code used to create an output file
:sql:table:`filefilelink`          Input files used to create an output file
:sql:table:`inspector`             Codes that link files to products
:sql:table:`instrument`            Instrument (for grouping related products)
:sql:table:`instrumentproductlink` Connect instruments to products
:sql:table:`logging`               Log of :ref:`scripts_ProcessQueue_py` runs.
:sql:table:`logging_file`          Unused
:sql:table:`mission`               Directories for mission codes, files, etc.
:sql:table:`process`               Process that converts inputs to output
:sql:table:`processqueue`          Files to be processed
:sql:table:`product`               Generalization of file types
:sql:table:`productprocesslink`    Relates processes to their input products
:sql:table:`release`               Record of files in a release
:sql:table:`satellite`             Satellite (for grouping related products)
:sql:table:`unixtime`              Unix start/stop time for files
================================== =============================================

.. graphviz:: ../images/schema.dot

.. sql:table:: code

   Table describing a single executable script, used for creating
   output files from input files (see :ref:`concepts_codes`). Note each
   version of a code has its own entry (with no explicit connection between
   them), and a given script may be referred to/used by more than one code
   entry.

.. sql:column:: code_id

   Auto-incremented ID for this code, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   Filename (only) of executable; this is passed as part of the command
   line. May include substitution strings.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: relative_path

   Directory containing :sql:column:`filename`, relative to
   :sql:column:`mission.codedir`.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_start_date

   Code is valid for files dated on or after this date; matching is by
   :sql:column:`~file.utc_file_date`.
   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_stop_date

   Code is valid for files dated on or after before date; matching is by
   :sql:column:`~file.utc_file_date`.
   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_description

   Human-readable description of a code. May be considered a name but
   usually longer than a name and usually not used for lookup.
   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: process_id

   The process implemented by this code.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`process.process_id`)

.. sql:column:: interface_version

   Version of the *code*. Full version is ``interface.quality.revision``.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   Version of the *code*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   Version of the *code*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_interface_version

   Interface (i.e. major) version of the *output product* of this code.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: active_code

   Whether the code is active; inactive codes are not used for processing
   files.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: date_written

   Date code was written; meant for human information only.
   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: shasum

   SHA-1 checksum of the code; meant for validation but not currently used.
   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: newest_version

   Whether this is the newest version of a particular code. This may somewhat
   conflict with having multiple versions of a code that are selected based
   on :sql:column:`code_start_date` and :sql:column:`code_stop_date`; in
   practice, to date only one version of a code has been marked
   ``newest_version`` and it is also usually the only one marked
   :sql:column:`active_code`.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: arguments

   Command line arguments for building the code. The full command line is
   built from :sql:column:`mission.codedir`, :sql:column:`relative_path`,
   :sql:column:`filename`, :sql:column:`process.extra_params`, ``arguments``,
   the input files, and then output files (in that order).
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: ram

   A relative measure of how much memory this code consumes. Purely
   relative and used in calculating how many codes are currently
   running for purposes of :std:option:`ProcessQueue.py -n`. Nominally
   1, so e.g. making 2 indicates a process that takes up twice as much
   RAM as "typical", and 0.5 indicates half as much as typical.
   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: cpu

   Analagous to :sql:column:`ram`, a relative measure of how much processor
   power it takes to run this code. More concretely, this should usually be
   set to the number of threads a code uses (thus being integral); a
   long-running single-threaded process should still be set to ``1``.
   (:py:class:`~sqlalchemy.types.SmallInteger`)

.. sql:table:: file

   A single data :ref:`file <concepts_files>`; conceptually maps to a single
   file on disk. Related, but not identical, to
   :py:class:`~dbprocessing.Diskfile.Diskfile` and
   :py:class:`~dbprocessing.DBfile.DBfile`. Much of this information is
   populated by the :py:class:`~dbprocessing.inspector.inspector`. See
   also :py:meth:`~dbprocessing.DButils.DButils.addFile`.

.. sql:column:: file_id

   Auto-incremented ID for this file, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   Name of the file, without path. The pathing is determined from
   :sql:column:`product.relative_path`.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: utc_file_date

   Single data "characterizing" the data within the file. For a file
   of a product on a ``DAILY`` timebase, this usually maps to the date
   of every timestamp within the file. However, a daily file may, due
   to conversions, include a small amount of data from the previous and
   following date, thus this is distinct from :sql:column:`utc_start_time`
   and :sql:column:`utc_stop_time`. Semantics on other timebases are not
   yet defined.
   (:py:class:`~sqlalchemy.types.Date`)

.. sql:column:: utc_start_time

   Timestamp of the first record in this file. The interpretation of this
   timestamp is not defined by dbprocessing.
   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: utc_stop_time

   Timestamp of the last record in this file. The interpretation of this
   timestamp is not defined by dbprocessing.
   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: data_level

   Numerical level of this file; somewhat redundant with
   :sql:column:`product.level`.
   (:py:class:`~sqlalchemy.types.Float`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: interface_version

   Version of the *file*. Full version is ``interface.quality.revision``.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   Version of the *file*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   Version of the *file*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: verbose_provenance

   Full command line which was used to build this file; in theory if the
   same codes and input files are in place, executing this command line
   will recreate the file.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: check_date

   Date the file was quality checked; unused. (Was meant to support the QA
   loop).
   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: quality_comment

   Comment from the quality check; unused.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: caveats

   Caveats on use of the file; unused.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: file_create_date

   Date/time the file was created.
   (:py:class:`~sqlalchemy.types.DateTime`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: met_start_time

   Mission Elapsed Time (MET) of first record in file, meant to correspond
   to :sql:column:`utc_start_time`. Not used by dbprocessing logic and
   interpretation is not defined by dbprocessing.
   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: met_stop_time

   MET of last record in file, corresponding to :sql:column:`utc_stop_time`.
   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: exists_on_disk

   Whether the file is believed to exist on disk, or is a historical
   record of a deleted file.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_checked

   Whether quality has been checked; part of the unused QA loop.
   (:py:class:`~sqlalchemy.types.Boolean`)

.. sql:column:: product_id

   This file is considered an instance of this product.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: shasum

   SHA-1 checksum of the file, calculated when ingested.
   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: process_keywords

   .. warning::
      This explanation may not be completely correct; this is not
      commonly used.

   When a product has keyword substitutions in the filename
   :sql:column:`~product.format` that are not directly calculatable by
   dbprocessing (not, e.g. date or version), the values of those
   keywords for this file are stored, allowing calculation of the
   filename.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: filecodelink

   Connects a single data file to the (single) code used to create it.
   A many-to-one relationship: many files are made from a single code.

.. sql:column:: resulting_file

   ID of the file created.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: source_code

   ID of the code used to create :sql:column:`resulting_file`.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`code.code_id`)

.. sql:table:: filefilelink

   Connects a single data file to the (potentially many) input files used
   to create it. A many-to-many relationship: each file may serve as input
   to multiple output files, and each output file may be created from
   multiple inputs. This table is expressed as pairs: each row links one
   output file to one of its input files.

.. sql:column:: source_file

   ID of the source (input) file for a particular pairing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: resulting_file

   ID of the resulting (output) file for a particular pairing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:table:: inspector

   An :ref:`inspector <concepts_inspectors>` is a small piece of
   code which examines (inspects) a file to determine its product and
   various metadata for dbprocessing; this table describes the codes.

   .. seealso::
      :py:class:`~dbprocessing.inspector.inspector`

.. sql:column:: inspector_id

   Auto-incremented ID for this inspector, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   Filename (only) of inspector module.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: relative_path

   Directory containing :sql:column:`filename`, relative to
   :sql:column:`mission.inspectordir`.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: description

   Human-readable description of an inspector. May be considered a name
   but usually longer than a name and usually not used for lookup.
   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: interface_version

   Version of the *inspector*. Full version is ``interface.quality.revision``.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   Version of the *inspector*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   Version of the *inspector*.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_interface_version

   .. warning:: The purpose of this column is unclear.

   Usually 1. May exist simply from copying the :sql:table:`code` definition.
   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: active_code

   Whether this inspector is active, i.e. actually executed to determine
   potential matches between files and products.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: date_written

   Date inspector was written; meant for human information only.
   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: shasum

   SHA-1 checksum of the inspector file; meant for validation but not
   currently used.
   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: newest_version

   .. warning:: This does not appear to be used, so its purpose is unclear.

   Whether this is the newest version of a particular inspector.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: arguments

   Space-separated list of ``key=value`` pairs, passed as keyword arguments
   to :py:class:`~dbprocessing.inspector.inspector.inspect`. This allows
   the same file to be used as an inspector for multiple products, by using
   different arguments.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: product

   ID of the product which this inspector identifies. Every inspector can
   identify on, and only one, product.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:table:: instrument

   Describes an instrument. An instrument is primarily a means of connecting
   related products for convenience (e.g. in queries and reprocessing);
   generally speaking it corresponds to a physical instrument. The hierarchy
   of association is :sql:table:`instrument`, :sql:table:`satellite`,
   :sql:table:`mission`, where each relation is many-to-one.

.. sql:column:: instrument_id

   Auto-incremented ID for this instrument, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: instrument_name

   Name of the instrument, normally short to make it easy to use in command
   line queries.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: satellite_id

   ID of the satellite of which this instrument is part.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`satellite.satellite_id`)

.. sql:table:: instrumentproductlink

   Connects each instrument to the products it is associated with. This is
   a many-to-many link: an instrument may have its data in several products,
   and a product may draw from several instruments. Most commonly a product
   is associated with only one instrument; having multiple instruments per
   product is not heavily used or tested.

   This table is expressed as pairs: each row links one product with one
   instrument.

   .. warning::
      The existence of :sql:column:`product.instrument_id` suggests a
      different approach than this.

.. sql:column:: instrument_id

   ID of the instrument in a pairing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`instrument.instrument_id`)

.. sql:column:: product_id

   ID of the product associated with the instrument in the same record.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:table:: logging

   Log of the state of :ref:`scripts_ProcessQueue_py` invocations. Every
   run creates a single record in this table, recording the state of
   processing and how it terminated.

.. sql:column:: logging_id

   Auto-incremented ID for each log entry, to maintain unique rows.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: currently_processing

   Is this instance of :ref:`scripts_ProcessQueue_py` still running.
   There should only be one instance running at a time, so this is used
   as a lock (:py:meth:`~dbprocessing.DButils.DButils.currentlyProcessing`).
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: pid

   Process ID of :ref:`scripts_ProcessQueue_py`.
   (:py:class:`~sqlalchemy.types.Integer`)

.. sql:column:: processing_start_time

   When this instance of :ref:`scripts_ProcessQueue_py` started.
   (:py:class:`~sqlalchemy.types.DateTime`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: processing_end_time

   When this instance of :ref:`scripts_ProcessQueue_py` completed.
   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: comment

   How :ref:`scripts_ProcessQueue_py` exited. In the event the processing
   flag was cleared manually with :ref:`scripts_clearProcessingFlag_py`,
   this includes the :option:`message <clearProcessingFlag.py message>`.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: mission_id

   ID of the mission on which this is executing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`mission.mission_id`)

.. sql:column:: user

   Username running the :ref:`scripts_ProcessQueue_py` process.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: hostname

   Name of the host on which :ref:`scripts_ProcessQueue_py` is running.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: logging_file

   .. warning:: This table appears to be unused.

   Likely intended to provide some sort of dbprocessing-level support for
   logging from data processing codes, but not used.

.. sql:column:: logging_file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: logging_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`logging.logging_id`)

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: code_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`code.code_id`)

.. sql:column:: comments

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: mission

   The :ref:`mission <concepts_missions>` for the data held in this database.
   A mission may pertain
   to multiple satellites, e.g. the Van Allen Probes mission had RBSP-A
   and RBSP-B. The hierarchy of association is :sql:table:`instrument`,
   :sql:table:`satellite`, :sql:table:`mission`, where each relation is
   many-to-one.

   This is the top-level table determining where dbprocessing looks for
   files and codes.

   .. warning::
      In theory a single database can contain multiple missions; in practice,
      this has always been a one-to-one, and many parts of the codebase assume
      just one mission. In particular, most command line arguments to
      specify "mission" really specify the database.

   A mission has many relevant directories; in older versions of the database,
   these were not all explicitly specified, and in newer versions they may
   often be null. In these cases a default is used; see
   :py:meth:`~dbprocessing.DButils.DButils.getDirectory`.

   .. note::
      Where specified in this table, directories are assumed to be absolute.
      If relative, they are relative to current directory, not any particular
      mission directory.

.. sql:column:: mission_id

   Auto-incremented ID for this code, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: mission_name

   Human-readable name of this mission, should be short for easy use in
   command line queries.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: rootdir

   All data paths are specified relative to this directory. Code and related
   paths are not.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

   .. seealso::

      :py:meth:`~dbprocessing.DButils.DButils.getMissionDirectory`

.. sql:column:: incoming_dir

   Directory from which new files are ingested for this mission.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

   .. seealso::

      :py:meth:`~dbprocessing.DButils.DButils.getIncomingPath`

.. sql:column:: codedir

   Data processing codes are specified relative to this directory.
   (:py:class:`~sqlalchemy.types.String`)

   .. seealso::

      :py:meth:`~dbprocessing.DButils.DButils.getCodeDirectory`

.. sql:column:: inspectordir

   Inspector module paths are specified relative to this directory.
   (:py:class:`~sqlalchemy.types.String`)

   .. seealso::

      :py:meth:`~dbprocessing.DButils.DButils.getInspectorDirectory`

.. sql:column:: errordir

   Outputs of failed data processing codes, both file outputs and stdout,
   are placed in this directory, by default ``'errors'`` in
   :sql:column:`codedir`.
   (:py:class:`~sqlalchemy.types.String`)

   .. seealso::

      :py:meth:`~dbprocessing.DButils.DButils.getErrorPath`

.. sql:table:: process

   A :ref:`process <concepts_processes>`, which converts files of input
   product(s) to a file of an output product.

.. sql:column:: process_id

   Auto-incremented ID for this process, mostly for cross-referencing
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: process_name

   Human-readable name of this process, normally short to make it easy
   to use in command line queries.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_product

   ID of the single output product.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: output_timebase

   Timebase of output files.
   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: extra_params

   Arguments to add to the processing command line. These are added before the
   code's :sql:column:`~code.arguments`.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: processqueue

   Queue of files which are to be evaluated as potential inputs to processes
   see :ref:`concepts_process_queue`. :std:option:`ProcessQueue.py -p` will
   evaluate all products which can be built using these as inputs, and create
   any which are out of date.

.. sql:column:: file_id

   ID of a file in the queue.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: version_bump

   Requested approach to processing. By default, will only run processes
   if the outputs are out of date. If this is set, processes are forced
   to run, and the specified version component of the output is incremented
   (0 for interface version, 1 for quality, 2 for revision.)
   (:py:class:`~sqlalchemy.types.SmallInteger`)

   .. seealso::

      :std:option:`reprocessByProduct.py --force`

.. sql:table:: product

   A generalization or "type" of a file; every file is an instance of a
   :ref:`product <concepts_products>`.

.. sql:column:: product_id

   Auto-incremented ID for this product, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: product_name

   Human-readable name of the product, normally short to make it easy to
   use in command line queries.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: instrument_id

   The instrument providing data for this product.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`instrument.instrument_id`)

   .. warning::
      The existence of :sql:table:`instrumentproductlink` suggests a
      different approach than this.

.. sql:column:: relative_path

   Location where data files of this product are stored, relative to
   :sql:column:`~mission.rootdir`. May contain fields to be filled
   (e.g. ``{Y}`` to have a by-year directory).
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: level

   Numerical level of this product; somewhat redundant with
   :sql:column:`file.data_level`.
   (:py:class:`~sqlalchemy.types.Float`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: format

   Filename template for files of this product. Normally contains fields
   to be filled (e.g. ``{Y}`` to include the year).
   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: product_description

   Human-readable description of the product. Usually not used for queries.
   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: productprocesslink

   Relates processes to the products that they need as inputs. Each record
   pairs a process with one of its input products and describes that
   relationship.

.. sql:column:: process_id

   ID of the process whose input product is described by this record.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`process.process_id`)

.. sql:column:: input_product_id

   ID of one input product for the process of this record.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: optional

   Whether :sql:column:`input_product_id` is an optional product, in which
   case the process can execute without it, or not. A product will only
   execute if all its required inputs are available. If all inputs are optional,
   it will only execute if at least one optional input is available.
   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: yesterday

   Number of days in the past of this product to include as inputs to the
   process. For instance, if this is 2, then in processing day ``n``, days
   ``n-1`` and ``n-2`` are also provided as inputs. Behavior is undefined
   for timebases other than ``DAILY``.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: tomorrow

   As :sql:column:`yesterday`, but specifying days in the future.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: release

   Tracks the files that are present in a public release. For every release,
   each file in that release has a record in this table. This is a
   many-to-many relationship: each file may be in multiple releases, which
   may contain multiple files.

.. sql:column:: file_id

   ID of the file which is included in the release.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: release_num

   Release number of which :sql:column:`file_id` is a part.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: satellite

   Describes an satellite. A satellite is primarily a means of connecting
   related products for convenience (e.g. in queries and reprocessing);
   generally speaking it corresponds to a physical instrument. The hierarchy
   of association is :sql:table:`instrument`, :sql:table:`satellite`,
   :sql:table:`mission`, where each relation is many-to-one.

.. sql:column:: satellite_id

   Auto-incremented ID for this satellite, mostly for cross-referencing.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: satellite_name

   Name of the satellite, normally short to make it easy to use in command
   line queries.
   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: mission_id

   ID of the mission of which this satellite is part.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`mission.mission_id`)

.. sql:table:: unixtime

   Stores the start and stop time for each file as a count of seconds since
   the Unix epoch. This makes certain lookups faster.

.. sql:column:: file_id

   ID of the file for which this record stores the start/stop times.
   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: unix_start

   :sql:column:`~file.utc_start_time` for this file expressed as seconds
   since Unix epoch.
   (:py:class:`~sqlalchemy.types.Integer`)

.. sql:column:: unix_stop

   :sql:column:`~file.utc_stop_time` for this file expressed as seconds
   since Unix epoch.
   (:py:class:`~sqlalchemy.types.Integer`)
