*******************
Configuration Files
*******************

Basics
======

Configuration files in this project are INI files as supported by the
:mod:`configparser` Python module. Refer to that documentation for
all the detailed features of the format.

The files consist of sections, delimited by a ``[section]`` header and
containing ``name=value`` properties. Leading whitespace is removed
from values. Lines starting with ``#`` or ``;`` are treated as
comments.

.. _configurationfiles_addFromConfig:

addFromConfig.py
================
.. code-block:: ini 

    # Honored database substitutions used as {Y}{MILLI}{PRODUCT}
    #       Y: 4 digit year
    #       m: 2 digit month
    #       b: 3 character month (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)
    #       d: 2 digit day
    #       y: 2 digit year
    #       j: 3 digit day of year
    #       H: 2 digit hour (24-hour time)
    #       M: 2 digit minute
    #       S: 2 digit second
    #       MILLI: 3 digit millisecond
    #       MICRO: 3 digit microsecond
    #       QACODE: the QA code (ok|ignore|problem)
    #       VERSION: version string, interface.quality.revision
    #       DATE: the UTC date from a file, same as Ymd
    #       MISSION: the mission name from the db
    #       SPACECRAFT: the spacecraft name from the db
    #       PRODUCT: the product name from the db


    ##########################
    # MANUAL
    #
    # Loops over the configuration file and if the DB does not have the mission,
    # satellite, instrument entries present they are added, skipped otherwise.
    # The product and process entries must be unique and not present.
    #
    # THERE IS CURRENTLY NO UPDATE IN THE DB BASED ON THIS CONFIG SCRIPT


    ##################
    # Required elements
    #
    # [mission]  <- once and only once with
    #   rootdir  (string)
    #   mission_name  (string)
    #   incoming_dir  (string)
    # [satellite] <- once and only once with
    #   satellite_name  (string)
    # [instrument] <- once and only once with
    #   instrument_name  (string)
    ##### products and inspector are defined together since they are a one-to-one
    # [product] <- multiple entries each starting with "product" then a unique identifer
    #   product_name  (string)
    #   relative_path  (string)
    #   level  (float)
    #   format  (string)
    #   product_description (string)
    #   inspector_filename (string)
    #   inspector_relative_path (string)
    #   inspector_description (string)
    #   inspector_version (version e.g. 1.0.0)
    #   inspector_output_interface (integer)
    #   inspector_active (Boolean e.g. True or 1 or False or 0)
    #   inspector_date_written (date e.g. 2013-07-12)
    #   inspector_newest_version  (Boolean e.g. True or 1 or False or 0)
    #   inspector_arguments (string)
    #### processes and codes operate on the names of the products, they can be in
    #### this config file or already in the db codes are one-to-one with processes
    # [process] <- multiple entries each starting with "process" then a unique identifer
    #   process_name (string)
    #   output_product (string)  - identifer from section heading
    #   output_timebase  (string, FILE/DAILY/WEEKLY/MONTHLY/YEARLY)
    #   extra_params (string)
    ## A collection of input names entered as such
    ## the required portion is "optional_input" or "required_input" then some
    ## unique identifer on the end
    #   optional_input1  (string) name of product - identifer from section heading
    #   optional_input2  (string) name of product - identifer from section heading
    #   optional_input3  (string) name of product - identifer from section heading
    #   required_input1  (string) name of product - identifer from section heading
    #   required_input2  (string) name of product - identifer from section heading
    #   required_input3  (string) name of product - identifer from section heading
    ## code is entered as part of process
    #   code_filename (string)
    #   code_relative_path (string)
    #   code_start_date (date, 2000-01-01)
    #   code_stop_date  (date, 2050-12-31)
    #   code_description (string)
    #   code_version  (version e.g. 1.0.0)
    #   code_output_interface  (integer)
    #   code_active (Boolean e.g. True or 1 or False or 0)
    #   code_date_written   (date, 2050-12-31)
    #   code_newest_version (Boolean e.g. True or 1 or False or 0)
    #   code_arguments (string)
    [mission]
    mission_name = testDB
    rootdir = /home/myles/dbprocessing/test_DB
    incoming_dir = L0

    [satellite]
    satellite_name = {MISSION}-a

    [instrument]
    instrument_name = rot13

    [product_input_first]
    product_name = {MISSION}_rot13_L0_first
    relative_path = L0
    level = 0.0
    format = testDB_{nnn}_first.raw
    product_description = 
    inspector_filename = rot13_L0_first.py
    inspector_relative_path = codes/inspectors
    inspector_description = Level 0
    inspector_version = 1.0.0
    inspector_output_interface = 1
    inspector_active = True
    inspector_date_written = 2016-05-31
    inspector_newest_version = True
    inspector_arguments = 

    [product_input_second]
    product_name = {MISSION}_rot13_L0_second
    relative_path = L0
    level = 0.0
    format = testDB_{nnn}_sec.raw
    product_description = 
    inspector_filename = rot13_L0_second.py
    inspector_relative_path = codes/inspectors
    inspector_description = Level 0
    inspector_version = 1.0.0
    inspector_output_interface = 1
    inspector_active = True
    inspector_date_written = 2016-05-31
    inspector_newest_version = True
    inspector_arguments = 

    [product_concat]
    product_name = {MISSION}_rot13_L1
    relative_path = L1
    level = 1.0
    format = testDB_{nnn}.cat
    product_description = 
    inspector_filename = rot13_L1.py
    inspector_relative_path = codes/inspectors
    inspector_description = Level 1
    inspector_version = 1.0.0
    inspector_output_interface = 1
    inspector_active = True
    inspector_date_written = 2016-05-31
    inspector_newest_version = True
    inspector_arguments = 

    [product_rot13]
    product_name = {MISSION}_rot13_L2
    relative_path = L2
    level = 2.0
    format = testDB_{nnn}.rot
    product_description = 
    inspector_filename = rot13_L2.py
    inspector_relative_path = codes/inspectors
    inspector_description = Level 2
    inspector_version = 1.0.0
    inspector_output_interface = 1
    inspector_active = True
    inspector_date_written = 2016-05-31
    inspector_newest_version = True
    inspector_arguments = 

    [process_rot13_L0-L1]
    process_name = rot_L0toL1
    output_product = product_concat
    output_timebase = FILE
    extra_params = 
    required_input1 = product_input_first
    required_input2 = product_input_second
    code_filename = run_rot13_L0toL1.py
    code_relative_path = scripts
    code_start_date = 2010-09-01
    code_stop_date = 2020-01-01
    code_description = Python L0->L1 
    code_version = 1.0.0
    code_output_interface = 1
    code_active = True
    code_date_written = 2016-05-31
    code_newest_version = True
    code_arguments = 
    code_cpu = 1
    code_ram = 1

    [process_rot13_L1-L2]
    process_name = rot_L1toL2
    output_product = product_rot13
    output_timebase = FILE
    extra_params = 
    required_input1 = product_concat
    code_filename = run_rot13_L1toL2.py
    code_relative_path = scripts
    code_start_date = 2010-09-01
    code_stop_date = 2020-01-01
    code_description = Python L1->L2
    code_version = 1.0.0
    code_output_interface = 1
    code_active = True
    code_date_written = 2016-05-31
    code_newest_version = True
    code_arguments = 
    code_cpu = 1
    code_ram = 1

mission
-------
This section defines the :ref:`mission <concepts_missions>`; this configuration
only supports one mission per database. The keys map directly to the fields
in the :sql:table:`mission` table:

   * :sql:column:`~mission.rootdir`
   * :sql:column:`~mission.mission_name`
   * :sql:column:`~mission.incoming_dir`

satellite
---------
Although a database may contain multiple satellites, products can only be
defined within a single config file for a single satellite. (Multiple config
files can be used to add multiple satellites.)

The ``satellite_name`` key specified the name of the satellite as in
:sql:column:`satellite.satellite_name`; the instrument and products in
this file will all be associated with the satellite of this name.

instrument
----------
As with ``satellite``, this section specifies, via ``instrument_name``,
the instrument (:sql:column:`instrument.instrument_name`) with which
all the products in this file will be associated.

product
-------
Each section starting with ``product_`` defines a
:ref:`product <concepts_products>`. An entry in the :sql:table:`product`
table will be added for each section; the keys map to columns of the table.

   product_name
      :sql:column:`~product.product_name`

   relative_path
      :sql:column:`~product.product_name`

   level
      :sql:column:`~product.product_name`

   format
      :sql:column:`~product.product_name`

   product_description
      :sql:column:`~product.product_name` (may be blank)

Boolean values can be expressed in the config file in a number of
ways, e.g. ``0``/``1``, ``True``/``False``. Dates are expressed as
``YYYY-MM-DD``.

A single entry in the :sql:table:`inspector` table is also added,
associated with this product (via :sql:column:`inspector.product`).

   inspector_filename
      :sql:column:`~inspector.filename`

   inspector_relative_path
      :sql:column:`~inspector.relative_path`

   inspector_description
      :sql:column:`~inspector.description`

   inspector_version
      :sql:column:`~inspector.interface_version`.
      :sql:column:`~inspector.quality_version`.
      :sql:column:`~inspector.revision_version`

   inspector_output_interface
      :sql:column:`~inspector.output_interface_version`

   inspector_active
      :sql:column:`~inspector.active_code`

   inspector_date_written
      :sql:column:`~inspector.date_written`

   inspector_newest_version
      :sql:column:`~inspector.newest_version`

   inspector_arguments
      :sql:column:`~inspector.arguments` (may be blank)

process
-------
Each section starting with ``process_`` defines a
:ref:`process <concepts_processes>`. An entry in the :sql:table:`process`
table will be added for each section; the keys map to columns of the table:

   process_name
      :sql:column:`~process.process_name`

   output_product
      :sql:column:`~process.output_product`. This is specified in the config
      file as a name, and mapped to the :sql:column:`~product.product_id`.
      If the product exists in the same config file, it must be the name
      of the product section, including the ``product_``. If the product
      is already in the database, it must be specified as the
      :sql:column:`~product.product_name`. (Making this more flexible is
      planned.)

   output_timebase
      :sql:column:`~process.output_timebase`

   extra_params
      :sql:column:`~process.extra_params`

Keys starting with ``optional_input`` or ``required_input`` create
entries in the :sql:table:`productprocesslink` table. They must be
named sequentially, e.g.:

   * ``required_input1``
   * ``required_input2``
   * ``optional_input1``

etc.

The *values* of each entry are ordinarily the name of a product. As
with the output product, if the product exists in the same config
file, it must be the name of the product section, including the
``product_``. If the product is already in the database, it must be
specified as the :sql:column:`~product.product_name`.

If the key starts with ``optional``, the
:sql:column:`~productprocesslink.optional` column is ``True``; otherwise
it is ``False``.

To specify the inclusion of previous or following days of input data
(i.e. to set :sql:column:`~productprocesslink.yesterday` and
:sql:column:`~productprocesslink.tomorrow`, use a different format, of
``("name", days_before, days_after)``. For example, to specify the
product from section ``product_level2`` as a required input, and use
one day prior to the current day as input and no addition days after,
use:

.. code-block:: ini

   required_input1 = ("product_level2", 1, 0)

The product name is only in quotes if using this form.

Finally, an entry is created in the :sql:table:`code` table, with
:sql:column:`~code.process_id` set to associate it with this
process. Ini key to column mappings are:

   code_filename
      :sql:column:`~code.filename`

   code_relative_path
      :sql:column:`~code.relative_path`

   code_start_date
      :sql:column:`~code.code_start_date`

   code_stop_date
      :sql:column:`~code.code_stop_date`

   code_description
      :sql:column:`~code.code_description`

   code_version
      :sql:column:`~inspector.interface_version`.
      :sql:column:`~inspector.quality_version`.
      :sql:column:`~inspector.revision_version`

   code_output_interface
      :sql:column:`~code.output_interface_version`

   code_active
      :sql:column:`~code.active_code`

   code_date_written
      :sql:column:`~code.date_written`

   code_newest_version
      :sql:column:`~code.newest_version`

   code_arguments
      :sql:column:`~code.arguments`

   code_cpu
      :sql:column:`~code.cpu`

   code_ram
      :sql:column:`~code.ram`

.. _configurationfiles_coveragePlot:

coveragePlot.py
===============
.. code-block:: ini 

    #############################
    # sample config file
    #############################
    ##################
    # Substitutions
    # {TODAY}
    # {N DAYS}  -> add N days to the previous where N is an int


    ##################
    # Required elements

    [settings]
    mission = ~/RBSP_MAGEIS.sqlite
    outformat = pdf
    filename_format = MagEIS_L3_Coverage_{TODAY}
    startdate = 20120901
    enddate = {TODAY} + {7 DAYS}


    ###############################################
    # Plots
    ###############################################
    [panel]
    # in the panel section we define what will be plotted
    # N keys pf plotN define subplots
    # daysperplot gives the days per plot that will be on each page
    plot1 = plot1
    plot2 = plot2
    daysperplot = 60
    title = MagEIS L3 Coverage
    preset = green
    missing = red
    expected = grey

    [plot1]
    # in the plot section rows are defined bottom up
    # ylabel is what to put on the plot ylabel
    # productN is the product to plot
    # yticklabelN is what to call each product
    # productN_glob is a glob that a file has to match in order to be valid
    # productN_version is a minimum version allowed for files (e.g. 4.0.0)
    ylabel = RBSP-A
    product1 = rbspa_int_ect-mageisLOW-L3
    product2 = rbspa_int_ect-mageisM35-L3
    product3 = rbspa_int_ect-mageisM75-L3
    product4 = rbspa_int_ect-mageisHIGH-L3
    product5 = rbspa_int_ect-mageis-L3
    yticklabel1 = LOW
    yticklabel2 = M35
    yticklabel3 = M75
    yticklabel4 = HIGH
    yticklabel5 = FULL


    [plot2]
    ylabel = RBSP-B
    product1 = rbspb_int_ect-mageisLOW-L3
    product2 = rbspb_int_ect-mageisM35-L3
    product3 = rbspb_int_ect-mageisM75-L3
    product4 = rbspb_int_ect-mageisHIGH-L3
    product5 = rbspb_int_ect-mageis-L3
    yticklabel1 = LOW
    yticklabel2 = M35
    yticklabel3 = M75
    yticklabel4 = HIGH
    yticklabel5 = FULL

.. _configurationfiles_makeLatestSymlinks:

makeLatestSymlinks.py
=====================
.. code-block:: ini

    [isois]
    # Directory containing the data files
    sourcedir = ~/dbp_py3/data/ISOIS/level1/
    # Directory to make the symlinks in
    destdir = ~/tmp/
    # First date to link
    startdate = 2010-01-01
    # Last date to link
    enddate = 2021-01-01
    # Number of days before present not to link (e.g. to keep internal-only)
    deltadays = 60
    # glob for files to match
    filter = psp_isois_l1-sc-hk_*.cdf
    # Link directories as well as files
    linkdirs = True
    # Mode to use when making output directory
    outmode = 775
    # Do not limit based on date (i.e., ignore date options; they're still required)
    nodate = False

