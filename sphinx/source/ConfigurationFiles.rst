Configuration Files
===================

Basics
------
Configuration files in this project are INI files. The configuration file consists of sections,
led by a [section] header and followed by name=value properties.

Note that leading whitespace is removed from values.
The optional values can contain format strings which refer to other values in the same section,
or values in a special DEFAULT section. Additional defaults can be provided on initialization
and retrieval.

Note that leading whitespace is removed from values. Configuration files may include comments,
prefixed by '#' or ';'. Comments may appear on their own in an otherwise empty line, or may be
entered in lines holding values or section names. In the latter case, they need to be preceded
by a whitespace character to be recognized as a comment. (For backwards compatibility, only ;
starts an inline comment, while # does not.)


coveragePlot.py
---------------
.. code-block:: ini 

    #############################
    # sample config file
    #############################
    ## ##################
    ## # Substitutions
    ## # {TODAY}
    ## # {N DAYS}  -> add N days to the previous where N is an int


    ## ##################
    ## # Required elements

    [settings]
    mission = ~/RBSP_MAGEIS.sqlite
    outformat = pdf
    filename_format = MagEIS_L3_Coverage_{TODAY}
    startdate = 20120901
    enddate = {TODAY} + {7 DAYS}


    ## ###############################################
    ## # Plots
    ## ###############################################
    [panel]
    ## # in the panel section we define what will be plotted
    ## # N keys pf plotN define subplots
    ## # daysperplot gives the days per plot that will be on each page
    plot1 = plot1
    plot2 = plot2
    daysperplot = 60
    title = MagEIS L3 Coverage
    preset = green
    missing = red
    expected = grey

    ## [plot1]
    ## # in the plot section rows are defined bottom up
    ## # ylabel is what to put on the plot ylabel
    ## # productN is the product to plot
    ## # yticklabelN is what to call each product
    ## # productN_glob is a glob that a file has to match in order to be valid
    ## # productN_version is a minimum version allowed for files (e.g. 4.0.0)
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


    ## [plot2]
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

addFromConfig.py
----------------
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