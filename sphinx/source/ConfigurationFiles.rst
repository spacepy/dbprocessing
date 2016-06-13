Configuration Files
===================

Basics
------
The configuration file consists of sections, led by a [section] header and followed by
name: value entries, name=value is also accepted.

Note that leading whitespace is removed from values.
The optional values can contain format strings which refer to other values in the same section,
or values in a special DEFAULT section. Additional defaults can be provided on initialization
and retrieval. Lines beginning with '#' or ';' are ignored and may be used to provide comments.

Configuration files may include comments, prefixed by specific characters (# and ;).
Comments may appear on their own in an otherwise empty line, or may be entered in lines
holding values or section names. In the latter case, they need to be preceded by a whitespace
character to be recognized as a comment. (For backwards compatibility, only ; starts an inline
comment, while # does not.)


coveragePlot.py
---------------
test

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


TODO
----
Add in an example and describe it
