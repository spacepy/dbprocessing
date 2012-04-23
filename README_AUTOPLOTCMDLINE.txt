Description of how to use autoplot in command line mode.
========================================================

Reiner Friedel, October 6, 2010

General
-------

To call autoplot you'll need the full complete .jar file available
locally. This is currently located at 

/u/ectsoc/codes/soc-sdc/autoplot/AutoplotAll.jar

You use a direct call to the java executable (should be on path) with
a set of arguments.

Java supports being called called in a "headless" mode - no
widgets will open. use the switch

-Djava.awt.headless=true

Autoplot internally can be controlled through the use of jython
scripts, through which you have access to all the commands that make
up the autoplot "language". Anything you can do in the widget you can
do in a jython script that is run by autoplot. 

Note: jython is a lightweight puthon port into native java. Do not
expect anything but the standard python modules to be available!

So the idea is to call Autoplot with the --script switch and to
specify a .py script that it should execute. You can pass arguments to
that script too.

This script can be simple or more comlicated - make a single plot, or
make a whole list of them. We need to decide what makes more sense, as
calling up autoplot through java does take time...

Note: to launch the autoplot widget for the local .jar just execute

java -cp /u/ectsoc/codes/soc-sdc/autoplot/AutoplotAll.jar
org.virbo.autoplot.AutoplotUI

NOTE: this verison of the .jar does NOT contain the cdf libraries - so
don't try to load a cdf file...

Implementation
--------------

All files located under
 
/n/toaster/u/ectsoc/codes/soc-sdc/autoplot

cmdl_autoplot:

This is the top level executable shell script to run autoplot from the
command line. It takes three arguments:

 $1 -  The .vap file to for the plot to be made
 $2 -  The date string for which the plot is to be made (yyymmdd0
 $3 -  The output filename for the plot.

This script calls autoplot with the cmdl_autoplot.jy script .


cmdl_autoplot.jy:

The jython script that autoplot executes. 


products/

Subdirectry containg the .vap products for command line autoplot to
use.

Currently for testing there is one .vap:
Test-one_R0_evinst-L1_diag1.vap

The .vap must be set up with an aggregated URI or the fata
source(s). Here for the Test satellite, instrument evinst, L1
diagnostic plot (it's an ascii file with a sin wave that is plotted. 



