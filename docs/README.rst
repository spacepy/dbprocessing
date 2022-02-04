dbprocessing README
===================
``dbprocessing`` is a Python-based, database-driven process controller which
automates the production of derived data products upon the arrival of new
input data. Although originally written for Heliophysics data, it is
intended to be flexible enough to manage most forms of digital time-series
data.

What dbprocessing does
----------------------
Given a description of relationship between data files, a set of codes
to process data files to derived products, and input files themselves,
``dbprocessing`` iteratively runs the appropriate codes to make all
possible output files.

``dbprocessing`` delegates the details of producing files to
mission-specific processing codes.  A processing code must have a
command-line interface and produce a single output file from one or
more inputs. There are no language restrictions; dbprocessing has been
used with C, Interactive Data Language (IDL), Java, and Python.

Support for a file format requires about 30 lines of Python to
identify the product and extract required metadata, which can support
many different products

When new versions of codes or input files are provided, codes are re-run
to ensure all outputs are up to date.

Current Status
--------------
``dbprocessing`` has been used in production for nine years in several
different projects; however, this has always been with the direct support
of the developers.

``dbprocessing`` should be considered to be in an early beta
state. It is not currently suitable for use without developer support;
however, if you are considering dbprocessing to support a mission or
project, the developers would be happy to work with you.
The developers are working daily to improve the maturity of the code,
documentation, and the infrastructure supporting development.

Relationship to SpacePy
-----------------------
Several ``dbprocessing`` developers are also SpacePy developers. The SpacePy
organization is hosting ``dbprocessing`` and providing community support
as ``dbprocessing`` is prepared for the public and grows its own community.
The SpacePy developers are not, as a whole, responsible for ``dbprocessing``.

``dbprocessing`` is not a component of SpacePy, nor does it require SpacePy.
SpacePy is generally useful in processing Heliophysics data, e.g. in the
codes that ``dbprocessing`` manages.

Development
-----------
Development of ``dbprocessing`` is primarily supported by the projects
which make use of it to deliver data. Development is performed in the public
github repository at <https://github.com/spacepy/dbprocessing/>.
