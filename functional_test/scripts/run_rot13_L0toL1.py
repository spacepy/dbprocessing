#!/usr/bin/env python
from optparse import OptionParser

def doProcess(infiles, outfile):
    with open(outfile, 'w') as output:
        for fname in infiles:
            with open(fname) as infile:
                output.write(infile.read())

if __name__ == '__main__':
    usage = "usage: %prog [infiles] outfile"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.error("incorrect number of arguments")

    infiles = sorted(args[:-1])
    outfile = args[-1]

    print "infiles", infiles
    print "outfile", outfile
    doProcess(infiles, outfile)
