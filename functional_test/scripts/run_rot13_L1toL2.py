#!/usr/bin/env python
from optparse import OptionParser

def doProcess(infile, outfile):
    with open(outfile, 'w') as output:
        with open(infile) as infile:
            output.write(infile.read().encode('rot13'))

if __name__ == '__main__':
    usage = "usage: %prog infile outfile"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) is not 2:
        parser.error("incorrect number of arguments")

    infile = args[0]
    outfile = args[-1]

    print "infile", infile
    print "outfile", outfile
    doProcess(infile, outfile)
