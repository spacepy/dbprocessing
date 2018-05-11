#!/usr/bin/env python
from optparse import OptionParser

def doProcess(infile):
	with open('out.txt', 'a') as f:
		f.write("What a time to be alive\n")

if __name__ == '__main__':
	usage = "usage: %prog infile outfile"
	parser = OptionParser(usage=usage)

	(options, args) = parser.parse_args()

	if len(args) != 1:
		parser.error("incorrect number of arguments")

	infile = args[0]

	print "infile", infile
	doProcess(infile)
