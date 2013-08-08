#!/usr/bin/python

import sys

if __name__ == "__main__":
	
	
	summap = {}

	for line in sys.stdin:
		
		toks = line.strip().split("\t")
		
		for i in range(len(toks)):
			summap.setdefault(i, 0)
			toadd = int(toks[i]) if toks[i].isdigit() else float(toks[i])
			summap[i] += toadd
		
	output = []
	for i in summap:
		output.append(str(summap[i]))
		
	print "%s" % ("\t".join(output))

