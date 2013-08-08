#!/usr/bin/python

import re, os, sys

if __name__ == "__main__":


	for line in sys.stdin:
		(reskey, count) = line.strip().split("\t")
		(wtp, pixid) = reskey.split("_____");
		print wtp
		
	
