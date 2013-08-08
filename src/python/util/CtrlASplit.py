#!/usr/bin/python

import re, os, sys, fileinput
import CompJava

if __name__ == "__main__":


	for line in sys.stdin:
		toks = line.strip().split("\x01")
		
		print "\t".join(toks)
