#!/usr/bin/python

import re, os, sys, fileinput
			
# This is just a wrapper around the Java program.			
			
if __name__ == "__main__":

	hadcall = "hadoop jar /mnt/jars/adnetik.jar com.adnetik.data_management.MissingFileCheck"
	os.system(hadcall)
