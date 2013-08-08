#!/usr/bin/python

import sys, os, LocalConf, JavaCall

if __name__ == "__main__":

	JavaCall.runCall(sys.argv[1], sys.argv[2:], grabhost="gandalf.adnetik.com")

