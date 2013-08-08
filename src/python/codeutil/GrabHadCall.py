#!/usr/bin/python

import sys, os, LocalConf, hadJavaCall

if __name__ == "__main__":

	hadJavaCall.runHadoopCall(sys.argv[1], sys.argv[2:], grabhost="gandalf.adnetik.com")

