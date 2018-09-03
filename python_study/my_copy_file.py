import boto3
import urllib2
import urlparse
import os
import commands
import json
import codecs
import time
import subprocess
import multiprocessing
def loadUrl(filePath):
    of = open(filePath,"r")
    lines = of.readlines()
    of.close
    return lines

def process(filePath):
    urls = loadUrl(filePath)
    print len(urls)
	fileList = []
	for url in urls:
	    fileList.append(url)
		if len(fileList)==10:
		    runThead(fileList)
			fileList = []
	if fileList:
	    runThead(fileList)
    return

def run(folder):
	for rt,dirs,files in os.walk(folder):
		for f in files:
			process(os.path.join(rt,f))
if __name__ == '__name__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file",action="store", type="string",dest="file")
    (options,files) = parser.parse_args()
	  
    if not options.file:
	    print "Usage:python python.py  --file file"
	    exit(1)
    run(options.file)
