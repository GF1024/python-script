import boto3
import urllib2
import urlparse
import os
import commands
import json
import codecs
import time
import subprocess
import shutil
import multiprocessing
#s3 = boto3.resource('s3')


def downloadAndUpload(filePath):
    productInfo = json.load(open(filePath))
    if os.path.exists("files/"+productInfo["id"]):
        shutil.rmtree("files/"+productInfo["id"])
    os.mkdir("files/"+productInfo["id"]) 
    for file in productInfo["files"]:
        productId = productInfo["id"]
        metaData = file["metaData"]
        print productId+"__"+metaData
        myparsed = urlparse.urlparse(file['url'])
        if metaData == "iosDat":
            filename = "ios-model.dat"
        if metaData == "ue4":
            filename = "ue4.zip"
        else:
            filename = os.path.basename(myparsed.path)
        dir = myparsed.path[1:len(myparsed.path)]
        newUrl = file['url'].replace("https","http")
        try:
            data = urllib2.urlopen(newUrl).read()
        except Exception as e:
            print str(e)
            with open("downloadfailed.txt", "a") as err:
                err.write(file)
                err.write("\n")
        with open( "files/"+productId+"/"+filename, 'wb') as stream:
            stream.write(data)
            stream.close()


def parRun(fileList):
    ps = [ multiprocessing.Process(target = downloadAndUpload,args=(t,)) for t in fileList]
    for p in ps:p.start()
    for p in ps:p.join()

def process(filePath):
    fileList = []
    count = 0
    fileList.append(filePath)
    if len(fileList) == 10:
        parRun(fileList)
        fileList = []
    else :
        parRun(fileList)

def run(materDir):
    for rt, dirs, files in os.walk(materDir):
        for f in files:
           process(os.path.join(rt,f))

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file", action="store", type="string", dest="file")
    (options, args) = parser.parse_args()

    if not options.file:
        print "Usage: python ./mig_design.py --file ./path/to/idlist"
        exit(1)

    run(options.file)
