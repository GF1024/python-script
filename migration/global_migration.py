# -*- coding:UTF-8 -*-
import json
import os
import urlparse
import urllib2
import shutil
import boto3
from couchbase.bucket import Bucket
#mys3 = boto3.resource('s3')
s3 = boto3.resource('s3')
conn_src  = Bucket('couchbase://47.94.135.179:8091/catalogue') # Production


#bucketName = "ezhome-uat-pim-assets"
#bucketName = "juran-prod-assets"
bucketName = "prod-hsm-assets"

def getData(key):
    data = None
    data = conn_src.get(key)
    return data.value

def readFiles(productData,productId):
    if os.path.exists(productId):
        shutil.rmtree(productId)
    os.mkdir(productId)   
    files = productData["files"]
    for file in files:
        metaData = file["metaData"]
        if metaData == "scene":
            print file['url']
        myparsed = urlparse.urlparse(file['url'])
        filename = os.path.basename(myparsed.path)
        dir = myparsed.path[1:len(myparsed.path)]
        data = urllib2.urlopen(file['url']).read()
        try:
            if metaData == "scene":
                print productId
                with open(productId+"/"+filename, 'wb') as stream:
                    stream.write(data)
                    stream.close()
                s3.meta.client.upload_file(productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
        except Exception as e:
            print str(e)
            with open("uploadfailed_log.txt", "a") as err:
                err.write(productId+"_"+metaData)
                err.write("\n")

def doProcess(line):
    data = json.loads(line)

    productId = data["guid"]
    productName = data["name"]
    productEName = data["e_name"]

    productData = getData(productId)
    readFiles(productData,productId)

def process(filePath):
    items = loadProductIds(filePath)

    with open("uploadfailed.txt", "a") as err:
        for line in items:
            try:
                doProcess(line)
            except Exception as e:
                print str(e)
                err.write(line)
                err.write("\n")

def loadProductIds(filePath):
    fo = open(filePath, "r")
    lines = fo.readlines()
    fo.close()
    return lines

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--ids", action="store", type="string", dest="ids")
    (options, args) = parser.parse_args()

    if not options.ids:
        print "Usage: python ./XXXXXXXX.py --ids ./path/to/id/list"
        exit(1)

    process(options.ids)

