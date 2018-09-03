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
s3 = boto3.resource('s3')

#bucketName = "ezhome-uat-pim-assets"
bucketName = "juran-prod-assets"
#bucketName = "prod-hsm-assets"

def downloadAndUpload(filePath):
    productInfo = json.load(open(filePath))
    for file in productInfo["files"]:
        productId = productInfo["id"]
        metaData = file["metaData"]
        print productId+"__"+metaData
        myparsed = urlparse.urlparse(file['url'])
        filename = os.path.basename(myparsed.path)
        dir = myparsed.path[1:len(myparsed.path)]
        try:
            if metaData == "androidDat" or metaData == "modelNormalized":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "isoResizedW160H160" or metaData == "isoAuto" or metaData == "iso":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"image/jpeg"})
            elif metaData == "iosDat":
                s3.meta.client.upload_file( "files/"+productId+"/ios-model.dat", bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "modelGzNormalized" or metaData == "modelBinary" or metaData == "modelGz":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"text/plain","ContentEncoding":"gzip"})
            elif metaData == "vrscene":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/zip"})
            elif metaData == "objInfoExtension":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "ue4":
                s3.meta.client.upload_file("files/"+productId+"/ue4.zip", bucketName, dir, ExtraArgs={"ContentType":"application/x-zip-compressed"})
            elif metaData == "extension":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "renderImage":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"image/jpeg"})
            elif metaData == "obj":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "snapPlane":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/x-tgif"})
            elif metaData == "modelPng":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"image/png","CacheControl":"max-age=86400"})
            elif metaData == "txtr":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"image/png"})
            elif metaData == "scene":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName, dir, ExtraArgs={"ContentType":"application/octet-stream"})
            elif metaData == "topView":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName,os.path.dirname(myparsed.path)+'/Top.png' , ExtraArgs={"ContentType":"image/png","CacheControl":"max-age=86400"})
            elif metaData == "topViewLarge":
                s3.meta.client.upload_file( "files/"+productId+"/"+filename, bucketName,os.path.dirname(myparsed.path)+'/Top-large.png' , ExtraArgs={"ContentType":"image/png"})
        except Exception as e:
            print str(e)
            with open("uploadfailed_log.txt", "a") as err:
                err.write(productId+"_"+metaData)
                err.write("\n")

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
