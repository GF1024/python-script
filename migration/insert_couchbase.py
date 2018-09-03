# -*- coding:UTF-8 -*-
import json
import copy
import uuid
import os
import urlparse
import urllib2
import shutil
from couchbase.bucket import Bucket
import multiprocessing
#mys3 = boto3.resource('s3')
#s3 = boto3.resource('s3', region_name = "cn-north-1")
#conn_src  = Bucket('couchbase://127.0.0.1:8091/catalogue')
#conn_src  = Bucket('couchbase://47.94.135.179:8091/catalogue') # Production
global_src  = Bucket('couchbase://127.0.0.1:8091/catalogue')
#lobal_src  = Bucket('couchbase://localhost:8091/catalogue')



def insertData(key, data):
    global_src.insert(key, data) 

#prod-hsm-assets.s3.amazonaws.com
def insertCouchbase(fileList):
    productData = json.load(open(fileList))
    idkey = productData["id"] 
    files = productData["files"]
    print productData["id"]
    for file in files:
        myparsed = urlparse.urlparse(file['url'])
        file['url'] = file["url"].replace(myparsed.netloc,"juran-prod-assets.s3.cn-north-1.amazonaws.com.cn")
    productData["tenantProducts"]["ezhome"] = productData["tenantProducts"]["hsm"]
    del productData["tenantProducts"]["hsm"]
    productData["tenantProducts"]["ezhome"]["categories"]=["c8e00a93-a9fb-49d0-9451-f6f27db85e5a"]
    for attr in productData["attributes"]:
        if attr["typeId"] =='6d6d224e-bae0-4045-8c98-b044332ee7aa':
            attr["typeId"] = "dbd9b78c-fbae-4931-ac30-ff218059b3c6"
            attr["values"] = ["434871ea-2869-4622-9b2f-80a9fd1ed47f"]
    insertData(idkey,productData)

def parRun(fileList):
    ps = [ multiprocessing.Process(target = insertCouchbase,args=(t,)) for t in fileList]
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
   # brands = []
    for rt, dirs, files in os.walk(materDir):
        for f in files:
            #data = json.load(open(os.path.join(rt,f)))
            #for brand in data["brandsIds"]:
               # brands.append(brand)
            process(os.path.join(rt,f))
   # brandIds = []
   # for bran in brands:
        #if bran not in brandIds:
         #   brandIds.append(bran)
    #with open("newbrand.txt","a") as stream:
       # for brand2 in brandIds:
        #    stream.write(brand2)
          #  stream.write("\n")
        #stream.close()

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--file", action="store", type="string", dest="file")
    (options, args) = parser.parse_args()

    if not options.file:
        print "Usage: python ./XXXXXXXX.py --file ./path/to/id/list"
        exit(1)

    run(options.file)

