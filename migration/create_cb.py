# -*- coding:UTF-8 -*-
import json
import copy
import uuid
import os
import urlparse
import urllib2
import shutil
from couchbase.bucket import Bucket
#mys3 = boto3.resource('s3')
#s3 = boto3.resource('s3', region_name = "cn-north-1")
#conn_src  = Bucket('couchbase://127.0.0.1:8091/catalogue')
conn_src  = Bucket('couchbase://127.0.0.1:8091/catalogue') # Production
global_src  = Bucket('couchbase://127.0.0.1:8091/catalogue')
#lobal_src  = Bucket('couchbase://localhost:8091/catalogue')

#bucketName = "ezhome-uat-pim-assets"
#bucketName = "juran-prod-assets"


def getData(key):
    data = None
    data = conn_src.get(key)
    return data.value

def insertData(key, data):
    global_src.insert(key, data) 

def updateData(key, data):
    global_src.set(key, data)

#prod-hsm-assets.s3.amazonaws.com
def createAndUpdateFiles(productData):
    print productData["id"] 
    files = productData["files"]
    #for file in files:
        #myparsed = urlparse.urlparse(file['url'])
        #file['url'] = file["url"].replace(myparsed.netloc,"juran-prod-assets.s3.cn-north-1.amazonaws.com.cn")
    return productData
def doProcess(line):
    
    data = json.loads(line)
    productId = data["id"]
    productInfo = getData(productId)
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
    #newData = createAndUpdateFiles(productData)
    #with open("files/"+str(productId)+".txt", "a") as stream:
        #stream.write(json.dumps(newData))
        #stream.write("\n")
        #stream.close()
    #with open("createfailed.txt", "a") as err:
        #try:
          #  if getData(productId) is None:
           #     insertData(productId,newData)
           # else:
            #    updateData(productId,newData)
           # for brand in newData["brandsIds"]:
             #   brandData = getData(brand)
             #   myparsed = urlparse.urlparse(brandData['url'])
             #   brandData["url"] = brandData["url"].replace(myparsed.netloc,"prod-hsm-assets.s3.amazonaws.com")
             #   if getData(brand) is None:
              #      insertData(brand,brandData)
             #   else:
                #    updateData(brand,brandData)
        #except Exception as e:
         #   print str(e)
         #   err.write(productId +"--"+newData["brandsIds"]+"---"+e)
         #   err.write("\n")

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

