import requests
import traceback
import logging
from io import BytesIO
import unicodecsv as csv
from urllib import parse
from queue import Queue
from threading import Thread
import threading
import re
import json
import boto3
import datetime
import time
import os
import xlrd
from boto3.session import Session
from botocore.client import Config
from bs4 import BeautifulSoup
from config.defines import djangoSettings,logDir,LIBTECH_AWS_SECRET_ACCESS_KEY,LIBTECH_AWS_ACCESS_KEY_ID,LIBTECH_AWS_BUCKET_NAME,AWS_S3_REGION_NAME,BASEURL,AUTHENDPOINT,apiusername,apipassword
from datetime import datetime, timedelta
from nrega.crawler.commons.nregaFunctions import getCurrentFinYear,stripTableAttributes,getCenterAlignedHeading,htmlWrapperLocal,getFullFinYear,correctDateFormat,table2csv,array2HTMLTable,getDateObj,stripTableAttributesPreserveLinks,getFinYear
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Jobcard,PanchayatCrawlInfo,Worker,PanchayatStat,Report

def calculatePercentage(num,den,defaultPercentage=100):
  if den == 0:
    return defaultPercentage
  else:
    try:
      return int(int(num)*100/int(den))
    except:
      return defaultPercentage

def csv_from_excel(excelFile,csvFile):
    wb = xlrd.open_workbook(excelFile)
    sh = wb.sheet_by_name('Documents')
    your_csv_file = open(csvFile, 'wb')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)
    for rownum in range(sh.nrows):
        wr.writerow(sh.row_values(rownum))
    your_csv_file.close()

def getAuthToken():
  data={
      'username' : apiusername,
      'password' : apipassword
            }
  r=requests.post(AUTHENDPOINT,data=data)
  token=r.json()['token']
  return token

def getjcNumber(jobcard):
  jobcardArray=jobcard.split('/')
#  print(jobcardArray[1])
  if len(jobcardArray) > 1:
    jcNumber=re.sub("[^0-9]", "", jobcardArray[1])
  else:
    jcNumber='0'
  try:
    jcNumber=str(int(jcNumber))
  except:
    jcNumber='0'
  return jcNumber


def uploadReportAmazon(filepath,filedata,contentType):
  session = boto3.session.Session(aws_access_key_id=LIBTECH_AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=LIBTECH_AWS_SECRET_ACCESS_KEY)
  s3 = session.resource('s3',config=Config(signature_version='s3v4'))
  if contentType == "text/html":
    response=s3.Bucket(LIBTECH_AWS_BUCKET_NAME).put_object(ACL='public-read',Key=filepath, Body=filedata,ContentType=contentType)
  elif contentType == "application/json":
    response=s3.Bucket(LIBTECH_AWS_BUCKET_NAME).put_object(ACL='public-read',Key=filepath, Body=filedata,ContentType=contentType)
  else:
    response=s3.Bucket(LIBTECH_AWS_BUCKET_NAME).put_object(ACL='public-read',Key=filepath, Body=filedata)
  s3url="https://s3.%s.amazonaws.com/%s/%s" % (AWS_S3_REGION_NAME,LIBTECH_AWS_BUCKET_NAME,filepath)
  return s3url


def isReportUpdated(logger,pobj,finyear,reportType,reportThreshold=None):
  isUpdated=False
  if reportThreshold is None:
    reportThreshold = datetime.now() - timedelta(days=3)
  logger.info(reportThreshold)
  myReport=Report.objects.filter(location=pobj,reportType=reportType,finyear=finyear,modified__gt=reportThreshold).first()
  logger.info(myReport)
  if myReport is not None:
    isUpdated=True
  return isUpdated
def savePanchayatReport(logger,pobj,finyear,reportType,filedata,filepath,locationType=None,contentType=None):
  if ((locationType is None) or (locationType == "panchayat")):
    locationType='panchayat'
    locationCode=pobj.panchayatCode
  else:
    locationType='block'
    locationCode=pobj.blockCode

  if contentType is None:
    contentType="text/html"
  else:
    contentType=contentType
  code="%s_%s_%s" % (locationCode,finyear,reportType)
  logger.info(code)
  myReport=Report.objects.filter(code=code).first()
  if myReport is None:
    if locationType == 'block':
      myReport=Report.objects.create(block=pobj.block,finyear=finyear,reportType=reportType)
    else:
      myReport=Report.objects.create(panchayat=pobj.panchayat,finyear=finyear,reportType=reportType)

  s3url=uploadReportAmazon(filepath,filedata,contentType)
  myReport.reportURL=s3url
  myReport.save()

def saveReport(logger,pobj,finyear,reportType,filedata,contentType=None):
  if contentType is None:
    contentType="text/html"
  else:
    contentType=contentType
  if contentType == "text/csv":
    reportExtension="csv"
  else:
    reportExtension="html"
  myReport=Report.objects.filter(location=pobj,finyear=finyear,reportType=reportType).first()
  if myReport is None:
    myReport=Report.objects.create(location=pobj,finyear=finyear,reportType=reportType)
  filename="%s_%s_%s_%s.%s" % (reportType,pobj.slug,pobj.code,finyear,reportExtension)
  filepath=pobj.filepath.replace("filename",filename)
  s3url=uploadReportAmazon(filepath,filedata,contentType)
  myReport.reportURL=s3url
  myReport.save()
    
def saveReportOld(logger,pobj,finyear,reportType,filedata,filepath,contentType=None):
  if ((pobj.locationType is None) or (pobj.locationType == "panchayat")):
    locationType='panchayat'
    locationCode=pobj.panchayatCode
  elif (pobj.locationType == "block"):
    locationType='block'
    locationCode=pobj.blockCode
  elif (pobj.locationType == "district"):
    locationType="district"
    locationCode=pobj.districtCode

  if contentType is None:
    contentType="text/html"
  else:
    contentType=contentType
  code="%s_%s_%s" % (locationCode,finyear,reportType)
  logger.info(code)
  myReport=Report.objects.filter(code=code).first()
  if myReport is None:
    if locationType == "district":
      myReport=Report.objects.create(district=pobj.district,finyear=finyear,reportType=reportType)
    elif locationType == 'block':
      myReport=Report.objects.create(block=pobj.block,finyear=finyear,reportType=reportType)
    else:
      myReport=Report.objects.create(panchayat=pobj.panchayat,finyear=finyear,reportType=reportType)

  s3url=uploadReportAmazon(filepath,filedata,contentType)
  myReport.reportURL=s3url
  myReport.save()


def getReportHTML(logger,pobj,reportType,finyear,locationType=None):
  myhtml=None
  myReport=Report.objects.filter(location=pobj,finyear=finyear,reportType=reportType).first()
 #if locationType == 'district':
 #  myReport=Report.objects.filter(district=pobj.district,finyear=finyear,reportType=reportType).first()
 #elif locationType == 'block':
 #  myReport=Report.objects.filter(block=pobj.block,finyear=finyear,reportType=reportType).first()
 #else:
 #  myReport=Report.objects.filter(panchayat=pobj.panchayat,finyear=finyear,reportType=reportType).first()
  if myReport is None:
    error="Report not found"
  else:
    reportURL=myReport.reportURL
    if reportURL is not None:
      r=requests.get(reportURL)
      if r.status_code == 200:
        myhtml=r.content
        error=None
      else:
        myhtml=None
        error="Could not download %s " % str(reportURL)
    else:
      error="%s Report URL not found %s" % (str(myReport),finyear)
  return error,myhtml

def validateNICReport(logger,pobj,myhtml,jobcardPrefix=None):
  if jobcardPrefix is None:
    jobcardPrefix=pobj.jobcardPrefix
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  error="table not found"
  myTable=None
  tables=htmlsoup.findAll("table")
  for eachTable in tables:
    if jobcardPrefix in str(eachTable):
      myTable=eachTable
      error=None
  return error,myTable


def validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear=None,locationType=None,jobcardPrefix=None,preserveLinks=False,validate=True):
    error=None
    if finyear is None:
      finyearString=''
    else:
      finyearString="_"+finyear
    locationName=pobj.displayName
    filepath=pobj.filepath

    if validate == True:
      outhtml=''
      outhtml+=getCenterAlignedHeading(locationName)
      if finyear is not None:
        outhtml+=getCenterAlignedHeading("Financial Year: %s " % (getFullFinYear(finyear)))
        
      error,myTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=jobcardPrefix)
      if error is None:
        if preserveLinks == False:
          outhtml+=stripTableAttributes(myTable,"myTable")
        else:
          outhtml+=stripTableAttributesPreserveLinks(myTable,"myTable")
    else:
      outhtml=myhtml
      error=None
    
    if error is None:
      outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      saveReport(logger,pobj,finyear,reportType,outhtml)
    return error 

