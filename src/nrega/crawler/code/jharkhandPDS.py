import os
from queue import Queue
from bs4 import BeautifulSoup
from io import BytesIO
import unicodecsv as csv
from threading import Thread
import threading
from logging.handlers import RotatingFileHandler
import sys
import time
import json
import requests
import boto3
from boto3.session import Session
from botocore.client import Config
from datetime import datetime
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
sys.path.insert(0, rootDir)
from config.defines import djangoSettings,logDir,LIBTECH_AWS_SECRET_ACCESS_KEY,LIBTECH_AWS_ACCESS_KEY_ID,LIBTECH_AWS_BUCKET_NAME,AWS_S3_REGION_NAME
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from nrega.crawler.commons.nregaSettings import startFinYear,panchayatCrawlThreshold,panchayatRetryThreshold,telanganaStateCode,panchayatAttemptRetryThreshold,apStateCode,crawlRetryThreshold,crawlProcessTimeThreshold,crawlerTimeThreshold,nregaPortalMinHour,nregaPortalMaxHour,jharkhandPDSListDownloadThreshold
from nrega.crawler.commons.nregaFunctions import getCurrentFinYear,stripTableAttributes,getCenterAlignedHeading,htmlWrapperLocal,getFullFinYear,correctDateFormat,table2csv,array2HTMLTable,getDateObj,stripTableAttributesPreserveLinks,getFinYear
#from crawlFunctions import crawlPanchayat,crawlPanchayatTelangana,libtechCrawler
from crawlerFunctions import crawlerMain
from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,savePanchayatReport,table2csv,getFullFinYear,loggerFetch
from nrega.crawler.code.commons import saveReport,savePanchayatReport,uploadReportAmazon,getjcNumber,isReportUpdated,getReportHTML,validateAndSave,validateNICReport,csv_from_excel,calculatePercentage
from nrega import models as nregamodels

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Jobcard,PanchayatCrawlInfo,Worker,PanchayatStat,CrawlRequest,PDSLocation,PDSStat

def crawlPDSDistrictBlocks(logger,locationType):
  if locationType == 'district':
    url="https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults"
    r=requests.get(url)
    if r.status_code == 200:
      myhtml=r.content
      mysoup=BeautifulSoup(myhtml,"lxml")
      districtSelect=mysoup.find("select",id="SeccCardholderRgiDistrictCode")
      options=districtSelect.findAll('option')
      stateLocation=PDSLocation.objects.filter(code='34').first()
      for option in options:
        districtName=option.text
        districtCode=option['value']
        if option['value'] != "":
          code=option['value']
          name=option.text
          logger.info("District Name %s Code %s " % (option.text,option['value']))
          myLocation=PDSLocation.objects.filter(code=code).first()
          if myLocation is None:
            myLocation=PDSLocation.objects.create(code=code)
          myLocation.locationType='district'
          myLocation.parentLocation=stateLocation
          myLocation.stateCode='34'
          myLocation.name=name
          myLocation.englishName=name
          myLocation.districtCode=code
          myLocation.displayName="Jharkhand-%s" % (name)
          myLocation.save()
  #Now for Crawling Blocks
  if locationType == 'village':
    objs=PDSLocation.objects.filter(locationType='block')
    for obj in objs:
      url="https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults"
      r=requests.get(url)
      if r.status_code == 200:
        cookies=r.cookies
        headers = {
         'Accept': 'text/javascript, text/html, application/xml, text/xml, */*',
         'Accept-Encoding': 'gzip, deflate, br',
         'Accept-Language': 'en-GB,en;q=0.5',
         'Connection': 'keep-alive',
         'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
         'Host': 'aahar.jharkhand.gov.in',
         'Referer': 'https://aahar.jharkhand.gov.in/secc_cardholders/searchRation',
         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
         'X-Prototype-Version': '1.6.1_rc2',
         'X-Requested-With': 'XMLHttpRequest',
         'X-Update': 'SeccCardholderRgiBlockCode',
       }

        data = {
         'data[SeccCardholder][rgi_district_code]': obj.districtCode
        }

        r = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/getRgiBlock', headers=headers, cookies=cookies, data=data)
        data = {
          'data[SeccCardholder][rgi_block_code]': obj.blockCode
        }
        headers['X-Update']='SeccCardholderRgiVillageCode'
        response = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/getRgiVillages', headers=headers, cookies=cookies, data=data)
        myhtml=response.content
        mysoup=BeautifulSoup(myhtml,"lxml")
        options=mysoup.findAll('option')
        for option in options:
          if option['value'] != "":
            code=option['value']
            name=option.text
            logger.info(f"{name}-{code}")
            myLocation=PDSLocation.objects.filter(code=code).first()
            if myLocation is None:
              myLocation=PDSLocation.objects.create(code=code)
            myLocation.locationType='village'
            myLocation.parentLocation=obj
            myLocation.stateCode=obj.stateCode
            myLocation.name=name
            myLocation.englishName=name
            myLocation.districtCode=obj.districtCode
            myLocation.blockCode=obj.blockCode
            myLocation.displayName="Jharkhand-%s-%s-%s" % (obj.parentLocation.name,obj.name,name)
            myLocation.save()

        headers['X-Update']='SeccCardholderDealerId'
        response = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/getRgiDealer', headers=headers, cookies=cookies, data=data)
        myhtml=response.content
        mysoup=BeautifulSoup(myhtml,"lxml")
        options=mysoup.findAll('option')
        for option in options:
          if option['value'] != "":
            code=option['value']
            name=option.text
            logger.info(f"{name}-{code}")
            myLocation=PDSLocation.objects.filter(code=code).first()
            if myLocation is None:
              myLocation=PDSLocation.objects.create(code=code)
            myLocation.locationType='dealer'
            myLocation.parentLocation=obj
            myLocation.stateCode=obj.stateCode
            myLocation.name=name
            myLocation.englishName=name
            myLocation.districtCode=obj.districtCode
            myLocation.blockCode=obj.blockCode
            myLocation.displayName="Jharkhand-%s-%s-%s" % (obj.parentLocation.name,obj.name,name)
            myLocation.save()

            

      
  if locationType == 'block':
    myDistricts=PDSLocation.objects.filter(locationType='district')
    for obj in myDistricts:
      url="https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults"
      r=requests.get(url)
      if r.status_code == 200:
        cookies=r.cookies
        headers = {
         'Accept': 'text/javascript, text/html, application/xml, text/xml, */*',
         'Accept-Encoding': 'gzip, deflate, br',
         'Accept-Language': 'en-GB,en;q=0.5',
         'Connection': 'keep-alive',
         'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
         'Host': 'aahar.jharkhand.gov.in',
         'Referer': 'https://aahar.jharkhand.gov.in/secc_cardholders/searchRation',
         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
         'X-Prototype-Version': '1.6.1_rc2',
         'X-Requested-With': 'XMLHttpRequest',
         'X-Update': 'SeccCardholderRgiBlockCode',
       }

        data = {
         'data[SeccCardholder][rgi_district_code]': obj.districtCode
        }

        response = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/getRgiBlock', headers=headers, cookies=cookies, data=data)
        myhtml=response.content
        mysoup=BeautifulSoup(myhtml,"lxml")
        blockSelect=mysoup.find("select",id="SeccCardholderRgiBlockCode")
        options=mysoup.findAll('option')
        for option in options:
          if option['value'] != "":
            code=option['value']
            name=option.text
            logger.info(f"{name}-{code}")
            myLocation=PDSLocation.objects.filter(code=code).first()
            if myLocation is None:
              myLocation=PDSLocation.objects.create(code=code)
            myLocation.locationType='block'
            myLocation.parentLocation=obj
            myLocation.stateCode=obj.stateCode
            myLocation.name=name
            myLocation.englishName=name
            myLocation.districtCode=obj.districtCode
            myLocation.blockCode=code
            myLocation.displayName="Jharkhand-%s-%s" % (obj.name,name)
            myLocation.save()
             
 
def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This implements the crawl State Machine')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-e', '--execute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-n', '--numberOfThreads', help='Number of Threds to be run', required=False)
  parser.add_argument('-m', '--populateLimit', help='Number of Taks to be populated in one go', required=False)
  parser.add_argument('-p', '--populate', help='Populate CrawlQueue', required=False,action='store_const', const=1)
  parser.add_argument('-se', '--singleExecute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-sm', '--stateMachine', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=2)
  parser.add_argument('-d', '--debug', help='Debug Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--test', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-id', '--inputID', help='Input ID that needs to eb processed', required=False)
  parser.add_argument('-ds', '--downloadStage', help='Input ID that needs to eb processed', required=False)
  parser.add_argument('-t1', '--test1', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-f', '--force', help='Force download', required=False,action='store_const', const=1)
  parser.add_argument('-ti', '--testInput', help='Test Input', required=False)
  parser.add_argument('-limit', '--limit', help='Limit', required=False)

  args = vars(parser.parse_args())
  return args

def getRationList(logger,lobj):
  pdsBaseURL="https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults"
  r=requests.get(pdsBaseURL)
  cookies=r.cookies
  districtCode=lobj.districtCode
  blockCode=lobj.blockCode
  myhtml=None
  error=None
  outhtml=''
  title="PDSList"
  timeout=10
  cardTypeArray=['5','6','7']
  headers = {
        'Origin': 'https://aahar.jharkhand.gov.in',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.97 Safari/537.36 Vivaldi/1.94.1008.44',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://aahar.jharkhand.gov.in/secc_cardholders/searchRation',
        'Connection': 'keep-alive',
    }

  for cardType in cardTypeArray:
    data = [
            ('_method', 'POST'),
            ('data[SeccCardholder][rgi_district_code]', districtCode),
            ('data[SeccCardholder][rgi_block_code]', blockCode),
            ('r1', 'dealer'),
            ('data[SeccCardholder][rgi_village_code]', lobj.code),
            ('data[SeccCardholder][dealer_id]', ''),
            ('data[SeccCardholder][cardtype_id]', cardType),
            ('data[SeccCardholder][rationcard_no]', ''),
        ]
    response = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults', headers=headers, cookies=cookies, data=data, timeout=timeout, verify=False)
    if response.status_code == 200:
      myhtml=response.content
      logger.info("able to download list")
      soup=BeautifulSoup(myhtml,"lxml")
      myTable=soup.find('table',id='maintable')
      outhtml+=getCenterAlignedHeading(cardType)
      outhtml+=stripTableAttributes(myTable,"maintable")
    else:
      error="unable to download"
  outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
  processPDSData(logger,lobj,outhtml)
  return error 

def processPDSData(logger,lobj,myhtml):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',',
                lineterminator='\r\n',
                quotechar = '"'
                )
  l=['stateName','stateCode','districtName','districtCode','blockName','blockCode','villageName','villageCode']
  a=['rationCardNumber','name','nameHindi','fatherHusbandName','cardType','familyCount','uidCount','dealer','dealerCode','data','mappedStatus']
  w.writerow(l+a)
  locationData=[lobj.parentLocation.parentLocation.parentLocation.name,lobj.stateCode,lobj.parentLocation.parentLocation.name,lobj.districtCode,lobj.parentLocation.name,lobj.blockCode,lobj.name,lobj.code]
  soup=BeautifulSoup(myhtml,"lxml")
  myTables=soup.findAll('table',id='maintable')

  for myTable in myTables:
    rows=myTable.findAll('tr')
    for row in rows:
      cols=row.findAll('td')
      if len(cols) == 11:
        srNo=cols[0].text.lstrip().rstrip()
        name=cols[2].text.lstrip().rstrip()
        #logger.info(name)
        rationCardNumber=cols[1].text.lstrip().rstrip()
        nameHindi=cols[3].text.lstrip().rstrip()
        fatherHusbandName=cols[4].text.lstrip().rstrip()
        cardType=cols[5].text.lstrip().rstrip()
        familyCount=cols[6].text.lstrip().rstrip()
        uidCount=cols[7].text.lstrip().rstrip()
        dealer=cols[8].text.lstrip().rstrip()
        data=cols[9].text.lstrip().rstrip()
        mappedStatus=cols[10].text.lstrip().rstrip()
        myDealer=PDSLocation.objects.filter(name=dealer,locationType='dealer',parentLocation=lobj.parentLocation).first()
        if myDealer is None:
          dealerCode=''
        else:
          dealerCode=myDealer.code
        a=[rationCardNumber,name,nameHindi,fatherHusbandName,cardType,familyCount,uidCount,dealer,dealerCode,data,mappedStatus]
        w.writerow(locationData+a)
  f.seek(0)
  filename="%s_pdsList.csv" % (lobj.slug)
  filepath="PDSData/%s/%s/%s/%s" % (lobj.parentLocation.parentLocation.parentLocation.slug,lobj.parentLocation.parentLocation.slug,lobj.parentLocation.slug,filename)
  outcsv=f.getvalue()
  contentType="text/csv"
  s3url=uploadReportAmazon(filepath,outcsv,contentType)
  logger.info(s3url)
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    objs=PDSLocation.objects.filter(locationType='village').order_by('-id')
    for obj in objs:
      logger.info(obj.id)
      pdsStat=PDSStat.objects.create(pdsLocation=obj)
      pdsStat.downloadDate='2000-01-01'
      pdsStat.save()
  if args['singleExecute']:
    limit=int(args['limit'])
    if limit is None:
      limit=1
    objs=PDSStat.objects.filter(downloadDate__lt = jharkhandPDSListDownloadThreshold).order_by('downloadDate','pdsLocation__districtCode','pdsLocation__blockCode')[:limit]
    for obj in objs:
      lobj=obj.pdsLocation
      logger.info(f"{lobj.id}-{lobj.displayName}")
      try:
        error=getRationList(logger,lobj)
      except:
        error="Cound not download"
      if error is None:
        obj.downloadDate=timezone.now()
        obj.save()
    
  logger.info("...END PROCESSING") 

if __name__ == '__main__':
  main()
