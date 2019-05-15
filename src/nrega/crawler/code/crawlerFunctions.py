import codecs
from contextlib import closing
import requests
import xlrd
import collections
import traceback
import logging
from io import BytesIO
import unicodecsv as csv
import csv as oldcsv
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
from boto3.session import Session
from botocore.client import Config
from bs4 import BeautifulSoup
from config.defines import djangoSettings,logDir,LIBTECH_AWS_SECRET_ACCESS_KEY,LIBTECH_AWS_ACCESS_KEY_ID,LIBTECH_AWS_BUCKET_NAME,AWS_S3_REGION_NAME,BASEURL,AUTHENDPOINT,apiusername,apipassword
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.utils.text import slugify
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()

from nrega import models as nregamodels
from nrega.crawler.commons.nregaFunctions import getCurrentFinYear,stripTableAttributes,getCenterAlignedHeading,htmlWrapperLocal,getFullFinYear,correctDateFormat,table2csv,array2HTMLTable,getDateObj,stripTableAttributesPreserveLinks,getFinYear
from nrega.crawler.commons.nregaSettings import statsURL,telanganaStateCode,crawlerTimeThreshold,delayPaymentThreshold,crawlerErrorTimeThreshold
from nrega.crawler.code.commons import saveReport,savePanchayatReport,uploadReportAmazon,getjcNumber,isReportUpdated,getReportHTML,validateAndSave,validateNICReport,csv_from_excel
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Jobcard,PanchayatCrawlInfo,Worker,PanchayatStat,Village,Wagelist,FTO,WagelistTransaction,JobcardStat,DPTransaction,FTOTransaction,APWorkPayment,Report,WorkerStat,DemandWorkDetail,MISReportURL,RejectedPayment,WorkDetail,CrawlRequest,CrawlState,PaymentTransaction,WorkPayment
from nrega.crawler.code.delayURLs import delayURLs

musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
downloadToleranceDict={
        "Jobcard" : 95,
        "Muster" : 40,
        "Wagelist" : 95,
        "FTO" : 95,
        "RejectedPayment" : 80,
        }
def makehash():
  return collections.defaultdict(makehash)

class CrawlerObject:
  def __init__(self,cqID):
    cq=CrawlRequest.objects.filter(id=cqID).first()
    self.id=cqID
    self.startFinYear=cq.startFinYear
    if cq.endFinYear is not None:
      self.endFinYear=cq.endFinYear
    else:
      self.endFinYear=getCurrentFinYear()
    self.downloadStage=cq.crawlState.name
    self.sequence=cq.crawlState.sequence
    self.isBlockLevel=cq.crawlState.isBlockLevel
    self.needFullBlockData=cq.crawlState.needFullBlockData
    self.iterateFinYear=cq.crawlState.iterateFinYear
    self.attemptCount=cq.attemptCount
    self.remarks=''
    if cq.panchayat is not None:
      self.locationCode=cq.panchayat.code
    else:
      self.locationCode=cq.block.code
     
class LocationObject:
  def __init__(self, cobj, code=None):
    self.cobj=cobj
    if code is None:
      code=cobj.locationCode 
    if len(code) == 10:
      self.locationType='panchayat'
    elif len(code) == 7:
      self.locationType='block'
    elif len(code) == 4:
      self.locationType='district'
    else:
      self.locationType='unknown'
    
    if self.locationType=="panchayat":
      panchayatObj=Panchayat.objects.filter(code=code).first()
      blockObj=panchayatObj.block
      districtObj=blockObj.district
      stateObj=panchayatObj.block.district.state
    if self.locationType=="block":
      panchayatObj=None
      blockObj=Block.objects.filter(code=code).first()
      districtObj=blockObj.district
      stateObj=districtObj.state
    else:
      panchayatObj=None
      blockObj=None
      districtObj=District.objects.filter(code=code).first()
      stateObj=districtObj.state

    self.searchIP="mnregaweb4.nic.in"
    self.stateCode=stateObj.code
    self.stateName=stateObj.name
    self.stateSlug=stateObj.slug
    self.isNIC=stateObj.isNIC
    self.crawlIP=stateObj.crawlIP
    self.stateShortCode=stateObj.stateShortCode
    self.jobcardPrefix=self.stateShortCode+"-"
    self.state=stateObj

    self.districtCode=districtObj.code
    self.districtName=districtObj.name
    self.districtSlug=districtObj.slug
    self.district=districtObj
    self.districtID=districtObj.id
    self.locationName="%s-%s" % (self.stateName,self.districtName)
    self.districtFilepath="%s/%s/%s/%s/%s/%s" % ("nrega",self.stateSlug,self.districtSlug,"DATA","NICREPORTS","filename")
    self.displayName=f'{self.districtID}-{self.districtCode}-{self.districtName}'
    
    if ((self.locationType=='panchayat') or (self.locationType=='block')):
      self.blockFilepath="%s/%s/%s/%s/%s/%s/%s" % ("nrega",self.stateSlug,self.districtSlug,self.blockSlug,"DATA","NICREPORTS","filename")
      self.dataFilepath="%s/%s/%s/%s/%s/%s" % ("DATA",self.stateSlug,self.districtSlug,self.blockSlug,"foldername","filename")
      self.error=False
      self.blockCode=blockObj.code
      self.block=blockObj
      self.blockID=blockObj.id
      self.blockName=blockObj.name
      self.blockSlug=blockObj.slug
      self.displayName=f'{self.blockID}-{self.blockCode}-{self.blockName}'
    if self.locationType=='panchayat':
      self.panchayatCode=panchayatObj.code
      self.panchayatName=panchayatObj.name
      self.panchayatID=panchayatObj.id
      self.panchayatSlug=panchayatObj.slug
      self.panchayat=panchayatObj
      self.displayName=f'{self.panchayatID}-{self.panchayatCode}-{self.panchayatName}'
      self.panchayatPageURL="http://%s/netnrega/IndexFrame.aspx?lflag=eng&District_Code=%s&district_name=%s&state_name=%s&state_Code=%s&block_name=%s&block_code=%s&fin_year=%s&check=1&Panchayat_name=%s&Panchayat_Code=%s" % (self.crawlIP,self.districtCode,self.districtName,self.stateName,self.stateCode,self.blockName,self.blockCode,"fullfinyear",self.panchayatName,self.panchayatCode)
      self.panchayatFilepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",self.stateSlug,self.districtSlug,self.blockSlug,self.panchayatSlug,"DATA","NICREPORTS","filename")
 

def crawlerMain(logger,cqID,downloadStage=None):
    startTime=datetime.datetime.now()
    cobj=CrawlerObject(cqID)
    pobj=LocationObject(cobj)
    if pobj.error == False:
      logger.info("No Error Found")
      try:
        if pobj.stateCode == '00':
          error=crawlFullPanchayatTelangana(logger,pobj)
        else:
          error=crawlNICPanchayat(logger,pobj,downloadStage=downloadStage)
      except:
        error = traceback.format_exc()
      logger.info("Finished CrawlQueue ID %s with error %s " % (str(cqID),error))
      
      cq=CrawlRequest.objects.filter(id=cqID).first()
      endTime=datetime.datetime.now()
      if error is None:
        attemptCount=0
        crawlDuration=(endTime-startTime).seconds
      else:
        attemptCount=cobj.attemptCount+1
        crawlDuration=0
      cq.error=error
      cq.inProgress=False
      cq.attemptCount=attemptCount
    #  cq.crawlDuration=crawlDuration
      cq.crawlAttempDate=timezone.now()
      cq.remarks=cobj.remarks
      cq.save()


    else:
      logger.info('Multple panchayats found')


def crawlNICPanchayat(logger,lobj,downloadStage=None):
  error=None
  accuracy=None
  cobj=lobj.cobj
  nextSequence=cobj.sequence+1
  nextCrawlState=CrawlState.objects.filter(sequence=nextSequence).first()
    
  if downloadStage is None:
    downloadStage=lobj.cobj.downloadStage
  if lobj.locationType == "block":
    logger.info("Running Crawl for Block %s-%s-%s for stage %s" % (str(lobj.cobj.id),lobj.blockCode,lobj.blockName,downloadStage))
  else:
    logger.info("Running Crawl for Panchayat %s-%s-%s for stage %s" % (str(lobj.cobj.id),lobj.panchayatCode,lobj.panchayatName,downloadStage))
  
  finyearArray=[]
  if cobj.iterateFinYear == False:
    finyearArray=[None]
  else:
    for finyear in range(int(cobj.startFinYear),int(cobj.endFinYear)+1):
      finyearArray.append(str(finyear))
   
  locationCodeArray=[]
  if cobj.isBlockLevel == True:
    locationCodeArray.append(lobj.blockCode)
  elif ((cobj.needFullBlockData == False) and (lobj.locationType == "panchayat")):
    locationCodeArray.append(lobj.panchayatCode)
  else:
    myPanchayats=Panchayat.objects.filter(block__code=lobj.blockCode).order_by("-code")
    for eachPanchayat in myPanchayats:
      locationCodeArray.append(eachPanchayat.code)
  
  logger.info(finyearArray)
  logger.info(f'Location Code Array { locationCodeArray }')

  for code in locationCodeArray:
    pobj=LocationObject(cobj,code=code)
    createCodeObjDict(logger,pobj)
    for finyear in finyearArray:
      logger.info(f'Crawling for { pobj.displayName } for finyear { finyear } downloadStage { downloadStage }')


      if (downloadStage == "init"):
        try:
          PanchayatStat.objects.create(finyear=finyear,panchayat=pobj.panchayat)
        except:
          s=f'Panchayat Stat already exists'
        error,totalEmploymentProvided=downloadPanchayatStat(logger,pobj,finyear)
        if error is not None:
          return error 
        if int(totalEmploymentProvided) > 0:
          error,isZeroMusters=crawlMusters(logger,pobj,finyear)
          if error is not None:
            return error
          error=crawlWagelists(logger,pobj,finyear)
          if error is not None:
            return error


      elif (downloadStage == "jobcardRegister"):
        error=jobcardRegister(logger,pobj) 
        if error is not None:
          return error

      elif (downloadStage == "telanganaJobcardRegister"):
        error=jobcardRegisterTelangana(logger,pobj) 
        if error is not None:
          return error


      elif (downloadStage == "crawlFTO"):
        error=crawlFTORejectedPayment(logger,pobj,finyear) 
        if error is not None:
          return error

      elif (downloadStage =="downloadWagelists"):
        modelName="Wagelist"
        error,downloadAccuracy=objectDownloadMain(logger,pobj,modelName,finyear)
        if error is not None:
          return error
        objectProcessMain(logger,pobj,modelName,finyear)

      elif (downloadStage =="downloadProcessFTO"):
        modelName="FTO"
        error,downloadAccuracy=objectDownloadMain(logger,pobj,modelName,finyear)
        if error is not None:
          return error
        objectProcessMain(logger,pobj,modelName,finyear)

      elif ( (downloadStage =="downloadJobcards") or (downloadStage=="telanganaDownloadJobcards")):
        modelName="Jobcard"
        error,downloadAccuracy=objectDownloadMain(logger,pobj,modelName,finyear)
        if error is not None:
          return error
        objectProcessMain(logger,pobj,modelName,finyear)
      
      elif (downloadStage == "telanganaDownloadMusters"):
        error=telanganaMusterDownload(logger,pobj,finyear)
        if error is not None:
          return error
       #error=telanganaMusterProcess(logger,pobj,finyear)
       #if error is not None:
       #  return error
       
      elif (downloadStage =="downloadMusters"):
        modelName="Muster"
        error,downloadAccuracy=objectDownloadMain(logger,pobj,modelName,finyear)
        if error is not None:
          return error
        objectProcessMain(logger,pobj,modelName,finyear)

      elif (downloadStage =="downloadRejectedPayment"):
        modelName="RejectedPayment"
        error,downloadAccuracy=objectDownloadMain(logger,pobj,modelName,finyear)
        if error is not None:
          return error
        objectProcessMain(logger,pobj,modelName,finyear)

      elif (downloadStage =="DPDemandJobcardStat"):
        error=downloadMISDPReport(logger,pobj,finyear)
        if error is not None:
          return error
        processMISDPReport(logger,pobj,finyear)
        error=downloadWorkDemand(logger,pobj,finyear)
        if error is not None:
          return error
        processWorkDemand(logger,pobj,finyear)
        error=downloadJobcardStat(logger,pobj,finyear)
        if error is not None:
          return error
        processJobcardStat(logger,pobj,finyear)

      elif (downloadStage =="matchTransactions"):
        matchTransactions(logger,pobj,finyear)
        
      elif (downloadStage =="generateReports"):
        curAccuracy=computePanchayatStat(logger,pobj,str(finyear))
        createJobcardStatReport(logger,pobj,finyear)
        createDetailWorkPaymentReport(logger,pobj,finyear)
        if accuracy is None:
          accuracy=curAccuracy
        elif curAccuracy <= accuracy:
          accuracy=curAccuracy

      elif (downloadStage =="telanganaGenerateReports"):
        createDetailWorkPaymentReportAP(logger,pobj,finyear)

      elif (downloadStage =="dumpDataCSV"):
        dumpDataCSV(logger,pobj,finyear=finyear,modelName="Worker")
        dumpDataCSV(logger,pobj,finyear=finyear,modelName="WagelistTransaction")
        dumpDataCSV(logger,pobj,finyear=finyear,modelName="FTOTransaction")
        dumpDataCSV(logger,pobj,finyear=finyear,modelName="MusterTransaction")

  if accuracy is not None:
    cq=CrawlRequest.objects.filter(id=cobj.id).first()
    cq.accuracy=accuracy
    cq.save() 

  if nextCrawlState is not None:
    cq=CrawlRequest.objects.filter(id=cobj.id).first()
    cq.crawlState=nextCrawlState
    if ( (nextCrawlState.name == "complete") or (nextCrawlState.name == "telanganaComplete")):
      cq.isComplete=True
    cq.save() 
  return error

def getPanchayatActiveStatus(logger,pobj,finyear):
  ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
  isActive=ps.isActive
  return isActive

def processGlance(logger,pobj):
  reportType="nicGlanceStats"
  finyear=None
  if hasattr(pobj, 'panchayat'):
    error,myhtml=getReportHTML(logger,pobj,reportType,finyear)
  else:
    error,myhtml=getReportHTML(logger,pobj,reportType,finyear,locationType="block")
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find("table")
    rows=myTable.findAll("tr")
    finyearArray=[None]
    for row in rows:
      cols=row.findAll("td")
      if len(cols) >= 2:
        rowHeader=cols[0].text.lstrip().rstrip()
        if ("II" in rowHeader) and ("Progress" in rowHeader):
          logger.info(rowHeader)
          for col in cols:
            finyearString=col.text.replace("FY","").lstrip().rstrip()
            logger.info(finyearString)      

def getGlance(logger,pobj):
  reportType="nicGlanceStats"
  reportName="NIC At a Glance Stats"
  reportIdentifier="Financial Progress"
  url="http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
  urlPrefix="http://mnregaweb4.nic.in/netnrega/"
  r=requests.get(url)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
    logger.info(viewState)
    logger.info(validation) 
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Connection': 'keep-alive',
    'Host': 'mnregaweb4.nic.in',
    'Referer': url,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
      '__EVENTTARGET': 'ddl_state',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': viewState,
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTVALIDATION': validation,
      'ddl_state': pobj.stateCode
    }
 
    response = requests.post(url,headers=headers, data=data)
    if response.status_code==200:
      myhtml=response.content
    else:
      myhtml=None
    if myhtml is not None:
      logger.info("my html is not None")
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
      viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
      data = {
        '__EVENTTARGET': 'ddl_dist',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': viewState,
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': validation,
        'ddl_state': pobj.stateCode,
        'ddl_dist' : pobj.districtCode
      }
      response = requests.post(url,headers=headers, data=data)
      if response.status_code==200:
        myhtml=response.content
      else:
        myhtml=None
      if myhtml is not None:
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        data = {
          '__EVENTTARGET': 'ddl_blk',
          '__EVENTARGUMENT': '',
          '__LASTFOCUS': '',
          '__VIEWSTATE': viewState,
          '__VIEWSTATEENCRYPTED': '',
          '__EVENTVALIDATION': validation,
          'ddl_state': pobj.stateCode,
          'ddl_dist' : pobj.districtCode,
          'ddl_blk' : pobj.blockCode
        }
        response = requests.post(url,headers=headers, data=data)
        if response.status_code==200:
          myhtml=response.content
        else:
          myhtml=None
        if myhtml is not None:
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
          viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
          logger.info(viewState)
          myPanchayats=Panchayat.objects.filter(block=pobj.block)
          locationCodeArray=["ALL"]
          for eachPanchayat in myPanchayats: 
            locationCodeArray.append(eachPanchayat.code)
          for locationCode in locationCodeArray:
            data = {
              '__EVENTTARGET': '',
              '__EVENTARGUMENT': '',
              '__LASTFOCUS': '',
              '__VIEWSTATE': viewState,
              '__VIEWSTATEENCRYPTED': '',
              '__EVENTVALIDATION': validation,
              'ddl_state': pobj.stateCode,
              'ddl_dist' : pobj.districtCode,
              'ddl_blk' : pobj.blockCode,
              'ddl_pan' : locationCode,
              'btproceed' : 'View Detail'
            }
            response = requests.post(url,headers=headers, data=data)
            if response.status_code==200:
              myhtml=response.content
            else:
              myhtml=None
            if myhtml is not None:
              htmlsoup=BeautifulSoup(myhtml,"lxml")
              validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
              viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
              myiFrame=htmlsoup.find("iframe")
              if myiFrame is not None:
                statsURL=urlPrefix+myiFrame['src']
                logger.info(statsURL)
                r=requests.get(statsURL)
                if r.status_code == 200:
                  if locationCode == "ALL":
                    error=validateAndSave(logger,pobj,r.content,reportName,reportType,locationType="block",jobcardPrefix=reportIdentifier)
                  else:
                    lobj=LocationObject(pobj.cobj,code=locationCode)
                    error=validateAndSave(logger,lobj,r.content,reportName,reportType,jobcardPrefix=reportIdentifier)
                   
  


def downloadPanchayatStat(logger,pobj,finyear):
  error1="could not download Panchayat Stat for finyear %s "  % (finyear)
  reportType="nicStatHTML"
  totalEmploymentProvided=0
  logger.info(pobj.panchayat)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,locationType="block")
  if isUpdated == False:
    getPanchayatStats(logger,pobj,finyear)
  else:
    logger.info("Report is already updated")
  error,myhtml=getReportHTML(logger,pobj,reportType,finyear,locationType="block")
  if error is None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find("table",id="myTable")
    if myTable is not None:
      rows=myTable.findAll("tr")
      for row in rows:
        cols=row.findAll("td")
        panchayatName=cols[1].text.lstrip().rstrip()
        if panchayatName == pobj.panchayatName:
          logger.info("Panchayat Found")
          totalEmploymentProvided=cols[18].text.lstrip().rstrip()
          totalJobcards=cols[2].text.lstrip().rstrip()
          totalWorkers=cols[3].text.lstrip().rstrip()
          ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
          ps.nicEmploymentProvided=totalEmploymentProvided
          ps.nicJobcardsTotal=totalJobcards
          ps.nicWorkersTotal=totalWorkers
          if int(totalEmploymentProvided) == 0:
            ps.isActive=False
          error1=None
          ps.save()
  return error1,totalEmploymentProvided

def getPanchayatStats(logger,pobj,finyear):
  error="unable to download Panchayt Stat"
  mru=MISReportURL.objects.filter(state__code=pobj.stateCode,finyear=finyear).first()
  urlPrefix="http://mnregaweb4.nic.in/netnrega/citizen_html/"
  reportType="nicStatHTML"
  reportName="Block Statistics"
  logger.info("MRU is %s " % str(mru))
  if mru is not None:
    url=mru.demandRegisterURL
    logger.info(url)
    r=requests.get(url)
    if r.status_code==200:
      s="district_code=%s" % (pobj.districtCode)
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      a=htmlsoup.find("a", href=re.compile(s))
      url="%s%s" % (urlPrefix,a['href'])
      logger.info(url)
      r=requests.get(url)
      if r.status_code==200:
        s="block_code=%s" % (pobj.blockCode)
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        a=htmlsoup.find("a", href=re.compile(s))
        url="%s%s" % (urlPrefix,a['href'])
        logger.info(url)
        r=requests.get(url)
        if r.status_code == 200:
          s="Employment Provided"
          myhtml=r.content
          error=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear=finyear,locationType="block",jobcardPrefix=s)
          logger.info(error)
          
  return error
 
 
def crawlMusters(logger,pobj,finyear):
  isZeroMusters=False
  error=None
  musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
  fullfinyear=getFullFinYear(finyear)
  logger.debug("Processing : panchayat: %s " % (pobj.panchayatName))
  stateCode=pobj.stateCode
  fullDistrictCode=pobj.districtCode
  fullBlockCode=pobj.blockCode
  fullPanchayatCode=pobj.panchayatCode
  districtName=pobj.districtName
  blockName=pobj.blockName
  stateName=pobj.stateName
  crawlIP=pobj.crawlIP
  panchayatName=pobj.panchayatName
  musterType='10'
  url="http://"+crawlIP+"/netnrega/state_html/emuster_wage_rep1.aspx?type="+str(musterType)+"&lflag=eng&state_name="+stateName+"&district_name="+districtName+"&block_name="+blockName+"&panchayat_name="+panchayatName+"&panchayat_code="+fullPanchayatCode+"&fin_year="+fullfinyear
  logger.debug(url)
  r  = requests.get(url)
  if r.status_code==200:
    curtime = time.strftime('%Y-%m-%d %H:%M:%S')
    htmlsource=r.content
    htmlsoup=BeautifulSoup(htmlsource,"lxml")
    try:
      table=htmlsoup.find('table',bordercolor="green")
      rows = table.findAll('tr')
      errorflag=0
    except:
      status=0
      errorflag=1
    if errorflag==0:
      musterCount=0
      for tr in rows:
        cols = tr.findAll('td')
        tdtext=''
        district= cols[1].string.strip()
        block= cols[2].string.strip()
        panchayat= cols[3].string.strip()
        worknameworkcode=cols[5].text
        if district!="District":
          musterCount=musterCount+1
          emusterno="".join(cols[6].text.split())
          datefromdateto="".join(cols[7].text.split())
          datefromstring=datefromdateto[0:datefromdateto.index("-")]
          datetostring=datefromdateto[datefromdateto.index("-") +2:len(datefromdateto)]
          if datefromstring != '':
            datefrom = time.strptime(datefromstring, '%d/%m/%Y')
            datefrom = time.strftime('%Y-%m-%d', datefrom)
          else:
            datefrom=''
          if datetostring != '':
            dateto = time.strptime(datetostring, '%d/%m/%Y')
            dateto = time.strftime('%Y-%m-%d', dateto)
          else:
            dateto=''
          #worknameworkcodearray=re.match(r'(.*)\(330(.*)\)',worknameworkcode)
          worknameworkcodearray=re.match(r'(.*)\('+stateCode+r'(.*)\)',worknameworkcode)
          if worknameworkcodearray is not None:
            workName=worknameworkcodearray.groups()[0]
            workCode=stateCode+worknameworkcodearray.groups()[1]
            logger.info(emusterno+" "+datefromstring+"  "+datetostring+"  "+workCode)
            musterURL="http://%s/citizen_html/musternew.asp?lflag=&id=1&state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&block_code=%s&msrno=%s&finyear=%s&workcode=%s&dtfrm=%s&dtto=%s&wn=%s" % (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.blockCode,emusterno,fullfinyear,workCode,datefromstring,datetostring,workName.replace(" ","+"))
            newMusterURL="http://%s/citizen_html/musternew.asp?id=1&msrno=%s&finyear=%s&workcode=%s&panchayat_code=%s" % (pobj.crawlIP,emusterno,fullfinyear,workCode,pobj.panchayatCode)

            try:
              myMuster=Muster.objects.create(block=pobj.block,finyear=finyear,musterNo=emusterno)
            except:
              myMuster=Muster.objects.filter(block=pobj.block,finyear=finyear,musterNo=emusterno).first()
            myMuster.dateFrom=datefrom
            myMuster.dateTo=dateto
            myMuster.workCode=workCode
            myMuster.workName=workName 
            myMuster.musterType='10'
            myMuster.musterURL=musterURL
            myMuster.newMusterURL=newMusterURL
            myMuster.panchayat=pobj.panchayat
            myMuster.save()
    else:
      error="Could not find Muster Summary table for finyear %s " % str(finyear)
  else:
    error="Could not Download"
  return error,isZeroMusters


def crawlWagelists(logger,pobj,finyear):
  error=None
  errorflag=0
  musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
  wagelistDict={}
  logger.debug("Crawling Wagelist for panchyat %s-%s finyear %s " % (pobj.panchayatCode,pobj.panchayatName,finyear))
  fullFinYear=getFullFinYear(finyear)
  url="http://%s/netnrega/state_html/emuster_wage_rep1.aspx?type=6&lflag=eng&state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&panchayat_code=%s&fin_year=%s" % (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.panchayatCode,fullFinYear)
  logger.debug(url)
  r  = requests.get(url)
  if r.status_code == 200:
    curtime = time.strftime('%Y-%m-%d %H:%M:%S')
    htmlsource=r.content
    #htmlsource1=re.sub(musterregex,"",htmlsource)
    htmlsoup=BeautifulSoup(htmlsource,"lxml")
    try:
      table=htmlsoup.find('table',bordercolor="green")
      rows = table.findAll('tr')
      errorflag=0
    except:
      status=0
      errorflag=1
    if errorflag==0:
      for tr in rows:
        cols = tr.findAll('td')
        if "WageList No." in str(tr):
          logger.debug("Found the header row")
        else:
          wagelistNo=cols[6].text.lstrip().rstrip()
          code="%s_%s_%s" % (pobj.blockCode,finyear,wagelistNo)
          dateString=cols[7].text.lstrip().rstrip()
          dateObject=correctDateFormat(dateString)
          #logger.info(dateObject)
          try:
            myWagelist=Wagelist.objects.create(block=pobj.block,finyear=finyear,wagelistNo=wagelistNo)
          except:
            myWagelist=Wagelist.objects.filter(block=pobj.block,finyear=finyear,wagelistNo=wagelistNo).first()
          url="http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=%s&district_code=%s&state_name=%s&district_name=%s&block_code=%s&wg_no=%s&short_name=%s&fin_year=%s&mode=wg" % (pobj.stateCode,pobj.districtCode,pobj.stateName,pobj.districtName,pobj.blockCode,wagelistNo,pobj.stateShortCode,fullFinYear)
          myWagelist.generateDate=dateObject
          myWagelist.panchayat=pobj.panchayat
          myWagelist.wagelistURL=url
          myWagelist.save()
    else:
      error="Could not find Wagelist Summary table for finyear %s " % finyear
  else:
    error="Could not fetch Wagelist for finyear %s " % str(finyear)
  return error

def fetchDataTelanganaJobcardRegister(logger,pobj):
  stateCode='02'
  urlHome="http://www.nrega.ap.gov.in/Nregs/FrontServlet"
  urlHome="http://www.nrega.telangana.gov.in/Nregs/FrontServlet"
  url="http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=WageSeekersRH&actionVal=JobCardHolder&page=WageSeekersHome&param=JCHI"
  url="http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_Ajax_engRH&actionVal=Display&page=commondetails_eng"
  districtCode=pobj.districtCode[-2:]
  blockCode=pobj.blockCode[-2:]
  #districtCode=fullPanchayatCode[2:4]
  #blockCode=fullPanchayatCode[5:7]
  panchayatCode=pobj.panchayatCode[8:10]
  logger.debug("DistrictCode: %s, blockCode : %s , panchayatCode: %s " % (districtCode,blockCode,panchayatCode))
  headers = {
    'Host': 'www.nrega.telangana.gov.in',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': url,
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

  params = (
    ('requestType', 'Household_engRH'),
    ('actionVal', 'view'),
)

  data = [
  ('State', '02'),
  ('District', districtCode),
  ('Mandal', blockCode),
  ('Panchayat', panchayatCode),
  ('Village', '-1'),
  ('HouseHoldId', ''),
  ('Go', ''),
  ('spl', 'Select'),
  ('input2', ''),
]
  url='http://www.nrega.telangana.gov.in/Nregs/FrontServlet'
  response = requests.post(urlHome, headers=headers, params=params, data=data)
  cookies = response.cookies
  logger.debug(cookies)
      
  logger.debug(response.cookies)
  response=requests.post(urlHome, headers=headers, params=params, cookies=cookies, data=data)
  return response.text


def saveJobcardRegisterTelangana(logger,pobj):
  jobcardPrefix="%s-" % (pobj.stateShortCode) 
  finyear=getCurrentFinYear()
  fullfinyear=getFullFinYear(finyear)
  error=None
  reportType="telanganaJobcardRegister"
  reportName="Telangana Jobcard Register"
  logger.debug("Saving Jobcard Register for Telangana")
  fullPanchayatCode=pobj.panchayatCode
  stateName=pobj.stateName
  districtName=pobj.districtName
  blockName=pobj.blockName
  panchayatName=pobj.panchayatName

  myhtml=fetchDataTelanganaJobcardRegister(logger,pobj)
  myhtml=myhtml.replace("<tbody>","")
  myhtml=myhtml.replace("</tbody>","")
  error=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear=getCurrentFinYear())
  return error

def processJobcardRegisterTelangana(logger,pobj):
  error=None
  finyear=getCurrentFinYear()
  reportType="telanganaJobcardRegister"
  error,myhtml=getReportHTML(logger,pobj,reportType,finyear)
  if error is None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find('table',id="myTable")
    rows=myTable.findAll("tr")
    for row in rows:
      if pobj.jobcardPrefix in str(row):
        cols=row.findAll('td')
        tjobcard=cols[1].text.lstrip().rstrip()
        jobcard=cols[2].text.lstrip().rstrip()
        headOfHousehold=cols[3].text.lstrip().rstrip()
        caste=cols[5].text.lstrip().rstrip()
        issueDateString=cols[4].text.lstrip().rstrip()
        issueDate=correctDateFormat(issueDateString)
        code=jobcard
        logger.info(jobcard)
        try:
          myJobcard=Jobcard.objects.create(jobcard=jobcard,panchayat=pobj.panchayat)
        except:
          myJobcard=Jobcard.objects.filter(jobcard=jobcard,panchayat=pobj.panchayat).first()
        myJobcard.tjobcard=tjobcard
        myJobcard.headOfHousehold=headOfHousehold
        myJobcard.caste=caste
        myJobcard.applicationDate=issueDate
        myJobcard.jcNo=getjcNumber(jobcard)
        myJobcard.save()
  return error


def jobcardRegisterTelangana(logger,pobj):
  error=saveJobcardRegisterTelangana(logger,pobj)
  if error is not None:
    return error
  error=processJobcardRegisterTelangana(logger,pobj)
  if error is not None:
    return error
def jobcardRegister(logger,pobj):
  reportType="applicationRegister"
  reportName="Application Register"
  finyear=getCurrentFinYear()
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=5)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,reportThreshold=reportThreshold)
#  isUpdated=False
  if isUpdated == False:
    fullfinyear=getFullFinYear(finyear)
    myhtml=getJobcardRegister(logger,pobj)
    myhtml=myhtml.replace(b'</nobr><br>',b',')
    myhtml=myhtml.replace(b"bordercolor='#111111'>",b"bordercolor='#111111'><tr>")
    error=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear)
  else:
    error=None
  if error is None:
    error=processJobcardRegister(logger,pobj)
  return error

def getJobcardRegister(logger,pobj):
  logger.debug("Processing : panchayat: %s " % (pobj.panchayatName))
  stateCode=pobj.stateCode
  fullDistrictCode=pobj.districtCode
  fullBlockCode=pobj.blockCode
  fullPanchayatCode=pobj.panchayatCode
  districtName=pobj.districtName
  blockName=pobj.blockName
  stateName=pobj.stateName
  panchayatName=pobj.panchayatName
  crawlIP=pobj.crawlIP
  finyear=getCurrentFinYear()
  fullfinyear=getFullFinYear(finyear) 
  logger.debug("Processing StateCode %s, fullDistrictCode : %s, fullBlockCode : %s, fullPanchayatCode: %s " % (stateCode,fullDistrictCode,fullBlockCode,fullPanchayatCode))
  panchayatPageURL="http://%s/netnrega/IndexFrame.aspx?lflag=eng&District_Code=%s&district_name=%s&state_name=%s&state_Code=%s&block_name=%s&block_code=%s&fin_year=%s&check=1&Panchayat_name=%s&Panchayat_Code=%s" % (crawlIP,fullDistrictCode,districtName,stateName,stateCode,blockName,fullBlockCode,fullfinyear,panchayatName,fullPanchayatCode)
#  panchayatPageURL=panchayatPageURL.replace(" ","+")
  panchayatDetailURL="http://%s/netnrega/Citizen_html/Panchregpeople.aspx" % crawlIP
  logger.debug(panchayatPageURL)
  logger.debug(panchayatDetailURL)
  #Starting the Download Process
  url="http://nrega.nic.in/netnrega/home.aspx"
  logger.info(panchayatPageURL)
  #response = requests.get(url, headers=headers, params=params)
  response = requests.get(panchayatPageURL)
  myhtml=str(response.content)
  splitString="Citizen_html/Panchregpeople.aspx?lflag=eng&fin_year=%s&Panchayat_Code=%s&type=a&Digest=" % (fullfinyear,fullPanchayatCode)
  myhtmlArray=myhtml.split(splitString)
  myArray=myhtmlArray[1].split('"')
  digest=myArray[0]
  cookies = response.cookies
  logger.debug(cookies)
  headers = {
    'Host': crawlIP,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
#    'Referer': panchayatPageURL,
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

  params = (
    ('lflag', 'eng'),
    ('fin_year', fullfinyear),
    ('Panchayat_Code', fullPanchayatCode),
    ('type', 'a'),
    ('Digest', digest),

  )

  response=requests.get(panchayatDetailURL, headers=headers, params=params, cookies=cookies)
  logger.debug("Downloaded StateCode %s, fullDistrictCode : %s, fullBlockCode : %s, fullPanchayatCode: %s " % (stateCode,fullDistrictCode,fullBlockCode,fullPanchayatCode))
  return response.content

def processJobcardRegister(logger,pobj):
    csvArray=[]
    locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
    jobcardArrayLabel=["jobcard","headOfHousehold","issue Date","caste","jcNo"]
    workerArrayLabel=["name","age","gender","fatherHusbandName","isDeleted","isMinority","isDisabled"]
    a=locationArrayLabel+jobcardArrayLabel+workerArrayLabel
    csvArray.append(a)
    reportType="applicationRegister"
    finyear=getCurrentFinYear()
    error,myhtml=getReportHTML(logger,pobj,reportType,finyear)
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find('table',id="myTable")
    jobcardPrefix=pobj.stateShortCode+"-"
    logger.debug(jobcardPrefix)
    if myTable is not None:
      logger.debug("Found the table")
      rows=myTable.findAll('tr')
      headOfHousehold=''
      applicantNo=0
      fatherHusbandName=''
      village=''
      villageDict={}
      for row in rows:
        if "Villages : " in str(row):
          logger.debug("Village Name Found")
          cols=row.findAll('td')
          villagestr=cols[0].text.lstrip().rstrip()
          villageName=villagestr.replace("Villages :" ,"").lstrip().rstrip()
          villageCode="%s_%s" % (pobj.panchayatCode,villageName)
          try:
            myVillage=Village.objects.create(panchayat=pobj.panchayat,name=villageName)
          except:
            myVillage=Village.objects.filter(panchayat=pobj.panchayat,name=villageName).first()
        if jobcardPrefix in str(row):
          isDeleted=False
          isDisabled=False
          isMinority=False
          cols=row.findAll('td')
          rowIndex=cols[0].text.lstrip().rstrip()
          jobcard=cols[9].text.lstrip().rstrip().split(",")[0]
          if len(cols[9].text.lstrip().rstrip().split(",")) > 1:
            issueDateString=cols[9].text.lstrip().rstrip().split(",")[1]
          else:
            issueDateString=''
          gender=cols[6].text.lstrip().rstrip()
          age=cols[7].text.lstrip().rstrip()
          applicationDateString=cols[8].text.lstrip().rstrip()
          remarks=cols[10].text.lstrip().rstrip()
          disabledString=cols[11].text.lstrip().rstrip()
          minorityString=cols[12].text.lstrip().rstrip()
          name=cols[4].text.lstrip().rstrip()
          name=name.rstrip('*')
          name=name.rstrip().strip()
          #logger.debug("Processing %s - %s " % (jobcard,name))
          issueDate=correctDateFormat(issueDateString)
          applicationDate=correctDateFormat(applicationDateString)
          if cols[1].text.lstrip().rstrip() != '':
            headOfHousehold=cols[1].text.lstrip().rstrip()
            caste=cols[2].text.lstrip().rstrip()
            applicantNo=1
            fatherHusbandName=cols[5].text.lstrip().rstrip()
            #Gather Jobcard Replated Info
            try:
              myJobcard=Jobcard.objects.create(jobcard=jobcard,panchayat=pobj.panchayat)
            except:
              myJobcard=Jobcard.objects.filter(jobcard=jobcard,panchayat=pobj.panchayat).first()
            myJobcard.caste=caste
            myJobcard.headOfHousehold=headOfHousehold
            myJobcard.village=myVillage
            myJobcard.issueDate=issueDate
            myJobcard.applicationDate=applicationDate
            myJobcard.jcNo=getjcNumber(jobcard)
            myJobcard.save()
            jobcardArray=[jobcard,headOfHousehold,str(issueDate),caste,str(getjcNumber(jobcard))]
            code=jobcard
          else:
            applicantNo=applicantNo+1

          if '*' in name:
            isDeleted=True
          if disabledString == 'Y':
            isDisabled=True
          if minorityString == 'Y':
            isMinority=True
          try:
            myWorker=Worker.objects.create(jobcard=myJobcard,name=name,applicantNo=applicantNo)
          except:
            myWorker=Worker.objects.filter(jobcard=myJobcard,name=name,applicantNo=applicantNo).first()

          myWorker.gender=gender
          myWorker.age=age
          myWorker.fatherHusbandName=fatherHusbandName
          myWorker.isDisabled=isDisabled
          myWorker.isDeleted=isDeleted
          myWorker.isMinority=isMinority
          myWorker.remarks=remarks
          myWorker.save()
          locationArray=[pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,villageName,pobj.stateCode,pobj.districtCode,pobj.blockCode,pobj.panchayatCode]
          workerArray=[name,str(age),gender,fatherHusbandName,str(isDeleted),str(isMinority),str(isDisabled)]
          a=locationArray+jobcardArray+workerArray
          csvArray.append(a)
    #dumpDataCSV(pobj,csvArray,"workers","workers_%s.csv" % (pobj.panchayatSlug))
    totalJobcards=len(Jobcard.objects.filter(panchayat=pobj.panchayat))
    totalWorkers=len(Worker.objects.filter(jobcard__panchayat=pobj.panchayat))
    if totalJobcards== 0:
      error="No Jobcards Found"
    objs=PanchayatStat.objects.filter(panchayat=pobj.panchayat)
    for obj in objs:
      obj.jobcardsTotal=totalJobcards
      obj.workersTotal=totalWorkers
      obj.save()
    return error

def crawlFTORejectedPayment(logger,pobj,finyear):
  urls,urlsRejected=getFTOListURLs(logger,pobj,finyear)
  logger.info(urls)
  logger.info("Printing Rejected URLs")
  logger.info(urlsRejected)
  reportType="rejectedPaymentHTML"
  reportName="Rejected Payment HTML"
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=3)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,locationType='block',reportThreshold=reportThreshold)
  if isUpdated == False:
    jobcardPrefix="%s%s" % (pobj.stateShortCode,pobj.stateCode)
    locationName="%s-%s-%s" % (pobj.stateName,pobj.districtName,pobj.blockName)
    filename="%s_%s_%s_%s.html" % (reportType,pobj.blockSlug,pobj.blockCode,finyear)
    filepath=pobj.blockFilepath.replace("filename",filename)
    outhtml=''
    outhtml+=getCenterAlignedHeading(locationName)
    outhtml+=getCenterAlignedHeading("Financial Year: %s " % (getFullFinYear(finyear)))
    baseURL="http://%s/netnrega/FTO/" % (pobj.crawlIP)
    for url in urlsRejected:
      r=requests.get(url)
      if r.status_code == 200:
        myhtml=r.content
        error,myTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=jobcardPrefix)
        if myTable is not None:
          logger.info("Found the table")
          outhtml+=stripTableAttributesPreserveLinks(myTable,"myTable",baseURL)
    outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)
    savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath,locationType='block')
    processBlockRejectedPayment(logger,pobj,finyear)

  reportName="FTO List"
  reportType="ftoList"
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=3)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,locationType='block',reportThreshold=reportThreshold)
  if isUpdated == False:
    if len(urls) == 0:
      error="No FTO URL Found for finyear %s " % (finyear)
      return error
    outhtml=""
    outhtml+="<html><body><table>"
    outhtml+="<tr><th>%s</th><th>%s</th></tr>" %("ftoNo","paymentMode")
    for url in urls:
      logger.info(url)
      r=requests.get(url)
      if r.status_code == 200:
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        try:
            table=htmlsoup.find('table',bordercolor="black")
            rows = table.findAll('tr')
            errorflag=0
        except:
          status=0
          errorflag="Unable to find table in url %s " % (url)
        if errorflag==0:
          for tr in rows:
            cols = tr.findAll('td')
            if "FTO No" in str(tr):
              logger.debug("Found the header row")
            else:
              ftoNo=cols[1].text.lstrip().rstrip()
              ftoFound=1
              if pobj.stateShortCode in ftoNo:
                #logger.info(cols[1])
                ftoRelativeURL=cols[1].find("a")['href']
                urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
                ftoURL="%s%s" % (urlPrefix,ftoRelativeURL)
                #logger.info(ftoURL)
                paymentMode=cols[2].text.lstrip().rstrip()
                secondSignatoryDateString=cols[3].text.lstrip().rstrip()
                secondSignatoryDate=getDateObj(secondSignatoryDateString)
                ftoArray=ftoNo.split("_")
                firstSignatoryDateString=ftoArray[1][:6]
                firstSignatoryDate=getDateObj(firstSignatoryDateString,dateFormat='%d%m%y')
                try:
                  myFTO=FTO.objects.create(block=pobj.block,finyear=finyear,ftoNo=ftoNo)
                except:
                  myFTO=FTO.objects.filter(block=pobj.block,finyear=finyear,ftoNo=ftoNo).first()
                outhtml+="<tr><td>%s</td><td>%s</td></tr>" %(ftoNo,paymentMode)
                myFTO.secondSignatoryDate=secondSignatoryDate
                myFTO.firstSignatoryDate=firstSignatoryDate
                myFTO.ftoNo=ftoNo
                myFTO.paymentMode=paymentMode
                myFTO.ftoURL=ftoURL
                myFTO.save() 
    outhtml+="</table></body></html>"
    error=validateAndSave(logger,pobj,outhtml,reportName,reportType,finyear=finyear,locationType="block",jobcardPrefix="Financial Institution",validate=False)

def getBlockStat(logger,pobj,finyear):
  reportType="ftoStats"
  reportName="FTO Statistics"
  tableIdentifier="First Signatory"
  urls=[]
  mru=MISReportURL.objects.filter(state__code=pobj.stateCode,finyear=finyear).first()
  urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
  fullFinYear=getFullFinYear(finyear)
  if mru is not None:
    bankTable=None
    postTable=None
    coBankTable=None
      
    url=mru.ftoURL
    logger.info(url)
    r=requests.get(url)
    if r.status_code==200:
      s="district_code=%s" % (pobj.districtCode)
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      a=htmlsoup.find("a", href=re.compile(s))
      bankurl="%s%s" % (urlPrefix,a['href'])
      logger.info(bankurl)
      r=requests.get(bankurl)
      if r.status_code == 200:
        cookies=r.cookies
        myhtml=r.content
        error,bankTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=tableIdentifier)
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        try:
          validation = htmlsoup.find(id='__EVENTVALIDATION').get('value',None)
        except:
          validation=''
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'C',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(bankurl,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          error,coBankTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=tableIdentifier)

        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'P',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(bankurl,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          error,postTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=tableIdentifier)

    baseURL="http://%s/netnrega/FTO/" % (pobj.crawlIP)
    outhtml=''
    outhtml+=getCenterAlignedHeading(pobj.locationName)
    outhtml+=getCenterAlignedHeading("Financial Year: %s " % (getFullFinYear(finyear)))
    if bankTable is not None:
      outhtml+=getCenterAlignedHeading("Bank Table")
      outhtml+=stripTableAttributesPreserveLinks(bankTable,"bankTable",baseURL)
    if coBankTable is not None:
      outhtml+=getCenterAlignedHeading("Cooperative Bank Table")
      outhtml+=stripTableAttributesPreserveLinks(coBankTable,"coBankTable",baseURL)
    if postTable is not None:
      outhtml+=getCenterAlignedHeading("Post Office Table")
      outhtml+=stripTableAttributesPreserveLinks(postTable,"postTable",baseURL)
    outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)

    filename="%s_%s_%s_%s.html" % (reportType,pobj.districtSlug,pobj.districtCode,finyear)
    filepath=pobj.districtFilepath.replace("filename",filename)
    saveReport(logger,pobj,finyear,reportType,outhtml,filepath)

def processBlockStat(logger,pobj,finyear):
  reportType="ftoStats"
  error,myhtml=getReportHTML(logger,pobj,reportType,finyear,locationType='district')
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  bankTable=htmlsoup.find("table",id="bankTable")
  p=makehash()
  if bankTable is not None:
    p=getFTOStatDataDict(logger,bankTable,p,paymentAgency="bank")  
def getFTOStatDataDict(logger,myTable,p,paymentAgency=None):
  rows=myTable.findAll('tr')
  for row in rows:
    cols=row.findAll('td')
    if len(cols) == 20:
      blockLinkA=cols[1].find("a")
      if blockLinkA is not None:
        blockURL=blockLinkA['href']
        blockURLArray=blockURL.split("block_code=")
        if len(blockURLArray) == 2:
          rejectedURL=None
          invalidURL=None
          URLA=cols[18].find("a")
          if URLA is not None:
            invalidURL=URLA["href"]
          rejectedURLA=cols[18].find("a")
          if rejectedURLA is not None:
            rejectedURL=rejectedURLA["href"]
          blockCode=blockURLArray[1][:7]
          p[blockCode][paymentAgency]['blockURL'] = blockURL
          p[blockCode][paymentAgency]['totalTransactions']=cols[19].text.lstrip().rstrip()
          p[blockCode][paymentAgency]['totalRejected']=cols[18].text.lstrip().rstrip()
          p[blockCode][paymentAgency]['totalInvalid']=cols[17].text.lstrip().rstrip()
          p[blockCode][paymentAgency]['totalProcessed']=cols[16].text.lstrip().rstrip()
          p[blockCode][paymentAgency]['rejectedURL']=cols[16].text.lstrip().rstrip()
          p[blockCode][paymentAgency]['totalProcessed']=cols[16].text.lstrip().rstrip()
def getFTOListURLs(logger,pobj,finyear):
  urls=[]
  urlsRejected=[]
  mru=MISReportURL.objects.filter(state__code=pobj.stateCode,finyear=finyear).first()
  urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
  fullFinYear=getFullFinYear(finyear)
  if mru is not None:
    url=mru.ftoURL
    logger.info(url)
    r=requests.get(url)
    if r.status_code==200:
      s="district_code=%s" % (pobj.districtCode)
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      a=htmlsoup.find("a", href=re.compile(s))
      url="%s%s" % (urlPrefix,a['href'])
      logger.info(url)
      r=requests.get(url)
      if r.status_code==200:
        cookies=r.cookies
        bankURL=None
        postURL=None
        coBankURL=None
        bankURLRejected=None
        postURLRejected=None
        coBankURLRejected=None
        s="block_code=%s" % (pobj.blockCode)
        s="block_code=%s&fin_year=%s&typ=sec_sig" % (pobj.blockCode,fullFinYear)
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        try:
          validation = htmlsoup.find(id='__EVENTVALIDATION').get('value',None)
        except:
          validation=''
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        a=htmlsoup.find("a", href=re.compile(s))
        if a is not None:
          bankURL="%s%s" % (urlPrefix,a['href'])
          urls.append(bankURL)
        logger.info(bankURL)
        #Lets get rejected Payment URL
        sr="&block_code=%s&fin_year=%s&typ=R" % (pobj.blockCode,fullFinYear)
        a=htmlsoup.find("a", href=re.compile(sr))
        if a is not None:
          bankURLRejected="%s%s" % (urlPrefix,a['href'])
          urlsRejected.append(bankURLRejected)
        
        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'C',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(url,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          a=htmlsoup.find("a", href=re.compile(s))
          if a is not None:
            coBankURL="%s%s" % (urlPrefix,a['href'])
            urls.append(coBankURL)
          a=htmlsoup.find("a", href=re.compile(sr))
          if a is not None:
            coBankURLRejected="%s%s" % (urlPrefix,a['href'])
            urlsRejected.append(coBankURLRejected)
        logger.info(coBankURL)

        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'P',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(url,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          a=htmlsoup.find("a", href=re.compile(s))
          if a is not None:
            postURL="%s%s" % (urlPrefix,a['href'])
            urls.append(postURL)
          a=htmlsoup.find("a", href=re.compile(sr))
          if a is not None:
            postURLRejected="%s%s" % (urlPrefix,a['href'])
            urlsRejected.append(postURLRejected)
        logger.info(postURL)

  return urls,urlsRejected

def processBlockRejectedPayment(logger,pobj,finyear):
  reportType='rejectedPaymentHTML'
  error,myhtml=getReportHTML(logger,pobj,reportType,finyear,locationType='block')
  jobcardPrefix="%s%s" % (pobj.stateShortCode,pobj.stateCode)
  baseURL="http://%s/netnrega/FTO/" % (pobj.crawlIP)
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    tables=htmlsoup.findAll('table',id="myTable")
    for myTable in tables:
      rows=myTable.findAll('tr')
      for row in rows:
        if jobcardPrefix in str(row):
          cols=row.findAll('td')
          ftoString=cols[1].text.lstrip().rstrip()
          referenceNo=cols[2].text.lstrip().rstrip()
          logger.info(referenceNo)
          primaryAccountHolder=cols[6].text.lstrip().rstrip()
          wagelistString=cols[7].text.lstrip().rstrip()
          bankCode=cols[8].text.lstrip().rstrip()
          ifscCode=cols[9].text.lstrip().rstrip()
          amount=cols[10].text.lstrip().rstrip()
          rejectionDateString=cols[11].text.lstrip().rstrip()
          rejectionReason=cols[12].text.lstrip().rstrip()
          a=cols[2].find('a')
          url=a['href']
          
          rp=RejectedPayment.objects.filter(block=pobj.block,finyear=finyear,referenceNo=referenceNo).first()
          if rp is None:
            rp=RejectedPayment.objects.create(block=pobj.block,finyear=finyear,referenceNo=referenceNo)
          rp.url=url
          rp.rejectionDate=getDateObj(rejectionDateString)
          rp.ftoString=ftoString
          rp.primaryAccountHolder=primaryAccountHolder
          rp.wagelistString=wagelistString
          rp.bankCode=bankCode
          rp.ifscCode=ifscCode
          rp.amount=amount
          rp.rejectionReason=rejectionReason
          rp.save()

def downloadWagelist(logger,pobj,obj):
  error=None
  s3url=None
  #Work;ing Wagelist URL
  #http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=&district_code=3405&state_name=JHARKHAND&district_name=PALAMU&block_code=3405008&wg_no=3405008WL060766&short_name=JH&fin_year=2018-2019&mode=wg
  wagelistNo=obj.wagelistNo
  finyear=obj.finyear
  fullFinYear=getFullFinYear(finyear)
  url="http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=%s&district_code=%s&state_name=%s&district_name=%s&block_code=%s&wg_no=%s&short_name=%s&fin_year=%s&mode=wg" % (pobj.stateCode,pobj.districtCode,pobj.stateName,pobj.districtName,pobj.blockCode,wagelistNo,pobj.stateShortCode,fullFinYear)
  #logger.info(url)
  r=requests.get(url)
  if r.status_code != 200:
    error="untable to download"
  if r.status_code == 200:
    myhtml=r.content
    error,myTable=validateNICReport(logger,pobj,myhtml)
    if error is None:  
      outhtml=''
      outhtml+=getCenterAlignedHeading("Wagelist Detail Table")
      outhtml+=stripTableAttributes(myTable,"wagelistDetails")
      title="Wagelist: %s state:%s District:%s block:%s  " % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName)
      outhtml=''
      outhtml+=getCenterAlignedHeading("Wagelist Detail Table")
      outhtml+=stripTableAttributes(myTable,"wagelistDetails")
      title="Wagelist: %s state:%s District:%s block:%s  " % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="%s.html" % (wagelistNo)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,"DATA","Wagelist",fullFinYear,filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
  
  obj=Wagelist.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def updateObjectDownload(logger,obj,error,s3url):
  downloadAttemptCount=obj.downloadAttemptCount
  if error is None:
    nextDownloadAttemptCount=0
  elif downloadAttemptCount >= 3:
    nextDownloadAttemptCount =0
  else:
    nextDownloadAttemptCount=downloadAttemptCount+1

  if error is None:
    obj.isDownloaded=True
    obj.downloadDate=timezone.now()
    obj.contentFileURL=s3url
    obj.isError=False
  else:
    if downloadAttemptCount >= 3:
      obj.isError=True
      obj.errorDate=timezone.now()
  obj.downloadAttemptCount=nextDownloadAttemptCount
  obj.save()

def processWagelist(logger,pobj,obj):
  myhtml=None
  csvArray=[]
  labels=['SrNo','stateName','districtName','blockName','panchayatName','stateCode','districtCode','blockCode','panchayatCode','wagelist','jobcard','name','ftoNo','daysWorked','totalWage']
  csvArray.append(labels)
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    finyear=obj.finyear
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    mytable=htmlsoup.find('table',id="wagelistDetails")
    rows = mytable.findAll('tr')
    allWorkerFound=True
    allDWDFound=False
    allWDFound=False
    allFTOFound=True
    isComplete=True
    multiplePanchayats=False
    remarks=''
    for row in rows:
      if pobj.jobcardPrefix  in str(row):
        cols=row.findAll('td')
        jobcard=cols[8].text.lstrip().rstrip()
        wagelistIndex=cols[0].text.lstrip().rstrip()
        name=cols[9].text.lstrip().rstrip()
        ftoNo=cols[12].text.lstrip().rstrip()
        daysWorked=cols[10].text.lstrip().rstrip()
        totalWage=cols[11].text.lstrip().rstrip()
        reGenFlag=cols[14].text.lstrip().rstrip()
        workerCode="%s_%s" % (jobcard,name)
        ftoCode="%s_%s" % (pobj.blockCode,ftoNo)
        regenerated=False
        if reGenFlag=="Y":
          #logger.info("Regenerated is True")
          regenerated=True
        try:
          myWorker=pobj.codeObjDict[workerCode]
        except:
          myWorker = None
          msg="Worker with workerCode %s not found in Code Obj Dict\n" % (workerCode)
          remarks+=msg
          myWorker=Worker.objects.filter(jobcard__jobcard=jobcard,name=name).first()
          if myWorker is None:
            msg="Worker with workerCode %s not found even in the block\n" % (workerCode)
            multiplePanchayats=True
            remarks+=msg
            allWorkerFound=False
        #logger.info("My Worker is %s "% str(myWorker))
        try:
          #logger.info(f"fto code {ftoCode}")
          myFTO=pobj.codeObjDict[ftoCode]
          isWagelistFTOAbsent=False
        except:
          myFTO=None
          isWagelistFTOAbsent=True
          allFTOFound=False
        if (myWorker is None) or (myFTO is None):
          isComplete=False
        try:
          wt=WagelistTransaction.objects.create(wagelist=obj,wagelistIndex=wagelistIndex)
        except:
          wt=WagelistTransaction.objects.filter(wagelist=obj,wagelistIndex=wagelistIndex).first()
        if myWorker is not None:
          wt.fto=myFTO
          wt.worker=myWorker
          wt.daysWorked=daysWorked
          wt.totalWage=totalWage
          wt.isRegenerated=regenerated
          wt.isWagelistFTOAbsent=isWagelistFTOAbsent
          wt.save()
    obj.allWorkerFound=allWorkerFound
    obj.allFTOFound=allFTOFound
    obj.isComplete=isComplete
    obj.multiplePanchayats=multiplePanchayats
    obj.isDownloaded=True
    obj.downloadDate=timezone.now()
    obj.downloadAttemptCount=0
    obj.remarks=remarks
    obj.save()
    #dumpDataCSV(pobj,csvArray,"wagelistTransactions","%s.csv" % (obj.wagelistNo))

def getDownloadObjects(logger,pobj,modelName,finyear):
  if modelName=="Jobcard":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (panchayat=pobj.panchayat) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  elif ((modelName == "FTO") or (modelName == "RejectedPayment")):
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,isComplete=False,block=pobj.block) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold,isComplete=False)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  else:
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,panchayat=pobj.panchayat) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold,isComplete=False)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  return myobjs

def getObjDownloadStat(logger,pobj,modelName,finyear):
  if modelName == "Jobcard":
      total=len(getattr(nregamodels,modelName).objects.filter(panchayat=pobj.panchayat))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,panchayat=pobj.panchayat))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,panchayat=pobj.panchayat))
  elif ((modelName == "FTO") or (modelName == "RejectedPayment")):
      total=len(getattr(nregamodels,modelName).objects.filter(block=pobj.block,finyear=finyear))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,block=pobj.block,finyear=finyear))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,block=pobj.block,finyear=finyear))
  else:
      total=len(getattr(nregamodels,modelName).objects.filter(panchayat=pobj.panchayat,finyear=finyear))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,panchayat=pobj.panchayat,finyear=finyear))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,panchayat=pobj.panchayat,finyear=finyear))
  return total,pending,downloaded


def objectDownloadMain(logger,pobj,modelName,finyear):
    error=None
    total=0
    pending=0
    downloaded=0
    finyear=str(finyear)
    myobjs=getDownloadObjects(logger,pobj,modelName,finyear)
    n=len(myobjs)
    logger.info("Number of %s to be downloaded is %s " % (modelName,str(n)))
    while n > 0:
      downloadNProcess(logger,pobj,myobjs,modelName=modelName,method='download')
      myobjs=getDownloadObjects(logger,pobj,modelName,finyear)
      n=len(myobjs)
      logger.info("Number of %s to be downloaded is %s " % (modelName,str(n)))

    t,p,d=getObjDownloadStat(logger,pobj,modelName,finyear)
    total +=t
    pending +=p
    downloaded += d
    downloadAccuracy=100
    if total != 0:
      downloadAccuracy=int(downloaded*100/total)
      if downloadAccuracy < downloadToleranceDict[modelName]:
        error="Unable to download %s %s out of %s for finyear %s " % (modelName,str(total-downloaded),str(total),str(finyear))
    if (modelName == "Wagelist"):
      ps=PanchayatStat.objects.filter(finyear=finyear,panchayat=pobj.panchayat).first()
      ps.wagelistDownloadAccuracy=downloadAccuracy
      ps.wagelistTotal=total
      ps.wagelistPending=pending
      ps.wagelistDownloaded=downloaded
      ps.save()
    if (modelName == "Muster"):
      ps=PanchayatStat.objects.filter(finyear=finyear,panchayat=pobj.panchayat).first()
      ps.musterDownloadAccuracy=downloadAccuracy
      ps.mustersTotal=total
      ps.mustersPending=pending
      ps.mustersDownloaded=downloaded
      ps.save()
    return error,downloadAccuracy

def downloadNProcess(logger,pobj,myobjs,modelName=None,method='download'):
  error=None
  q = Queue(maxsize=0)
  num_threads = 100
  i=0
  maxObjects=5000
  for obj in myobjs:
    i=i+1
    if i <= maxObjects:
      q.put(obj)

  for i in range(num_threads):
    name="libtechWorker%s" % str(i)
    worker = Thread(name=name,target=libtechQueueWorker, args=(logger,q,pobj,modelName,method))
    worker.setDaemon(True)
    worker.start()

  q.join()
  for i in range(num_threads):
    q.put(None)
  return error

def getStatePrefix(logger,pobj):
  s=''
  if pobj.isNIC == False:
    s=pobj.stateSlug
  return s

def libtechQueueWorker(logger,q,pobj,modelName,method):
  name = threading.currentThread().getName()
  statePrefix=getStatePrefix(logger,pobj)
  while True:
    obj=q.get()
    if obj is None:
      break
    if modelName == "Jobcard":
      finyear=None
    else:
      finyear=obj.finyear
    key="%s%s%s" % (method,modelName,statePrefix)
    try:
      locationCode=pobj.panchayatCode
    except:
      locationCode=pobj.blockCode
    logger.info("Queue Size %s Thread %s  ModelName %s locationCode %s finyear %s Object ID %s  code %s " % (str(q.qsize()),name,key,locationCode,finyear,obj.id,obj.code))
    try:
      libtechMethodNames[key](logger,pobj,obj)
    except Exception as e:
      logging.info(e, exc_info=True)
    q.task_done()

def createCodeObjDict(logger,pobj,modelArray=None,codeType=None):
  if modelArray is None:
    modelArray=["Worker","Jobcard","Muster","Wagelist","FTO"]
  pobj.codeObjDict={} #makehash()
  for modelName in modelArray:
    if modelName == "FTO":
      myobjs=getattr(nregamodels,modelName).objects.filter(block=pobj.block)
    elif modelName == "Worker":
      myobjs=getattr(nregamodels,modelName).objects.filter(jobcard__panchayat__block=pobj.block)
    else:
      myobjs=getattr(nregamodels,modelName).objects.filter(panchayat__block=pobj.block)
    for obj in myobjs:
      pobj.codeObjDict[obj.code]=obj
    logger.info("Finished dict of %s" % str(modelName))

def objectProcessMain(logger,pobj,modelName,finyear):
    error=None
    finyear=str(finyear)
    myobjs=getProcessedObjects(logger,pobj,modelName,finyear)
    n=len(myobjs)
    logger.info("Number of %s to be processed is %s " % (modelName,str(n)))
    downloadNProcess(logger,pobj,myobjs,modelName=modelName,method='process')
    return error

def getProcessedObjects(logger,pobj,modelName,finyear):
  if modelName == "Jobcard":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (panchayat=pobj.panchayat) )
  elif ( ( modelName == "FTO") or (modelName == "RejectedPayment")):
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,block=pobj.block) )
  else:
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,panchayat=pobj.panchayat,isComplete=False) )

  return myobjs


def downloadFTO(logger,pobj,obj):
  error=None
  s3url=None
  finyear=obj.finyear
  fullfinyear=getFullFinYear(finyear)
  r=requests.get(obj.ftoURL)
  if r.status_code==200:
    #logger.info("Downloaded Successfully")
    myhtml=r.content
    error,myTable=validateNICReport(logger,pobj,myhtml)
    if error is None:  
      outhtml=''
      outhtml+=getCenterAlignedHeading("FTO Detail Table")
      outhtml+=stripTableAttributes(myTable,"myTable")
      title="FTO: %s state:%s District:%s block:%s  " % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="%s.html" % (obj.ftoNo)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,"DATA","FTO",fullfinyear,filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
      #logger.debug("Save muster %s " % str(obj.id))
  else:
    error="could not download fto"

  obj=FTO.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def processFTO(logger,pobj,obj):
  myhtml=None
  csvArray=[]
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    isComplete=True
    logger.info("Processing FTO ")
    allWagelistFound=True
    allJobcardFound=True
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    mytable=htmlsoup.find('table',id="myTable")
    rows=mytable.findAll('tr')
    for row in rows:
      if pobj.jobcardPrefix in str(row):
        cols=row.findAll('td')
        refNo=cols[3].text.lstrip().rstrip()
        col2=cols[2].text.lstrip().rstrip()
        col2Array=col2.split("(")
        jobcard=col2Array[0].lstrip().rstrip()
        wagelistNo=cols[6].text.lstrip().rstrip()
        wagelistCode="%s_%s_%s" % (pobj.blockCode,obj.finyear,wagelistNo)
        try:
          jobcardObj=pobj.codeObjDict[jobcard]
        except:
          jobcardObj=None
          allJobcardFound=False 
          
#          logger.info("jobcard not found")
        try:
          wagelistObj=pobj.codeObjDict[wagelistCode]
        except:
          finyearMinus1=str(int(obj.finyear)-1)
          wagelistObj=Wagelist.objects.filter(block=pobj.block,finyear=finyearMinus1,wagelistNo=wagelistNo).first()
          if wagelistObj is None:
            finyearMinus1=str(int(finyearMinus1)-1)
            wagelistObj=Wagelist.objects.filter(block=pobj.block,finyear=finyearMinus1,wagelistNo=wagelistNo).first()
          if wagelistObj is None:
            allWagelistFound=False
          logger.info(wagelistObj)
        transactionDate=getDateObj(cols[4].text.lstrip().rstrip())
        processDate=getDateObj(cols[13].text.lstrip().rstrip())
        primaryAccountHolder=cols[7].text.lstrip().rstrip()
        status=cols[12].text.lstrip().rstrip()
        ftoIndex=cols[0].text.lstrip().rstrip()
        rejectionReason=cols[16].text.lstrip().rstrip()
        favorBankAPB=cols[17].text.lstrip().rstrip()
        IINBankAPB=cols[18].text.lstrip().rstrip()
        rejectionReason=cols[16].text.lstrip().rstrip()
        creditedAmount=cols[11].text.lstrip().rstrip()
        if ( (jobcardObj is None) or (wagelistObj is None) or (processDate is None)):
          isComplete=False
        try:
          ft=PaymentTransaction.objects.create(fto=obj,ftoIndex=ftoIndex)
        except:
          ft=PaymentTransaction.objects.filter(fto=obj,ftoIndex=ftoIndex).first()
        ft.transactionDate=transactionDate
        ft.processDate=processDate
        ft.primaryAccountHolder=primaryAccountHolder
        ft.status=status
        ft.rejectionReason=rejectionReason
        ft.creditedAmount=creditedAmount
        ft.wagelist=wagelistObj
        ft.jobcard=jobcardObj
        ft.referenceNo=refNo
        ft.jobcardRaw=jobcard
        ft.favorBankAPB=favorBankAPB
        ft.IINBankAPB=IINBankAPB
        ft.save()
    obj.allWagelistFound=allWagelistFound
    obj.allJobcardFound=allJobcardFound
    obj.isComplete=isComplete
    obj.save()

def fetchJobcardTelangana(logger,tjobcard):
  url='http://www.nrega.telangana.gov.in/Nregs/'
  response=requests.get(url)
  cookies = response.cookies

  #logger.debug(response.cookies)

  headers = {
    'Host': 'www.nrega.telangana.gov.in',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': 'http://www.nrega.telangana.gov.in/Nregs/',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
  }

  params = (
    ('requestType', 'HouseholdInf_engRH'),
    ('actionVal', 'SearchJOBNew'),
  )

  data = {
  'input2': tjobcard
  }

  response = requests.post('http://www.nrega.telangana.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, data=data)

  return response.text

def fetchPaymentInformationTelangana(logger,obj):
  myhtml=None
  headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Connection': 'keep-alive',
    'Host': 'www.nrega.telangana.gov.in',
    'Referer': 'http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=HouseholdInf_engRH&actionVal=SearchJOBNew',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
}

  params = (
    ('requestType', 'HouseholdInf_engRH'),
    ('actionVal', 'SearchJOBPayment'),
    ('JOB_No', obj.tjobcard),
  )

  response = requests.get('http://www.nrega.telangana.gov.in/Nregs/FrontServlet', headers=headers, params=params)
  if response.status_code == 200:
    myhtml=response.content
  return myhtml

def validateJobcardDataTelangana(logger,myhtml,paymenthtml):
  habitation=None
  result = re.search('Habitation(.*)nbsp',myhtml)
  if result is not None:
     #logger.debug("Found")
     searchText=result.group(1)
     habitation=searchText.replace("&nbsp","").replace(":","").replace(";","").replace("&","").lstrip().rstrip()
     #logger.debug(habitation)
  error=None
  jobcardTable=None
  workerTable=None
  aggregateTable=None
  paymentTable=None
  error="noError"
  bs = BeautifulSoup(myhtml, "html.parser")
  bs = BeautifulSoup(myhtml, "lxml")
  main1 = bs.find('div',id='main1')

  if main1 != None:
    table1 = main1.find('table')

    jobcardTable = table1.find('table', id='sortable')

    workerTable = jobcardTable.findNext('table', id='sortable')
  main2 = bs.find(id='main2')
  if main2 is not  None:
    aggregateTable = main2.find('table')
  main3 = bs.find(id='main3')

  paymentsoup=BeautifulSoup(paymenthtml,"lxml")
  tables=paymentsoup.findAll("table")
  for table in tables:
    if "Epayorder No:" in str(table):
      paymentTable=table 
 #if aggregateTable is None:
 #  error+="Aggregate Table not found"
 #if paymentTable is None:
 #  error+="Payment Table not found"
  if jobcardTable is None:
    error+="jobcardTable not found"
  if workerTable is None:
    error+="WorkerTable not found     " 
  if paymentTable is None:
    error+="paymentTable not found"
  if error == "noError":
    error=None
  return error,habitation,jobcardTable,workerTable,paymentTable


def telanganaJobcardDownload(logger,pobj,obj):
    #logger.info("Downloading tjobcard %s " % obj.tjobcard)
    stateName=pobj.stateName
    districtName=pobj.districtName
    blockName=pobj.blockName
    panchayatName=pobj.panchayatName
    error="startDownload"
    downloadCount = 0
    myhtml=fetchJobcardTelangana(logger,obj.tjobcard)
    paymenthtml=fetchPaymentInformationTelangana(logger,obj)
    error,villageName,jobcardTable,workerTable,paymentTable=validateJobcardDataTelangana(logger,myhtml,paymenthtml)
    #logger.info(villageName)
    if error is None:
        
      try:
        myVillage=Village.objects.create(panchayat=pobj.panchayat,name=villageName)
      except:
        myVillage=Village.objects.filter(panchayat=pobj.panchayat,name=villageName).first()
      obj.village=myVillage
      obj.save()

      outhtml=''
      outhtml+=getCenterAlignedHeading("Jobcard Details")      
      outhtml+=stripTableAttributes(jobcardTable,"jobcardTable")
      outhtml+=getCenterAlignedHeading("Worker Details")      
      outhtml+=stripTableAttributes(workerTable,"workerTable")
      outhtml+=getCenterAlignedHeading("Aggregate Work Details")      
   #   outhtml+=stripTableAttributes(aggregateTable,"aggregateTable")
      outhtml+=getCenterAlignedHeading("Payment Details")      
      outhtml+=stripTableAttributes(paymentTable,"paymentTable")
      title="Jobcard Details state:%s District:%s block:%s panchayat: %s jobcard:%s " % (stateName,districtName,blockName,panchayatName,obj.tjobcard)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      myhtml=outhtml
      filename="%s.html" % (obj.tjobcard)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,pobj.panchayatSlug,"DATA","JOBCARDS",filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
    obj=Jobcard.objects.filter(id=obj.id).first()
    updateObjectDownload(logger,obj,error,s3url)


def downloadJobcard(logger,pobj,obj):
  error=None
  s3url=None
  #url="http://%s/citizen_html/jcr.asp?reg_no=%s&panchayat_code=%s" % (pobj.crawlIP,obj.jobcard,pobj.panchayatCode)
  url="http://%s/citizen_html/jcr.asp?reg_no=%s&panchayat_code=%s" % (pobj.searchIP,obj.jobcard,pobj.panchayatCode)
  #logger.debug(url)
  r=requests.get(url)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    error,jobcardTable,workerTable,demandTable,workTable=validateJobcardHTML(logger,obj,myhtml)
    #logger.info(error)
    if error is None:  
      outhtml=''
      outhtml+=getCenterAlignedHeading("Jobcard Summary Table")
      outhtml+=stripTableAttributes(jobcardTable,"jobcardTable")
      outhtml+=getCenterAlignedHeading("Worker Summary Table")
      outhtml+=stripTableAttributes(workerTable,"workerTable")
      outhtml+=getCenterAlignedHeading("Demand Details")
      outhtml+=stripTableAttributes(demandTable,"demandTable")
      outhtml+=getCenterAlignedHeading("Work Details")
      baseURL="http://%s/placeHodler1/placeHolder2/" % (pobj.crawlIP)  # Because thelink on html page go back 2 levles
      outhtml+=stripTableAttributesPreserveLinks(workTable,"workTable",baseURL)
      title="Jobcard: %s state:%s District:%s block:%s panchayat:%s" % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      jobcardSlug=obj.jobcard.replace("/","_")
      filename="%s.html" % (jobcardSlug)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,pobj.panchayatSlug,"DATA","Jobcard",filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
      #logger.debug("Save muster %s " % str(obj.id))
  else:
    error="could not download jobcard"

  obj=Jobcard.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def validateJobcardHTML(logger,obj,myhtml):
  error=None
  demandTable=None
  jobcardTable=None
  workTable=None
  workerTable=None
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll("table")
#  with open("/tmp/%s.html" % (obj.jobcard.replace("/","_")),"wb") as f:
#    f.write(myhtml)
  for table in tables:
    if "Date from which employment requested" in str(table):
      demandTable=table
    elif "Aadhar No" in str(table):
      workerTable=table
    elif "Date from which Employment Availed" in str(table):
      workTable=table
#    elif obj.jobcard in str(table):
    elif obj.jobcard in str(table):
      jobcardTable=table
  if jobcardTable is None:
    error="Jobcard Table not found"
  if workerTable is None:
    error="Worker Table not found"
  elif demandTable is None:
    error="demandTable not found"
  elif workTable is None:
    error="workTable not found"
  return error,jobcardTable,workerTable,demandTable,workTable

def processJobcard(logger,pobj,obj):
  error=None
  myhtml=None
  isComplete=False
  allWorkerFound=True
  demandDict={}
  totalDemandDict={}
  totalWorkDict={}
  for finyear in range(int(pobj.cobj.startFinYear),int(pobj.cobj.endFinYear)+1):
     totalDemandDict[str(finyear)]=0
     totalWorkDict[str(finyear)]=0
  
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  logger.info(obj.jobcard)
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    #Process Work Demand
    demandTable=htmlsoup.find("table",id="demandTable")
    if demandTable is not None:
      rows=demandTable.findAll('tr')
      previousName=None
      for row in rows:
        if "Demand Id" not in str(row):
          cols=row.findAll('td')
          demandID=cols[1].text.lstrip().rstrip()
          name=cols[2].text.lstrip().rstrip()
          if name == "":
            name=previousName
          else:
            previousName=name
          workDemandDateString=cols[3].text.lstrip().rstrip()
          code="%s_%s" % (name,workDemandDateString)
          workDemandDate=getDateObj(workDemandDateString)
          daysDemanded=cols[4].text.lstrip().rstrip()
          finyear=str(getFinYear(dateObj=workDemandDate))
          #logger.info("Name %s  finyear %s workDemandDate %s " % (name,finyear,str(workDemandDate)))
          if int(finyear) >= int(pobj.cobj.startFinYear):
            workerCode="%s_%s" % (obj.jobcard,name)
            myWorker=pobj.codeObjDict[workerCode]
            if myWorker is not None:
              #logger.info(myWorker)
              wd=WorkPayment.objects.filter(worker=myWorker,workDemandDate=workDemandDate).first()
              if wd is None:
                wd=WorkPayment.objects.create(worker=myWorker,workDemandDate=workDemandDate)
              demandDict[code]=wd.id
              wd.daysDemanded=daysDemanded
              wd.demandID=demandID
              wd.finyear=finyear
              
              wd.save()
              totalDemandDict[finyear]=totalDemandDict[finyear]+int(daysDemanded)
            else:
              allWorkerFound=False
    #logger.info("All worker Found %s " % str(allWorkerFound))
    workTable=htmlsoup.find("table",id="workTable")
    lastWorkDateDict={}
    defaultLastWorkDate=getDateObj("01/01/1970")
    if workTable is not None:
      rows=workTable.findAll('tr')
      previousName=None
      for row in rows:
        if "Date from which Employment Availed" not in str(row):
          cols=row.findAll('td')
          name=cols[1].text.lstrip().rstrip()
          if name == "":
            name=previousName
          else:
            previousName=name
          srno=cols[0].text.lstrip().rstrip()
          #logger.info("processing %s %s" % (srno,name))
          workDateString=cols[2].text.lstrip().rstrip()
          workDate=getDateObj(workDateString)
          workDateMinusFour=workDate-datetime.timedelta(days=4)
          daysAllocated=cols[3].text.lstrip().rstrip()
          workName=cols[4].text.lstrip().rstrip()
          amountDue=cols[6].text.lstrip().rstrip()
          finyear=str(getFinYear(dateObj=workDate))
          #logger.info(finyear)
          
          if int(finyear) >= int(pobj.cobj.startFinYear):
            dwd=None
            myMuster=None
            code="%s_%s" % (name,workDateString)
            workerCode="%s_%s" % (obj.jobcard,name)
            #logger.info("code %s workercode %s " % (code,workerCode))
            myWorker=pobj.codeObjDict[workerCode]
            if myWorker is not None:
              totalWorkDict[finyear]=totalWorkDict[finyear]+int(daysAllocated)
            musterLink=cols[5].find('a')
            if musterLink is not None:
              musterURL=musterLink['href']
              parsedURL=parse.urlsplit(musterURL)
              queryDict=dict(parse.parse_qsl(parsedURL.query))
              emusterno=queryDict.get('msrno',None)
              fullFinYear=queryDict.get('finyear',None)
              dateFromString=queryDict.get('dtfrm',None)
              dateToString=queryDict.get('dtto',None)
              workCode=queryDict.get('workcode',None)
              musterPanchayatCode=queryDict.get('panchayat_code',None)
              newMusterURL="http://%s/citizen_html/musternew.asp?id=1&msrno=%s&finyear=%s&workcode=%s&panchayat_code=%s" % (pobj.crawlIP,emusterno,fullFinYear,workCode,musterPanchayatCode)
              #logger.info(emusterno)
              try:
                myMuster=Muster.objects.create(block=pobj.block,finyear=finyear,musterNo=emusterno)
              except:
                myMuster=Muster.objects.filter(block=pobj.block,finyear=finyear,musterNo=emusterno).first()
              myMuster.dateFrom=getDateObj(dateFromString)
              myMuster.dateTo=getDateObj(dateToString)
              myMuster.workCode=workCode
              myMuster.workName=workName 
              myMuster.musterType='10'
              myMuster.newMusterURL=newMusterURL
              myMuster.panchayat=pobj.panchayat
              #logger.info(myMuster.id)
              myMuster.save()
            if ( (myWorker is not None) and (myMuster is not None)):
              lastWorkDate=lastWorkDateDict.get(name,defaultLastWorkDate)
              wd=WorkPayment.objects.filter(worker=myWorker,workAllocatedDate=workDate).first()
              if wd is None:
                wd=WorkPayment.objects.filter(worker=myWorker,workDemandDate=workDate).first()
              if wd is None:
                wd=WorkPayment.objects.filter(worker=myWorker,workDemandDate__gt=lastWorkDate,workDemandDate__lte=workDate,daysDemanded__gte=daysAllocated,finyear=finyear).order_by("-workDemandDate").first()
              if wd is None:
                wd=WorkPayment.objects.create(worker=myWorker,workAllocatedDate=workDate,finyear=finyear,demandExists=False,daysDemanded=0)

              wd.workAllocatedDate=workDate
              wd.daysAllocated=daysAllocated
              wd.amountDue=amountDue
              wd.muster=myMuster
              lastWorkDateDict[name]=workDate
              wd.save()
            #logger.info(workName)
    #allLinks=html.find_all("a", href=re.compile("delayed_payment.aspx"))
  for finyear in range(int(pobj.cobj.startFinYear),int(pobj.cobj.endFinYear)+1):
    finyear=str(finyear)
    js=JobcardStat.objects.filter(jobcard=obj,finyear=finyear).first()
    if js is None:
      js=JobcardStat.objects.create(jobcard=obj,finyear=finyear)
    js.jobcardDaysDemanded=totalDemandDict[finyear]
    js.jobcardDaysWorked=totalWorkDict[finyear]
    js.save()
  return error

def downloadMusterNew(logger,pobj,obj):
  error=None
  s3url=None
  datefromstring=obj.dateFrom.strftime("%d/%m/%Y")
  datetostring=obj.dateTo.strftime("%d/%m/%Y")
  workName=obj.workName
  workCode=obj.workCode
  emusterno=obj.musterNo
  fullfinyear=getFullFinYear(obj.finyear)
  myhtml=fetchNewMuster(logger,pobj,obj)
  if myhtml is None:
    error="not able to download in new format"
  else:
    error,musterSummaryTable,musterTable=validateMusterHTML(logger,pobj,myhtml,workCode)
  if error is not None:
    myhtml=fetchOldMuster(logger,pobj,obj)
    error,musterSummaryTable,musterTable=validateMusterHTML(logger,pobj,myhtml,workCode)
    logger.info(f"Error is { error }")
  if myhtml is not None:
    if error is None:  
      outhtml=''
      outhtml+=getCenterAlignedHeading("Muster Summary Table")
      outhtml+=stripTableAttributes(musterSummaryTable,"musterSummary")
      outhtml+=getCenterAlignedHeading("Muster Detail Table")
      outhtml+=stripTableAttributes(musterTable,"musterDetails")
      title="Muster: %s state:%s District:%s block:%s  " % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="%s.html" % (emusterno)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,"DATA","Muster",fullfinyear,filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
      #logger.debug("Save muster %s " % str(obj.id))
  else:
    error="could not download muster"

  obj=Muster.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def fetchNewMuster(logger,pobj,obj):
  myhtml=None
  r=requests.get(obj.newMusterURL)
  #logger.info(r.status_code)
  if r.status_code==200:
    cookies=r.cookies
    headers = {
    'Host': pobj.crawlIP,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
     }
    #logger.info(pobj.panchayatCode[:7]) 
    params = (
      ('bc', pobj.panchayatCode[:7]),
      ('fy', getFullFinYear(obj.finyear)),
      ('q', '%s----' % (obj.musterNo)),
      ('sh', pobj.stateShortCode),
      ('t', 'D'),
      ('wc', '%s$$$' % (obj.workCode)),
     )
    url2="http://%s/citizen_html/msrLogic.asp?q=%s----&t=D&fy=%s&bc=%s&sh=%s&wc=%s$$$&sn=&dn=&bn=&pn=" % (pobj.crawlIP,obj.musterNo,getFullFinYear(obj.finyear),pobj.panchayatCode[:7],pobj.stateShortCode,obj.workCode)
    #logger.info(url2)
    #response = requests.get('http://%s/citizen_html/msrLogic.asp' % (pobj.crawlIP), headers=headers, params=params, cookies=cookies)
    response = requests.get(url2, headers=headers, cookies=cookies)
    if response.status_code == 200:
      myhtml=response.content
  return myhtml

def fetchOldMuster(logger,pobj,obj):
  myhtml=None
  musterURL=searchMusterURL(logger,pobj,obj)
  #logger.info(musterURL)
  if musterURL is not None:
    obj.musterURL=musterURL
    obj.newMusterFormat=False
    obj.save()
    r=requests.get(musterURL)
    cookies=r.cookies
    time.sleep(3)
    r=requests.get(musterURL,cookies=cookies)
    if r.status_code == 200:
      myhtml=r.content
  return myhtml

def searchMusterURL(logger,pobj,obj):
  musterURL=None
  fullFinYear=getFullFinYear(obj.finyear)
  digest=getMusterDigest(logger,pobj,obj)
  #logger.info(digest)
  searchURL="http://%s/netnrega/master_search1.aspx?flag=2&wsrch=msr&district_code=%s&state_name=%s&district_name=%s&short_name=%s&srch=%s&Digest=%s" % (pobj.searchIP,pobj.districtCode,pobj.stateName,pobj.districtName,pobj.stateShortCode,obj.musterNo,digest)
  logger.info(searchURL)
  r=requests.get(searchURL)
  if r.status_code == 200:
    shtml=r.content
    ssoup=BeautifulSoup(shtml,"lxml")
    validation = ssoup.find(id='__EVENTVALIDATION').get('value')
    viewState = ssoup.find(id='__VIEWSTATE').get('value')
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}


    data = {
  '__EVENTTARGET': 'ddl_yr',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState,
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation,
  'ddl_yr': fullFinYear, 
   }   
    cookies=r.cookies
    #logger.info(cookies)
    response = requests.post(searchURL, headers=headers,cookies=cookies, data=data)
    logger.info(response.status_code)
    if response.status_code == 200:
      myhtml=response.content.decode("utf-8")
      musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
      myhtml=re.sub(musterregex,"",myhtml)
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      allLinks=htmlsoup.find_all("a", href=re.compile("musternew.aspx"))
      for a in allLinks:
        #if pobj.panchayatCode in a['href']:
        if obj.workCode.replace("/","%2f") in a['href']:
          musterURL="http://%s/netnrega/%s" % (pobj.searchIP,a['href'])
  return musterURL

def validateMusterHTML(logger,pobj,myhtml,workCode):
  error=None
 #myhtml=re.sub(musterregex,"",myhtml.decode("UTF-8"))
 #with open("/tmp/muster.html","w") as f:
 #  f.write(myhtml)
 #logger.info(workCode)
  musterSummaryTable=None
  musterTable=None
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll('table')
  for table in tables:
    if workCode in str(table):
      musterSummaryTable=table
    elif jobcardPrefix in str(table):
      musterTable=table
  if musterSummaryTable is None:
    error="Muster Summary Table not found"
  elif musterTable is None:
    error="Muster Table not found"
  return error,musterSummaryTable,musterTable


def processMuster(logger,pobj,obj):
  musterStartAttendanceColumn=4
  musterEndAttendanceColumn=19
  myhtml=None
  isComplete=False
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    remarks=''
    finyear=obj.finyear
    allWorkerFound=True
    allWagelistFound=True
    allWDFound=True
    isComplete=True

    htmlsoup=BeautifulSoup(myhtml,"lxml")
    ptds=htmlsoup.find_all("td", text=re.compile("Payment Date"))
    paymentDate=None
    if len(ptds) == 1:
      ptdText=ptds[0].text
      paymentDateString=ptdText.split(":")[1].lstrip().rstrip()
      paymentDate=getDateObj(paymentDateString)
    mytable=htmlsoup.find('table',id="musterDetails")
    rows  = mytable.findAll('tr')
    sharpeningIndex=None
    if obj.newMusterFormat == False:
      sharpeningIndex=getSharpeningIndex(logger,rows[0])
      if sharpeningIndex is None:
        error="Sharpening Index not Found"
        obj.remarks=error
        obj.save()
        return
      musterEndAttendanceColumn=sharpeningIndex-5

    
    reMatchString="%s-" % (pobj.stateShortCode)
    for row in rows:
      wdRemarks=''
      cols=row.findAll("td")
	
      if len(cols) > 7:
        nameandjobcard=cols[1].text.lstrip().rstrip()
        if pobj.stateShortCode in nameandjobcard:
          musterIndex=cols[0].text.lstrip().rstrip()
          nameandjobcard=nameandjobcard.replace('\n',' ')
          nameandjobcard=nameandjobcard.replace("\\","")
          nameandjobcardarray=re.match(r'(.*)'+reMatchString+'(.*)',nameandjobcard)
          name_relationship=nameandjobcardarray.groups()[0]
          name=name_relationship.split("(")[0].lstrip().rstrip()
          #logger.info(name)
          jobcard=reMatchString+nameandjobcardarray.groups()[1].lstrip().rstrip()
           #logger.info(nameandjobcard)
           #logger.info(nameandjobcardarray)
           #logger.info(name_relationship)
           #logger.info(name)
          if obj.newMusterFormat==True:
            totalWage=cols[24].text.lstrip().rstrip()
            accountNo=cols[25].text.lstrip().rstrip()
            dayWage=cols[21].text.lstrip().rstrip()
            daysWorked=cols[20].text.lstrip().rstrip()
            wagelistNo=cols[29].text.lstrip().rstrip()
            bankName=cols[26].text.lstrip().rstrip()
            branchName=cols[27].text.lstrip().rstrip()
            branchCode=cols[28].text.lstrip().rstrip()
            creditedDateString=cols[30].text.lstrip().rstrip()
          else:
            totalWage=cols[sharpeningIndex+1].text.lstrip().rstrip()
            accountNo=""
            dayWage=cols[sharpeningIndex-3].text.lstrip().rstrip()
            daysWorked=cols[sharpeningIndex-4].text.lstrip().rstrip()
            wagelistNo=cols[sharpeningIndex+5].text.lstrip().rstrip()
            bankName=cols[sharpeningIndex+2].text.lstrip().rstrip()
            branchName=cols[sharpeningIndex+3].text.lstrip().rstrip()
            branchCode=cols[sharpeningIndex+4].text.lstrip().rstrip()
            creditedDateString=cols[sharpeningIndex+7].text.lstrip().rstrip()


          creditedDate=getDateObj(creditedDateString)

          daysProvided=0
          for attendanceIndex in range(int(musterStartAttendanceColumn),int(musterEndAttendanceColumn)+1):
            if cols[attendanceIndex].text.lstrip().rstrip() != "":
              daysProvided=daysProvided+1

          workerCode="%s_%s" % (jobcard,name)
          wagelistCode="%s_%s_%s" % (pobj.blockCode,finyear,wagelistNo)
          musterCode="%s_%s_%s" % (pobj.blockCode,finyear,obj.musterNo)
          #logger.info(workerCode)
          try:
            myWorker=pobj.codeObjDict[workerCode]
          except:
            myWorker=None
            remarks+="Worker not found %s " % (workerCode)
            allWorkerFound=False
          try:
            myWagelist=pobj.codeObjDict[wagelistCode]
          except:
            myWagelist=None
            remarks+="Wagelist not found %s " % (wagelistCode)
            allWagelistFound=False

          if ((creditedDate is None) and (int(totalWage) > 0)) or (myWorker is None) or (myWagelist is None):
            isComplete=False
          wd=None
          
          #logger.info(f"My worker si { myWorker }")
          #logger.info(f"My Wagelist is { myWagelist }")
          if myWorker is not None:
            wd=WorkPayment.objects.filter(muster=obj,worker=myWorker).first()
          if wd is not None:
            if myWagelist is not None:
              wd.curWagelist=myWagelist
              wd.wagelist.add(myWagelist)
            wd.musterIndex=musterIndex
            wd.accountNo=accountNo
            wd.bankName=bankName
            wd.branchName=branchName
            wd.branchCode=branchCode
            wd.totalWage=totalWage
            wd.dayWage=dayWage
            wd.daysWorked=daysWorked
            wd.daysProvided=daysProvided
            wd.creditedDate=creditedDate
            wd.remarks=wdRemarks
            wd.save()
            #logger.info("Saving WD ID %s " % (str(wd.id)))
          else:
            e="worker %s muster %s not found" % (str(myWorker),str(obj))
            remarks+=e
            allWDFound=False

    obj.allWorkerFound=allWorkerFound
    obj.allWagelistFound=allWagelistFound
    obj.allWDFound=allWDFound
    obj.paymentDate=paymentDate
    obj.isComplete=isComplete
    obj.remarks=remarks
    obj.save()

def getMusterDigest(logger,pobj,obj):
  searchURL="http://mnregaweb4.nic.in/netnrega/nregasearch1.aspx"
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,requestType="GET")
  #logger.info(validation)
  ##logger.info(viewState)
  #logger.info(cookies)
  headers = {
    'Host': 'mnregaweb4.nic.in',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': searchURL,
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}

  data = {
  '__EVENTTARGET': 'ddl_search',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'txt_keyword2': '',
  'searchname': '',
  'hhshortname': '',
  'districtname': '',
  'statename': '',
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': 'ddl_state',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': pobj.stateCode,
  'txt_keyword2': '',
  'searchname': 'MusterRoll',
  'hhshortname': '',
  'districtname': '',
  'statename': '',
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': 'ddl_district',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': pobj.stateCode,
  'ddl_district': pobj.districtCode,
  'txt_keyword2': '',
  'searchname': 'MusterRoll',
  'hhshortname': pobj.stateShortCode,
  'districtname': '',
  'statename': pobj.stateName,
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': '',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': pobj.stateCode,
  'ddl_district': pobj.districtCode,
  'txt_keyword2': obj.musterNo,
  'btn_go': 'GO',
  'searchname': 'MusterRoll',
  'hhshortname': pobj.stateShortCode,
  'districtname': pobj.districtName,
  'statename': pobj.stateName,
  'lab_lang': 'kruti dev 010'
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  digest=re.search(r'Digest=(.*?)\'\,', myhtml.decode('UTF-8')).group(1)
  return digest 

def nicRequest(logger,url,cookies=None,data=None,headers=None,requestType=None):
  myhtml=None
  viewState=None
  validation=None
  rcookies=None
  if requestType == "GET":
    r=requests.get(url)
  else:
    r = requests.post(url, headers=headers, cookies=cookies, data=data)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
    rcookies=r.cookies
    
  return myhtml,viewState,validation,rcookies

def getSharpeningIndex(logger,row):
  #logger.info(row)
  sharpeningIndex=None
  if "Sharpening Charge" in str(row):
    cols=row.findAll("th")
    i=0
    for col in cols:
      if "Sharpening Charge" in col.text:
        sharpeningIndex=i
      i=i+1 
  return sharpeningIndex

def downloadRejectedPayment(logger,pobj,obj):
  s3url=None
  url=obj.url
  r=requests.get(url)
  error=None
  finyear=obj.finyear
  fullfinyear=getFullFinYear(finyear)
  logger.info("downloading rejected Payemnt %s " % str(url))
  if r.status_code == 200:
    myhtml=r.content
    error,myTable=validateNICReport(logger,pobj,myhtml)
    if myTable is not None:
      outhtml=''
      outhtml+=getCenterAlignedHeading("Rejected PaymentDetail")
      outhtml+=stripTableAttributes(myTable,"myTable")
      title="RejectedPayment : %s state:%s District:%s block:%s  " % (obj.referenceNo,pobj.stateName,pobj.districtName,pobj.blockName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="%s.html" % (obj.referenceNo)
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,"DATA","RejectedPayment",fullfinyear,filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
  else:
    error="Could not download Payment"

  obj=RejectedPayment.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def processRejectedPayment(logger,pobj,obj):
  myhtml=None
  isComplete=False
  finyear=obj.finyear
  referenceNo=obj.referenceNo
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    table=htmlsoup.find("table",id="myTable")
    if table is not None:
      rows=table.findAll('tr')
      for row in rows:
        if referenceNo in str(row):
          cols=row.findAll('td')
          wagelistNo=cols[0].text.lstrip().rstrip()
          jobcard=cols[1].text.lstrip().rstrip()
          name=cols[3].text.lstrip().rstrip()
          musterNo=cols[6].text.lstrip().rstrip()
          status=cols[8].text.lstrip().rstrip()
          workerCode="%s_%s" % (jobcard,name)
          musterCode="%s_%s_%s" % (pobj.blockCode,finyear,musterNo)
          wagelistCode="%s_%s_%s" % (pobj.blockCode,finyear,wagelistNo)
          try:
            myWorker=pobj.codeObjDict[workerCode]
          except:
            myWorker=None

          try:
            myMuster=pobj.codeObjDict[musterCode]
          except:
            finyearMinus1=str(int(obj.finyear)-1)
            myMuster=Muster.objects.filter(panchayat__block=pobj.block,finyear=finyearMinus1,musterNo=musterNo).first()
            if myMuster is None:
              finyearMinus1=str(int(finyearMinus1)-1)
              myMuster=Muster.objects.filter(panchayat__block=pobj.block,finyear=finyearMinus1,musterNo=musterNo).first()
            
          try:
            wagelistObj=pobj.codeObjDict[wagelistCode]
          except:
            finyearMinus1=str(int(obj.finyear)-1)
            wagelistObj=Wagelist.objects.filter(block=pobj.block,finyear=finyearMinus1,wagelistNo=wagelistNo).first()
            if wagelistObj is None:
              finyearMinus1=str(int(finyearMinus1)-1)
              wagelistObj=Wagelist.objects.filter(block=pobj.block,finyear=finyearMinus1,wagelistNo=wagelistNo).first()
            logger.info(wagelistObj)
          if ( (myWorker is not None) and (myMuster is not None)):
            myFTO=FTO.objects.filter(ftoNo=obj.ftoString).first()
            obj.worker=myWorker
            obj.muster=myMuster
            obj.wagelist=wagelistObj
            obj.fto=myFTO
            obj.status=status
            obj.save()

              
def downloadWorkDemand(logger,pobj,finyear):
  error=None
  reportType="workDemandHTML"
  reportName="Work Demand HTML"
  finyearMinus1=str(int(finyear)-1)
  finyear=str(finyear)
  nicFinYear=finyearMinus1+finyear
  fullFinYear=getFullFinYear(finyear)
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=3)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,reportThreshold=reportThreshold)
  logger.info("Work demand report updated in last three days = %s " % str(isUpdated))
  if isUpdated==False:
    panchayatURL="http://%s/netnrega/IndexFrame.aspx?lflag=eng&District_Code=%s&district_name=%s&state_name=%s&state_Code=%s&block_name=%s&block_code=%s&fin_year=%s&check=1&Panchayat_name=%s&Panchayat_Code=%s" %(pobj.crawlIP,pobj.districtCode,pobj.districtName,pobj.stateName,pobj.stateCode,pobj.blockName,pobj.blockCode,fullFinYear,pobj.panchayatName,pobj.panchayatCode)
    logger.info(panchayatURL)
    r=requests.get(panchayatURL)
    if r.status_code == 200:
      cookies=r.cookies
      logger.info("Downloaded Successfully")
      myhtml=r.text
      match = re.findall("demreport.aspx\?lflag=local&Digest=(.*?)\" target", myhtml)
      logger.info(match)
      if len(match) == 1:
        digestString=match[0]
      else:
        return "Digest not found"
      logger.debug("Digest variable is %s " % (digestString))
      headers = {
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Encoding': 'gzip, deflate',
       'Accept-Language': 'en-GB,en;q=0.5',
       'Connection': 'keep-alive',
       'Host': pobj.crawlIP,
     }

      params = (
       ('lflag', 'local'),
       ('Digest', digestString),
      )  

      r = requests.get('http://%s/netnrega/state_html/demreport.aspx' % (pobj.crawlIP), headers=headers, params=params, cookies=cookies)
      redirectURL="http://%s/Netnrega/writereaddata/state_out/demreport_%s_%s.html" % (pobj.crawlIP,pobj.panchayatCode,nicFinYear)
      logger.info(redirectURL)
      time.sleep(10)
      r=requests.get(redirectURL,cookies=cookies,headers=headers)
      logger.info(r.status_code)
      if r.status_code == 200:
        dhtml=r.content
        if "Data not available" in str(dhtml):
          error1=None
        elif "Report Completed" in str(dhtml):
          error1=validateAndSave(logger,pobj,dhtml,reportName,reportType,finyear)
        else:
          error1="Report not Download Completely"
      if error1 is not None:
        if error is None:
          error=error1
        else:
          error+=error1
  return error 
def processWorkDemand(logger,pobj,finyear):
  error=None
  jobcardStatDict=makehash()
  reportType="workDemandHTML"
  error1,myhtml=getReportHTML(logger,pobj,reportType,finyear)
  totalDemand=0
  if error1 is  None:
    logger.info("Report Found")
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    mytable=htmlsoup.find('table',id="myTable")
    if mytable is not None:
      rows = mytable.findAll('tr')
      i=0
      jobcard=''
      jobcardPrefix="%s-" % (pobj.stateShortCode)
      for row in rows:
        if "Villages" not  in str(row):
          cols=row.findAll('td')
          curjobcard=cols[2].text.lstrip().rstrip()
          if jobcardPrefix in curjobcard:
            jobcard=curjobcard
          workerName=cols[3].text.lstrip().rstrip()
          demandDateString=cols[5].text.lstrip().rstrip()
          demandDate=correctDateFormat(demandDateString)
          daysDemanded=cols[6].text.lstrip().rstrip()
          if daysDemanded.isdigit():
            daysDemanded=int(daysDemanded)
            #logger.info(finyear)
            try:
              curCount=int(jobcardStatDict[jobcard][finyear]['daysDemanded'])
            except:
              curCount=0
            jobcardStatDict[jobcard][finyear]['daysDemanded']=str(curCount+daysDemanded)

            #logger.info(str(pobj.jobcardStatDict[jobcard]))
            
            workerCode="%s_%s" % (jobcard,workerName)
            myWorker=Worker.objects.filter(code=workerCode).first()
            if myWorker is not None:
              totalDemand+=daysDemanded
              try:
                wd=WorkDemand.objects.create(worker=myWorker,workDemandDate=demandDate,daysDemanded=daysDemanded,finyear=finyear)
              except:
                error=1
          #logger.info("Jobcard %s worker %s Demanded Date %s days %s " % (jobcard,workerName,str(demandDate),str(daysDemanded)))
 
  for jobcard,jdict in jobcardStatDict.items():
    myJobcard=Jobcard.objects.filter(code=jobcard).first()
    for finyear,myDict in jdict.items():
      try:
        js=JobcardStat.objects.create(jobcard=myJobcard,finyear=finyear)
      except:
        js=JobcardStat.objects.filter(jobcard=myJobcard,finyear=finyear).first()
      js.nicDaysDemanded=myDict['daysDemanded']  
      js.save()
  ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
  ps.nicDaysDemanded=totalDemand
  ps.save()
  return error

def getMISDPLink(logger,pobj,finyear,stateURL):
  urlPrefix="http://mnregaweb4.nic.in/netnrega/state_html/"
  distURL=None
  blockURL=None
  panchayatURL=None
  r=requests.get(stateURL)
  if r.status_code == 200:
    stateHTML=r.content
    statesoup=BeautifulSoup(stateHTML,"lxml")
    allLinks=statesoup.find_all("a", href=re.compile(pobj.districtCode))
    if len(allLinks) == 1:
      myLink=allLinks[0]
      distURL="%s%s" % (urlPrefix,myLink['href'])
  logger.info(distURL)
  if distURL is not None:
    r=requests.get(distURL)
    if r.status_code == 200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      allLinks=htmlsoup.find_all("a", href=re.compile(pobj.blockCode))
      if len(allLinks) == 1:
        myLink=allLinks[0]
        blockURL="%s%s" % (urlPrefix,myLink['href'])
  logger.info(blockURL)
  if blockURL is not None:
    r=requests.get(blockURL)
    if r.status_code == 200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      allLinks=htmlsoup.find_all("a", href=re.compile(pobj.panchayatCode))
      logger.info(len(allLinks))
      if len(allLinks) == 1:
        myLink=allLinks[0]
        panchayatURL="%s%s" % (urlPrefix,myLink['href'])
  logger.info(panchayatURL)
  return panchayatURL

 
def downloadMISDPReport(logger,pobj,finyear):
  error=None
  reportName="MIS Delayed Payment Report"
  reportType="MISDPHTML"
  fullFinYear=getFullFinYear(finyear)
  obj=MISReportURL.objects.filter(finyear=finyear,state__code=pobj.stateCode).first()
  if obj is not None:
    stateURL=obj.delayPaymentURL
    panchayatURL=getMISDPLink(logger,pobj,finyear,stateURL)
    outhtml=''
    title="MIS Delayed Payment Report state:%s District:%s block:%s panchayat: %s finyear : %s  " % (pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,fullFinYear)
    if panchayatURL is not None:
      r=requests.get(panchayatURL)
      if r.status_code==200:
        myhtml=r.content
        error=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear)
  else:
    error="Delay Payment URL not found %s " % (finyear)
  if error is not None:
    logger.info("Error found in Delay payment report")
    return error
  return error

def processMISDPReport(logger,pobj,finyear):
  reportType="MISDPHTML"
  myTable=None
  myReport=Report.objects.filter(panchayat=pobj.panchayat,finyear=finyear,reportType=reportType).first()
  if myReport is not None:
    r=requests.get(myReport.reportURL)
    if r.status_code == 200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      myTable=htmlsoup.find("table",id="myTable")

  if myTable is not None:
    rows=myTable.findAll("tr")
    for row in rows:
      cols=row.findAll("td")
      jobcard=cols[1].text.lstrip().rstrip()
      if pobj.jobcardPrefix in jobcard:
        name=cols[2].text.lstrip().rstrip()
        paymentDateString=cols[4].text.lstrip().rstrip()
        paymentDate=getDateObj(paymentDateString)
        delayDays=cols[5].text.lstrip().rstrip()
        musterno=cols[6].text.lstrip().rstrip()
        compensation=cols[9].text.lstrip().rstrip()
        status=cols[10].text.lstrip().rstrip()

        workerCode="%s_%s" % (jobcard,name)
        musterCode="%s_%s_%s" % (pobj.blockCode,finyear,musterno)
        try:
          myWorker=pobj.codeObjDict[workerCode]
        except:
          myWorker = None
         
        try:
          myMuster=pobj.codeObjDict[musterCode]
        except:
          myMuster = None

        dp=DPTransaction.objects.filter(jobcard=jobcard,name=name,musterNo=musterno,finyear=finyear).first()
        if dp is None:
          dp=DPTransaction.objects.create(jobcard=jobcard,name=name,musterNo=musterno,finyear=finyear)
        dp.muster=myMuster
        dp.worker=myWorker
        dp.paymentDate=paymentDate
        dp.delayCompensation=compensation
        dp.delayDays=delayDays
        dp.status=status
        dp.save()

def processJobcardStat(logger,pobj,finyear):
  error=None
  remarks=''
  reportType="nicJobcardStat"
  reportName="NIC Jobcard Stat"
  totalWorkDays=0
  error1,myhtml=getReportHTML(logger,pobj,reportType,finyear)
  if error1 is None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find("table",id="myTable")
    if myTable is not None:
      rows=myTable.findAll("tr")

      for row in rows:
        if pobj.jobcardPrefix in str(row):
          cols=row.findAll("td")
          jobcard=cols[2].text.lstrip().rstrip()
          #logger.info(jobcard)
          nicDaysProvided=cols[4].text.lstrip().rstrip()
          try:
            myJobcard=pobj.codeObjDict[jobcard]
          except:
            myJobcard=None
          if myJobcard is None:
            remarks+="%s jobcard not found \n  " % (jobcard)
          else:
            try:
              js=JobcardStat.objects.create(jobcard=myJobcard,finyear=finyear)
            except:
              js=JobcardStat.objects.filter(jobcard=myJobcard,finyear=finyear).first()
            js.nicDaysProvided=nicDaysProvided
            totalWorkDays+=int(nicDaysProvided)
            js.save()
  ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
  ps.nicDaysProvided=totalWorkDays
  ps.save()
 
def getJobcardStat(logger,pobj,finyear):
  fullFinYear=getFullFinYear(finyear)
  stathtml=None
  url="http://%s/netnrega/state_html/empspecifydays.aspx?page=P&lflag=eng&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&fin_year=%s&Block_code=%s&"  % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,fullFinYear,pobj.blockCode)

  logger.info(url)
  with requests.Session() as session:
    #session.headers.update({'x-test': 'true'})
    session.headers['user-agent'] = 'Mozilla/5.0'
    r=session.get(url)
    if r.status_code ==200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      try:
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
      except:
        validation=''
      viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
      cookies=session.cookies
      logger.info('Cookies: ' + str(cookies)) #  + '==' + r.text)
#      logger.info('Request Headers: [%s]' % str(r.request.headers))
      headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
        'Host': pobj.crawlIP,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
        'X-MicrosoftAjax': 'Delta=true',
      } 

      params = (
        ('page', 'P'),
        ('lflag', 'eng'),
        ('state_name', pobj.stateName),
        ('state_code', pobj.stateCode),
        ('district_name', pobj.districtName),
        ('district_code', pobj.districtCode),
        ('block_name', pobj.blockName),
        ('fin_year', fullFinYear),
        ('Block_code', pobj.blockCode),
        ('', ''),
      )

      data = {
        'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$btn_pro',
        'ctl00$ContentPlaceHolder1$ddr_panch': pobj.panchayatCode,
        'ctl00$ContentPlaceHolder1$ddr_cond': 'gt',
        'ctl00$ContentPlaceHolder1$lbl_days': '0',
        'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btn_pro',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': viewState,
        '__EVENTVALIDATION': validation,
        '__VIEWSTATEENCRYPTED': '',
        '__ASYNCPOST': 'true',
        '': ''
      }
      url2='http://%s/netnrega/state_html/empspecifydays.aspx' % (pobj.crawlIP)
      #response = session.post(url2, headers=headers, params=params, cookies=cookies, data=data)
      response = session.post(url2, headers=headers, params=params, data=data, allow_redirects=False)
      if response.status_code == 200:
        logger.info("correct")
        cookies=session.cookies
        logger.info('Yippie = [%s]' % cookies)
        #cookies = requests.cookies.RequestsCookieJar()
        #logger.info('Oopsie = [%s]' % cookies)
  #      outhtml=r.content
        url3='http://%s/netnrega/state_html/empprovdays.aspx?lflag=eng&fin_year=%s&RegWorker=Y' % (pobj.crawlIP,fullFinYear)
        logger.info(url3)
        r1=session.get(url3, headers=headers, cookies=cookies)
        if r1.status_code == 200:
          stathtml=r1.content
          return stathtml


def getJobcardStat1(logger,pobj,finyear):
    stathtml=None
    fullFinYear=getFullFinYear(finyear)
    url="http://%s/netnrega/state_html/empspecifydays.aspx?page=P&lflag=eng&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&fin_year=%s&Block_code=%s&"  % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,fullFinYear,pobj.blockCode)
    logger.info(url)
    r=requests.get(url)
    if r.status_code ==200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
      #logger.info(validation)
      viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
      #logger.info(viewState)
      cookies=r.cookies
      logger.info(cookies)
      headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Host': pobj.crawlIP,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'X-MicrosoftAjax': 'Delta=true',
      } 


      data = {
  'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$btn_pro',
  'ctl00$ContentPlaceHolder1$ddr_panch': pobj.panchayatCode,
  'ctl00$ContentPlaceHolder1$ddr_cond': 'gt',
  'ctl00$ContentPlaceHolder1$lbl_days': '0',
  'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
  '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btn_pro',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState,
  '__EVENTVALIDATION': validation,
  '__ASYNCPOST': 'true',
  '': ''
   }
      url2='http://%s/netnrega/state_html/empspecifydays.aspx' % (pobj.crawlIP)
      #response = requests.post(url2, headers=headers, params=params, cookies=cookies, data=data)
      response = requests.post(url, headers=headers, cookies=cookies, data=data)
      if response.status_code == 200:
        logger.info("correct")
        #cookies=response.cookies
        logger.info(cookies)
        outhtml=r.content
        url3='http://%s/netnrega/state_html/empprovdays.aspx?lflag=eng&fin_year=%s&RegWorker=Y' % (pobj.crawlIP,fullFinYear)
        logger.info(url3)
        r1=requests.get(url3, headers=headers, cookies=cookies)
        if r1.status_code == 200:
          stathtml=r1.content
    return stathtml

        
def downloadJobcardStat(logger,pobj,finyear):
  error=None
  reportType="nicJobcardStat"
  reportName="NIC Jobcard Stat"
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType)
  if isUpdated==False:
    if pobj.crawlIP == "nregasp2.nic.in":
      myhtml=getJobcardStat1(logger,pobj,finyear)
    else:
      myhtml=getJobcardStat(logger,pobj,finyear)
    error1="Unable to download Jobcard Stat"
    if myhtml is not None:
      error1=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear,locationType=None)
    if error1 is not None:
      if "No Data Found" in str(myhtml):
        error=None
      else:
        error=error1
  return error

def matchTransactions(logger,pobj,finyear):
  wds=WorkPayment.objects.filter(muster__block=pobj.block,finyear=finyear,daysAllocated__gt=0)
  s=''
  i=len(wds)
  for wd in wds:
    i=i-1
    logger.info("Process %s-%s " % (str(i),str(wd.id)))
    dp=DPTransaction.objects.filter(worker=wd.worker,muster=wd.muster).first()
    if dp is not None:
      wd.dpTransaction=dp
      wd.isNICDelayAccounted=True
    wagelists=wd.wagelist.all()
    for wagelist in wagelists:
      logger.info(wagelist)
      wts=WagelistTransaction.objects.filter(workPayment__isnull = True,wagelist=wagelist,worker=wd.worker)
      if len(wts) == 1:
        matchedWT=wts.first()
      elif len(wts) > 1:
         wts1=WagelistTransaction.objects.filter(workPayment__isnull=True, wagelist=wagelist,worker=wd.worker,totalWage=wd.totalWage)
         if len(wts1) >= 1:
           matchedWT=wts1.first()
         else:
           matchedWT=wts.first()
      else:
        matchedWT = None
      if matchedWT is not None:
        wd.wagelistTransaction.add(matchedWT)
        matchedWT.workPayment=wd
        matchedWT.save()
        logger.info("Matched WT %s to WD %s " % (str(matchedWT.id),str(wd.id)))  
      else:
        logger.info("COuld not find any WT for WD %s " % (str(wd.id)))
        s+="%s\n" % str(wd.id)
    wd.save()
  with open("/tmp/wd.csv","w") as f:
    f.write(s)

  rps=RejectedPayment.objects.filter(block=pobj.block)
  for rp in rps:
    if rp.workPayment is None:
      wp=WorkPayment.objects.filter(worker=rp.worker,muster=rp.muster).first()
      if wp is not None:
        rp.workPayment=wp
        rp.save()
    else:
      wp=rp.workPayment 
    if wp is not None:
      wt=WagelistTransaction.objects.filter(workPayment=wp,wagelist=rp.wagelist).first()
      if wt is not None:
        wt.rejectedPayment=rp
        logger.info("Mapped Wagelist Transaction %s to Rejected Payment %s " % (str(wt.id),str(rp.id)))
        wt.save()

def getAccuracy(input1,input2):
  if ((input1 is None) or (input2 is None)):
    accuracy=0
  elif ((input1 == 0) and (input2 == 0)):
    accuracy=100
  elif ((input1 == 0) or (input2 == 0)):
    accuracy = 0
  elif (input1 >= input2):
    accuracy = int(input2*100/input1)
  elif (input2 > input1):
    accuracy = int(input1*100/input2)
  else:
    accuracy=0
  return accuracy    


def computePanchayatStat(logger,pobj,finyear):
  ps=myPanchayatStat=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()

  ps.mustersTotal=len(Muster.objects.filter(panchayat=pobj.panchayat,finyear=finyear))
  ps.mustersPendingDownload=len(Muster.objects.filter(panchayat=pobj.panchayat,finyear=finyear,isDownloaded=False))
  ps.mustersPendingDownloadPCT=computePercentage(ps.mustersPendingDownload,ps.mustersTotal)
  ps.mustersMissingApplicant=len(Muster.objects.filter(panchayat=pobj.panchayat,finyear=finyear,isDownloaded=True,allWorkerFound=False))
  ps.mustersInComplete=len(Muster.objects.filter(panchayat=pobj.panchayat,finyear=finyear,isDownloaded=True,isComplete=False))


  workDaysAccuracy=getAccuracy(ps.nicEmploymentProvided,ps.musterDaysWorked)
  logger.info("nic workdays %s libtechworkdays %s provided %s  Accuracy %s " % (str(ps.nicEmploymentProvided),str(ps.musterDaysWorked),str(ps.musterDaysProvided),str(workDaysAccuracy)))
  accuracy=workDaysAccuracy
  ps.accuracy=accuracy
  ps.save()
  logger.info("Accuracy %s " % str(accuracy)) 
  return accuracy

def createDetailWorkPaymentReport(logger,pobj,finyear,reportObjs=None,filename=None):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',',
                lineterminator='\r\n',
                quotechar = '"'
                )
  table=[]
  locationArrayLabel=["state","district","block","panchayat","village","stateCode"]
  locationArraySource=["location"]*6
  workerArrayLabel=["workerID","name","fatherHusbandName","gender","age"]
  workerArraySource=["worker"]*5
  jobcardArrayLabel=["jobcardID","jobcard","jcNo","caste","headOfHousehold"]
  jobcardArraySource=["jobcard"]*5
  workDetailArrayLabel=["workDetailID","DemandDate","daysDemanded","daysAllocated","demandExists","daysWorked","dayWage","totalWage","accountNo","bankName","branchName","branchCode","creditedDate"]
  workDetailArraySource=["workDetail"]*13
  musterArrayLabel=["musterID","musterNo","workCode","workname","dateFrom","dateTo","paymentDate"]
  musterArraySource=["muster"]*7
  wagelistArrayLabel=["wagelistID","txnStatus","attemptCount","wagelistno","wgGenerateDate"]
  wagelistArraySource=["wagelist"]*5
  ftoArrayLabel=["ftoID","ftoNo","secondSignatoryDate","paymentMode","ftoFinYear"]
  ftoArraySource=["fto"]*5
  rpArrayLabel=["rejPayID","rejectionDate","referenceNo","rejectionReason","rpStatus"]
  rpArraySource=["rp"]*5
  dpArrayLabel=["DPid","delayDays","delayCompensation","delayStatus"]
  dpArraySource=["dp"]*4
  computedArrayLabel=["oldWorkerID"]
#  a=workerArraySource+jobcardArraySource+workDetailArraySource+musterArraySource+wagelistArraySource+ftoArraySource+rpArraySource+dpArraySource
#  w.writerow(a)
#  table.append(a)
  a=locationArrayLabel+workerArrayLabel+jobcardArrayLabel+workDetailArrayLabel+musterArrayLabel+wagelistArrayLabel+ftoArrayLabel+rpArrayLabel+dpArrayLabel+computedArrayLabel
  w.writerow(a)
  table.append(a)
  if reportObjs is not None:
    workRecords=reportObjs
  else:
    workRecords=WorkPayment.objects.filter( Q (worker__jobcard__panchayat=pobj.panchayat,finyear=finyear))
  logger.debug("Total Work Records: %s " %str(len(workRecords)))
  for wd in workRecords:
    logger.info(wd)
    wtArray=[]
    wts=WagelistTransaction.objects.filter(workPayment=wd).order_by("wagelist__generateDate")
    for wt in wts:
      wtArray.append(wt)
    if len(wts) == 0:
      wtArray.append(None)
    wdArray=[str(wd.id),str(wd.workDemandDate),str(wd.daysDemanded),str(wd.daysAllocated),str(wd.demandExists),str(wd.daysWorked),str(wd.dayWage),str(wd.totalWage),wd.accountNo,wd.bankName,wd.branchName,wd.branchCode,str(wd.creditedDate)]

    workerArray=[""]*5
    jobcardArray=[""]*5
    locationArray=[""]*6
    computedArray=[""]
    if wd.worker is not None:
      workerArray=[str(wd.worker.id),wd.worker.name,wd.worker.fatherHusbandName,wd.worker.gender,wd.worker.age]
      computedArray=[str(wd.worker.oldID)]
      jobcardArray=[str(wd.worker.jobcard.id),wd.worker.jobcard.jobcard,wd.worker.jobcard.jcNo,wd.worker.jobcard.caste,wd.worker.jobcard.headOfHousehold]
      if wd.worker.jobcard.village is not None:
        villageName=wd.worker.jobcard.village.name
      else:
        villageName=''
      locationArray=[wd.worker.jobcard.panchayat.block.district.state.name,wd.worker.jobcard.panchayat.block.district.name,wd.worker.jobcard.panchayat.block.name,wd.worker.jobcard.panchayat.name,villageName,wd.worker.jobcard.panchayat.block.district.state.stateShortCode]
    musterArray=[""]*7
    if wd.muster is not None:
      musterArray=[str(wd.muster.id),str(wd.muster.musterNo),str(wd.muster.workCode),str(wd.muster.workName),str(wd.muster.dateFrom),str(wd.muster.dateTo),str(wd.muster.paymentDate)] 
    dpArray=[""]*4
    if wd.dpTransaction is not None:
      dpArray=[str(wd.dpTransaction.id),str(wd.dpTransaction.delayDays),str(wd.dpTransaction.delayCompensation),str(wd.dpTransaction.status)]
    attemptCount=0
    for wt in wtArray:
      if wd.muster is None:
        attemptCount=''
      wagelistArray=["","Current",str(attemptCount),"",""]
      ftoArray=["","","","",""]
      rpArray=[""]*5
      if wt is not None:
        attemptCount=attemptCount+1
        txnStatus="Archive"
        if attemptCount == len(wtArray):
          txnStatus="Current"
        wagelistArray=[str(wt.wagelist.id),txnStatus,str(attemptCount),wt.wagelist.wagelistNo,str(wt.wagelist.generateDate)]
        if wt.fto is not None:
          ftoArray=[str(wt.fto.id),wt.fto.ftoNo,str(wt.fto.secondSignatoryDate),wt.fto.paymentMode,wt.fto.ftofinyear]
        if wt.rejectedPayment is not None:
          rpArray=[str(wt.rejectedPayment.id),str(wt.rejectedPayment.rejectionDate),wt.rejectedPayment.referenceNo,wt.rejectedPayment.rejectionReason,wt.rejectedPayment.status]
      a=locationArray+workerArray+jobcardArray+wdArray+musterArray+wagelistArray+ftoArray+rpArray+dpArray+computedArray
      table.append(a)
      w.writerow(a)
  f.seek(0)
  if reportObjs is not None:
    outcsv=f.getvalue()
    contentType="text/csv"
    filepath="misc/%s.csv" % filename
    s3url=uploadReportAmazon(filepath,outcsv,contentType)
    logger.info(s3url)
  else:
    reportType="detailWorkPayment"
    outcsv=f.getvalue()
    filename=pobj.panchayatSlug+"_"+finyear+"_wpDetailed.csv"
    filepath=pobj.panchayatFilepath.replace("filename",filename)
    contentType="text/csv"
    savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)
    #Saving Reports to data folder for athena Analysis
    filepath=pobj.dataFilepath.replace("foldername","detailReport").replace("filename",filename)
    s3url=uploadReportAmazon(filepath,outcsv,contentType)
        
    outhtml=''
    outhtml+=array2HTMLTable(table)
    title="Detail Work Payment Report state:%s District:%s block:%s panchayat: %s finyear %s " % (pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,getFullFinYear(finyear))
    outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
    try:
      outhtml=outhtml.encode("UTF-8")
    except:
      outhtml=outhtml
    reportType="detailWorkPaymentHTML"
    filename=pobj.panchayatSlug+"_"+finyear+"_wpDetailed.html"
    filepath=pobj.panchayatFilepath.replace("filename",filename)
    contentType="text/html"
    savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath,contentType=contentType)


def createJobcardStatReport(logger,pobj,finyear):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',')
  #w = csv.writer(f, newline='',delimiter=',')
  reportType="jobcardStat"
  logger.debug("Createing jobcard Stat Payment report for panchayat: %s panchayatCode: %s ID: %s" % (pobj.panchayatName,pobj.panchayatCode,str(pobj.panchayatID)))
  a=[]
#  workRecords=WorkDetail.objects.filter(id=16082209)
  a.extend(["jobcard","village","caste","name","nicDaysDemanded","nicDaysProvided","jobcardDaysDemanded","jobcardDaysWorked","musterDaysProvided","musterDaysWorked"])
  w.writerow(a)
  stats=JobcardStat.objects.filter(finyear=finyear,jobcard__panchayat=pobj.panchayat)
  for js in stats:
    try:
      villageName=js.jobcard.village.name
    except:
      villageName=""
    a=[]
    a.extend([js.jobcard.jobcard,villageName,js.jobcard.caste,js.jobcard.headOfHousehold,str(js.nicDaysDemanded),str(js.nicDaysProvided),str(js.jobcardDaysDemanded),str(js.jobcardDaysWorked),str(js.musterDaysProvided),str(js.musterDaysWorked)])
    w.writerow(a)
  f.seek(0)
#  with open("/tmp/a.csv","wb") as f1:
#    shutil.copyfileobj(f, f1)
  outcsv=f.getvalue()
  filename=pobj.panchayatSlug+"_"+finyear+"_jsStat.csv"
  filepath=pobj.panchayatFilepath.replace("filename",filename)
  contentType="text/csv"
  savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)

def computePercentage(num,den):
  if den==0:
    return 100
  elif (den is None) or (num is None):
    return None
  else:
    return int(num*100/den)

def doubleArray2CSV(pobj,csvArray,foldername,filename):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',',
                lineterminator='\r\n',
                quotechar = '"'
                )
  for row in csvArray:
    w.writerow(row)
  outcsv=f.getvalue()
  filepath=pobj.dataFilepath.replace("foldername",foldername).replace("filename",filename)
  contentType="text/csv"
  s3url=uploadReportAmazon(filepath,outcsv,contentType)


def dumpDataCSV(logger,pobj,finyear=None,modelName=None):
  if modelName == "Worker":
    csvArray=[]
    locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
    jobcardArrayLabel=["jobcard","headOfHousehold","issue Date","caste","jcNo"]
    workerArrayLabel=["name","age","gender","fatherHusbandName","isDeleted","isMinority","isDisabled"]
    a=locationArrayLabel+jobcardArrayLabel+workerArrayLabel
    csvArray.append(a)
    workers=Worker.objects.filter(jobcard__panchayat=pobj.panchayat)
    for eachWorker in workers:
      locationArray=[pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,eachWorker.jobcard.village.name,pobj.stateCode,pobj.districtCode,pobj.blockCode,pobj.panchayatCode]
      jobcardArray=[eachWorker.jobcard.jobcard,eachWorker.jobcard.headOfHousehold,str(eachWorker.jobcard.applicationDate),eachWorker.jobcard.caste,str(getjcNumber(eachWorker.jobcard.jobcard))]
      workerArray=[eachWorker.name,str(eachWorker.age),eachWorker.gender,eachWorker.fatherHusbandName,str(eachWorker.isDeleted),str(eachWorker.isMinority),str(eachWorker.isDisabled)]
      a=locationArray+jobcardArray+workerArray
      csvArray.append(a)
    doubleArray2CSV(pobj,csvArray,"workers","workers_%s.csv" % (pobj.panchayatSlug))

  if modelName == "WagelistTransaction":
    csvArray=[]
    labels=['stateName','districtName','blockName','panchayatName','stateCode','districtCode','blockCode','panchayatCode','wagelist','jobcard','name','ftoNo','daysWorked','totalWage']
    csvArray.append(labels)
    wts=WagelistTransaction.objects.filter(worker__jobcard__panchayat=pobj.panchayat,wagelist__finyear=finyear)
    for wt in wts:
      if wt.fto is not None:
        ftoNo=wt.fto.ftoNo
      else:
        ftoNo=''
      a=[pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.stateCode,pobj.districtCode,pobj.blockCode,pobj.panchayatCode,wt.wagelist.wagelistNo,wt.worker.jobcard,wt.worker.name,ftoNo,wt.daysWorked,wt.totalWage]
      csvArray.append(a)
    doubleArray2CSV(pobj,csvArray,"wagelistTransactions","%s_%s_%s.csv" % (pobj.panchayatSlug,modelName,finyear))
  if modelName == "rejectedTransaction":
    csvArray=[]
    a=["primaryAccountHolder","fto","wagelist","bankCode","ifscCode","amount","rejectionReason","rejectionDate"]
    csvArray.append(a)
    rps=RejectedPayment.objects.filter(block=pobj.block,finyear=finyear)
    for rp in rps:
      a=[rp.primaryAccountHolder,rp.ftoString,rp.wagelistString,rp.bankCode,rp.ifscCode,str(rp.amount),rp.rejectionReason,str(rp.rejectionDate)]
      csvArray.append(a)
    doubleArray2CSV(pobj,csvArray,"rejectedTransactions","%s_%s_%s.csv" % (pobj.blockSlug,modelName,finyear))

  if modelName == "MusterTransaction":
    csvArray=[]
    locationLabel=['stateName','districtName','blockName','panchayatName','stateCode','districtCode','blockCode','panchayatCode']
    workDetailArrayLabel=["workDetailID","DemandDate","daysDemanded","daysAllocated","demandExists","daysWorked","dayWage","totalWage","accountNo","bankName","branchName","branchCode","creditedDate"]
    musterArrayLabel=["musterID","musterNo","workCode","workname","dateFrom","dateTo","paymentDate"]
    workerArrayLabel=["name"]
    jobcardArrayLabel=["jobcard"]
    labels=locationLabel+jobcardArrayLabel+workerArrayLabel+musterArrayLabel+workDetailArrayLabel
    csvArray.append(labels)
    wps=WorkPayment.objects.filter(finyear=finyear,worker__jobcard__panchayat=pobj.panchayat)
    for wd in wps:
      locationArray=[pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.stateCode,pobj.districtCode,pobj.blockCode,pobj.panchayatCode]
      workerArray=[""]*1
      jobcardArray=[""]*1
      if wd.worker is not None:
        workerArray=[wd.worker.name]
        jobcardArray=[wd.worker.jobcard.jobcard]
      musterArray=[""]*7
      if wd.muster is not None:
        musterArray=[str(wd.muster.id),str(wd.muster.musterNo),str(wd.muster.workCode),str(wd.muster.workName),str(wd.muster.dateFrom),str(wd.muster.dateTo),str(wd.muster.paymentDate)] 
      wdArray=[str(wd.id),str(wd.workDemandDate),str(wd.daysDemanded),str(wd.daysAllocated),str(wd.demandExists),str(wd.daysWorked),str(wd.dayWage),str(wd.totalWage),wd.accountNo,wd.bankName,wd.branchName,wd.branchCode,str(wd.creditedDate)]
      a=locationArray+jobcardArray+workerArray+musterArray+wdArray
      csvArray.append(a)
    doubleArray2CSV(pobj,csvArray,"musterTransactions","%s_%s_%s.csv" % (pobj.panchayatSlug,modelName,finyear))
       
  if modelName == "FTOTransaction":
    csvArray=[]
    fts=PaymentTransaction.objects.filter(jobcard__panchayat=pobj.panchayat,fto__finyear=finyear)
    labels=['stateName','districtName','blockName','panchayatName','stateCode','districtCode','blockCode','panchayatCode','fto','wagelist','jobcard','secondSignatoryDate','status','creditedAmount','referenceNo','transactionDate','processDate','rejectionReason','primaryAccountHolder','favorBankAPB','IINBankAPB']
    csvArray.append(labels)
    for ft in fts:
      if ft.wagelist is not None:
        wagelistNo=ft.wagelist.wagelistNo
      else:
        wagelistNo=''
      a=[pobj.stateName,pobj.districtName,pobj.blockName,ft.jobcard.panchayat.name,pobj.stateCode,pobj.districtCode,pobj.blockCode,ft.jobcard.panchayat.code,ft.fto.ftoNo,wagelistNo,ft.jobcard.jobcard,str(ft.fto.secondSignatoryDate),ft.status,ft.creditedAmount,ft.referenceNo,str(ft.transactionDate),str(ft.processDate),ft.rejectionReason,ft.primaryAccountHolder,ft.favorBankAPB,ft.IINBankAPB]
      csvArray.append(a)
    doubleArray2CSV(pobj,csvArray,"ftoTransactions","%s_%s_%s.csv" % (pobj.panchayatSlug,modelName,finyear))

def findMissingFTO(logger,pobj,obj):
  fts=PaymentTransaction.objects.filter(wagelist=obj.wagelist,jobcard=obj.worker.jobcard)
  wts=WagelistTransaction.objects.filter(wagelist=obj.wagelist,worker__jobcard=obj.worker.jobcard)
  logger.info("Length of fts is %s " % str(len(fts)))
  if len(wts) == len(fts):
    myFTO=fts.first().fto
    logger.info(myFTO)

def telanganaMusterProcess(logger,pobj,finyear):
  dateFormat="%d-%b-%Y"
  reportType="telanganaMusters"
  myReport=Report.objects.filter(reportType=reportType,panchayat=pobj.panchayat,finyear=finyear).first()
  if myReport is not None:
    url=myReport.reportURL
    logger.info(url)
    with closing(requests.get(url, stream=True)) as r:
      reader = oldcsv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'))
      for row in reader:
        if len(row) == 18:
          jobcard=row[0]
          surname=row[1]
          caste=row[3].lstrip().rstrip() 
          payorderDate=getDateObj(row[11],dateFormat=dateFormat)
          dateTo=getDateObj(row[10],dateFormat=dateFormat)
          dateFrom=getDateObj(row[9],dateFormat=dateFormat)
          if ((len(jobcard) == 18) and (surname.lower() != 'total')):
         # if (((jobcard) == '142000602004010081') and (surname.lower() != 'total')):
            name=row[2]
            payorderAmount=row[14]
           #logger.info(jobcard)
           #logger.info(surname)
           #logger.info(row[11])
           #logger.info(name) 
            name=name.lstrip().rstrip()
           # logger.info(payorderDate)
           # logger.info(payorderAmount)
            myJobcard=Jobcard.objects.filter(tjobcard=jobcard).first()
            error=None
            wps=[]
            if myJobcard is  None:
              error=f"Jobcard not found { jobcard } \n"
              sampleJobcard=Jobcard.objects.filter(panchayat=pobj.panchayat).first()
              jobcardPrefix=sampleJobcard.jobcard.split("/")[0]
              jcNo=jobcard[-6:]
              constructedJobcard="%s/%s" % (jobcardPrefix,jcNo)
              logger.info("tjobcard %s, Jobcard %s " % (jobcard,constructedJobcard))
              myJobcard=Jobcard.objects.create(jobcard=jobcard,panchayat=pobj.panchayat,jcNo=jcNo,tjobcard=jobcard,caste=caste)
              telanganaJobcardDownload(logger,pobj,myJobcard)
              telanganaJobcardProcess(logger,pobj,myJobcard)
            if myJobcard is not None:
              error=None
              wp=None
              #logger.info(f"my Jobcard id { myJobcard.id }")
              wps=APWorkPayment.objects.filter(jobcard__tjobcard=jobcard,dateFrom=dateFrom,payorderDate=payorderDate,name__icontains=name,payorderAmount=payorderAmount)
              if len(wps) == 1:
                wp=wps.first()
              else:
                wps=APWorkPayment.objects.filter(jobcard__tjobcard=jobcard,dateFrom__isnull=True,payorderDate=payorderDate,name__icontains=name,payorderAmount=payorderAmount)
                if len(wps) >= 1:
                  wp=wps.first()
              if wp is None:
                error="Length of WP is %s Jobcard ID %s\n" % (str(len(wps)),str(myJobcard.id))
              else:
                wp.workCode=row[6].lstrip().rstrip()
                wp.workName=row[8].lstrip().rstrip()
                wp.dateFrom=dateFrom
                wp.dateTo=dateTo
                wp.isMusterRecordPresent=True
                finyear=str(getFinYear(dateObj=dateFrom))
                wp.finyear=finyear
                wp.save()
            if error is not None:
              logger.info(row)
              logger.info(error)
              msg=f'Panchayat Code { pobj.panchayatCode } and finyear { finyear }\n'
              pobj.cobj.remarks+=msg
              pobj.cobj.remarks+=error
              pobj.cobj.remarks+=str(row)
              pobj.cobj.remarks+="---------------------------\n"
 #error=None
 #reportType="telanganaMusters"
 #error1,mycsv=getReportHTML(logger,pobj,reportType,finyear)
 #if error1 is None:
 #  reader = oldcsv.reader(mycsv, delimiter=',') 
 #  logger.info(reader)
 #  for row in reader:
 #    logger.info(row)
def createDetailWorkPaymentReportAP(logger,pobj,finyear):
  finyear=str(finyear)
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',')
  reportType="workPaymentAP"
  a=[]
  locationArrayLabel=["state","district","block","panchayat","village","stateCode"]
  jobcardArrayLabel=["jobcardID","jobcard","tjobcard","jcNo","caste","headOfHousehold","tags"]
  wdLabel=["applicantName","finyear","workCode","workName","musterNo","dateFrom","dateTo","daysWorked","accountNo","payorderNo","payorderDate","epayorderno","epayorderDate","payingAgencyDate","creditedDate","disbursedDate","paymentMode","payOrdeAmount","disbursedAmount","isMusterReportFound"]
  a=locationArrayLabel+jobcardArrayLabel+wdLabel
  w.writerow(a)
  workRecords=APWorkPayment.objects.filter(jobcard__panchayat=pobj.panchayat,finyear=finyear).order_by("jobcard__tjobcard","epayorderDate")
  logger.info("Total Work Records: %s " %str(len(workRecords)))
  for wd in workRecords:
    jobcardArray=[""]*6
    locationArray=[""]*6
    if wd.jobcard is not None:
      libtechTag=''
      tagArray=wd.jobcard.libtechTag.all()
      for ltTag in tagArray:
        libtechTag+=ltTag.name
        libtechTag+="," 
      tjobcard1="~%s" % (wd.jobcard.tjobcard)
      jobcardArray=[str(wd.jobcard.id),wd.jobcard.jobcard,tjobcard1,wd.jobcard.jcNo,wd.jobcard.caste,wd.jobcard.headOfHousehold,libtechTag]
      if wd.jobcard.village is not None:
        villageName=wd.jobcard.village.name
      else:
        villageName=''
      locationArray=[wd.jobcard.panchayat.block.district.state.name,wd.jobcard.panchayat.block.district.name,wd.jobcard.panchayat.block.name,wd.jobcard.panchayat.name,villageName,wd.jobcard.panchayat.block.district.state.stateShortCode]
    wdArray=[wd.name,wd.finyear,wd.workCode,wd.workName,wd.musterNo,str(wd.dateFrom),str(wd.dateTo),str(wd.daysWorked),wd.accountNo,wd.payorderNo,str(wd.payorderDate),wd.epayorderNo,str(wd.epayorderDate),str(wd.payingAgencyDate),str(wd.creditedDate),str(wd.disbursedDate),wd.modeOfPayment,str(wd.payorderAmount),str(wd.disbursedAmount),str(wd.isMusterRecordPresent),str(wd.id)]
    a=locationArray+jobcardArray+wdArray
    w.writerow(a)
  f.seek(0)
  outcsv=f.getvalue()
  filename=pobj.panchayatSlug+"_"+str(finyear)+"_wpAP.csv"
  filepath=pobj.panchayatFilepath.replace("filename",filename)
  contentType="text/csv"
  savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)


def telanganaMusterDownload(logger, pobj,finyear):
  reportType="telanganaMusters"
  error=None
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=7)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,reportThreshold=reportThreshold)
  if isUpdated == True:
    return None
  else:
    startYear=int(finyear)-1+2000
    endYear=int(finyear)+2000
    startDate="01/04/%s" % str(startYear)
    endDate="31/03/%s" % str(endYear)
    logger.info(startDate+endDate)
    stateCode='02'
    districtCode=pobj.districtCode[2:]
    blockCode=pobj.blockCode[5:]
    panchayatCode=pobj.panchayatCode[8:]
    logger.info(districtCode+blockCode+panchayatCode)
    url = 'http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=NewReportsRH&actionVal=R1Display&page=Newreportcenter_ajax_eng#'
    url='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?requestType=Common_engRH&actionVal=musterinfo&page=MusterRolls_eng'
    url2='http://www.nrega.telangana.gov.in/Nregs/FrontServlet?'
    logger.info('Fetching URL[%s] for cookies' % url)
    with requests.Session() as session:
        response = session.get(url)

        cookies = session.cookies

        logger.info(cookies)
        '''
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/x-www-form-urlencoded',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        params = (
            ('requestType', 'Common_engRH'),
            ('actionVal', 'musterinfo'),
            ('page', 'MusterRolls_eng'),
        )

        data = {
            'State': '02',
            'District': '14',
            'Mandal': '06',
            'Panchayat': '12',
            'FromDate': '01/12/2018',
            'ToDate': '31/12/2018',
            'Go': '',
            'spl': 'Select',
            'input2': '',
            'userCaptcha': ''
        }

        response = session.post(url2,
                                headers=headers, params=params, cookies=cookies, data=data)
        '''
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/x-www-form-urlencoded',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        params = (
            ('requestType', 'PaymentsWork_engRH'),
            ('actionVal', 'musterrolls'),
            ('page', 'SocialAuditPrint_eng'),
            ('District', districtCode),
            ('Mandal', blockCode),
            ('Panchayat', panchayatCode),
            ('FromDate', startDate),
            ('ToDate', endDate),
            ('exec', 'muster'),
        )

        data = {
            'State': stateCode,
            'District': districtCode,
            'Mandal': blockCode,
            'Panchayat': panchayatCode,
            'FromDate': startDate,
            'ToDate': endDate,
            'Go': '',
            'spl': 'Select',
            'input2': '',
            'userCaptcha': ''
        }

        response = session.post(url2,
                                headers=headers, params=params, cookies=cookies, data=data)
        if response.status_code == 200:
          filename = '/tmp/muster_%s_%s.xlsx' %(pobj.panchayatCode,finyear)
          csvfilename = '/tmp/muster_%s_%s.csv'% (pobj.panchayatCode,finyear)
          logger.info(csvfilename)
          with open(filename, 'wb') as html_file:
              logger.info('Writing [%s]' % filename)
              html_file.write(response.content)
          csv_from_excel(filename,csvfilename)
          with open(csvfilename, 'r') as file:
            csvdata = file.read()
          if ( ("***No Data found for the selection" in csvdata) and (finyear != str(getCurrentFinYear()) )):
            error="Unable to download Musters for panchayat %s finyear %s" % (pobj.panchayatSlug,finyear)
          else:
            filename=pobj.panchayatSlug+"_"+finyear+"_telanganaMusters.csv"
            filepath=pobj.panchayatFilepath.replace("filename",filename)
            contentType="text/csv"
            savePanchayatReport(logger,pobj,finyear,reportType,csvdata,filepath,contentType=contentType)
        else:
          error="Unable to download Musters for panchayat %s finyear %s" % (pobj.panchayatSlug,finyear)
    if error is None:
      error=telanganaMusterProcess(logger,pobj,finyear)
    return error

def telanganaJobcardProcess(logger,pobj,obj):
  myhtml=None 
  dateFormat="%d-%b-%Y"
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")


    workerTable=htmlsoup.find('table',id="workerTable")
    allApplicantFound=True
    if  "Relationship" in str(workerTable):
      #logger.debug("Found the Worker Table")
      rows=workerTable.findAll('tr')
      for row in rows:
        cols=row.findAll('td')
        if len(cols)>0:
          applicantNo=cols[1].text.lstrip().rstrip()
          if applicantNo.isdigit():
            applicantNo=int(applicantNo)
          else:
            applicantNo=0
          name=cols[2].text.lstrip().rstrip()
          gender=cols[4].text.lstrip().rstrip()
          age=cols[3].text.lstrip().rstrip()
          relationship=cols[5].text.lstrip().rstrip()
          try:
            myWorker=Worker.objects.create(jobcard=obj,name=name,applicantNo=applicantNo)
          except:
            myWorker=Worker.objects.filter(jobcard=obj,name=name,applicantNo=applicantNo).first()
          myWorker.gender=gender
          myWorker.age=age
          myWorker.relationship=relationship
          myWorker.save()

    paymentTable=htmlsoup.find('table',id='paymentTable')
    rows=paymentTable.findAll('tr')
    for row in rows:
      cols=row.findAll('td')
      if len(cols) > 0:
        workCode=None
        workName=None
        payOrderNo=None
        payOrderDate=None
        epayOrderNo=None
        epayOrderDate=None
        creditedDate=None
        disbursedDate=None
        epayorderNo=cols[0].text.lstrip().rstrip()
       # logger.info(epayorderNo)
        if epayorderNo.isdigit():
          ftrNo=cols[1].text.lstrip().rstrip()
          musterNo=cols[2].text.lstrip().rstrip()
          musterOpenDateString=cols[3].text.lstrip().rstrip()
          musterClosureDateString=cols[4].text.lstrip().rstrip()
          payOrderDate=getDateObj(cols[5].text.lstrip().rstrip(),dateFormat=dateFormat)
          workCodeworkName=cols[6].text.lstrip().rstrip()
          payOrderNo=cols[7].text.lstrip().rstrip()
          applicantName=cols[8].text.lstrip().rstrip()
          daysWorked=cols[10].text.lstrip().rstrip()
          payorderAmount=cols[11].text.lstrip().rstrip()
          creditedDateString=cols[12].text.lstrip().rstrip()
          disbursedAmount=cols[13].text.lstrip().rstrip()
          disbursedDateString=cols[14].text.lstrip().rstrip()[:11]

          if "/" in workCodeworkName:
            workArray=workCodeworkName.split("/")
            workCode=workArray[0]
            workName=workArray[1]
          else:
            workName=workCodeworkName
            workCode=None

          dateTo=getDateObj(musterClosureDateString,dateFormat=dateFormat)
          dateFrom=getDateObj(musterOpenDateString,dateFormat=dateFormat)
          creditedDate=getDateObj(creditedDateString,dateFormat=dateFormat)
          disbursedDate=getDateObj(disbursedDateString,dateFormat=dateFormat)
          pr=APWorkPayment.objects.filter(jobcard=obj,epayorderNo=epayorderNo).first()
          if pr is None: 
            pr=APWorkPayment.objects.create(jobcard=obj,epayorderNo=epayorderNo)
          pr.name=applicantName
          pr.musterNo=musterNo
          pr.workCode=workCode
          pr.workName=workName
          pr.dateTo=dateTo
          pr.dateFrom=dateFrom
          pr.payorderDate=payOrderDate
          if daysWorked.isdigit():
            pr.daysWorked=daysWorked
          #logger.info(payorderAmount)
          if payorderAmount.isdigit():
            pr.payorderAmount=payorderAmount
          if disbursedAmount.isdigit():
            pr.disbursedAmount=disbursedAmount
          pr.payorderNo=payOrderNo
          pr.creditedDate=creditedDate
          pr.disbursedDate=disbursedDate
         #if (creditedDate is not None) and (dateTo is not None):
         #  diffDays=(creditedDate-dateTo).days
         #  if diffDays > 30:
         #    isDelayedPayment = True
         #  else:
         #    isDelayedPayment = False
         #elif (creditedDate is None) and (dateTo is not None):
         #  diffDays=(datetime.datetime.today().date() - dateTo).days
         #  if diffDays > 30:
         #    isDelayedPayment = True
         #  else:
         #    isDelayedPayment = False
         #else:
         #    isDelayedPayment=False
         #pr.isDelayedPayment=isDelayedPayment
          if payOrderDate is not None:
            datetimeObject=payOrderDate#datetime.datetime.strptime(epayOrderDate, '%Y-%m-%d')
            if datetimeObject.month <= 3:
              finyear=str(datetimeObject.year)[2:]
            else:
              finyear=str(datetimeObject.year+1)[2:]
            pr.ftofinyear=finyear
          #logger.debug("The PR id is %s " % str(pr.id))
          pr.save()


libtechMethodNames={
      'downloadJobcardtelangana'   : telanganaJobcardDownload,
      'downloadJobcard'            : downloadJobcard,
      'processJobcard'             : processJobcard,
      'downloadMuster'             : downloadMusterNew,
       'downloadWagelist'           : downloadWagelist,
      'downloadRejectedPayment'    : downloadRejectedPayment,
       'downloadFTO'                : downloadFTO,
     'processJobcardtelangana'    : telanganaJobcardProcess,
      'processMuster'              : processMuster,
       'processWagelist'            : processWagelist,
      'processRejectedPayment'     : processRejectedPayment,
       'processFTO'                 : processFTO,
#
        }
