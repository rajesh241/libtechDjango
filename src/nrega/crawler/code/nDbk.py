import requests
import traceback
import logging
from io import BytesIO
import unicodecsv as csv
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
from nrega.crawler.commons.nregaFunctions import getCurrentFinYear,stripTableAttributes,getCenterAlignedHeading,htmlWrapperLocal,getFullFinYear,correctDateFormat,table2csv,array2HTMLTable,getDateObj,stripTableAttributesPreserveLinks
from nrega.crawler.commons.nregaSettings import statsURL,telanganaStateCode,crawlerTimeThreshold,delayPaymentThreshold,musterStartAttendanceColumn,musterEndAttendanceColumn,crawlerErrorTimeThreshold
from config.defines import djangoSettings,logDir,LIBTECH_AWS_SECRET_ACCESS_KEY,LIBTECH_AWS_ACCESS_KEY_ID,LIBTECH_AWS_BUCKET_NAME,AWS_S3_REGION_NAME,BASEURL,AUTHENDPOINT,apiusername,apipassword
from nrega.crawler.code.commons import savePanchayatReport,uploadReportAmazon,getjcNumber,isReportUpdated
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()
from nrega import models as nregamodels
import collections

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Jobcard,PanchayatCrawlInfo,Worker,PanchayatStat,Village,Wagelist,WorkDetail,WorkDemand,FTO,WagelistTransaction,JobcardStat,DPTransaction,FTOTransaction,APWorkPayment,Report,WorkerStat
from nrega.crawler.code.delayURLs import delayURLs
downloadToleranceDict={
        "Jobcard" : 95,
        "Muster" : 40,
        "Wagelist" : 100,
        "FTO" : 100,
        }
def makehash():
  return collections.defaultdict(makehash)
class PanchayatCrawler:
  def __init__(self, crawlQueueID):
        cq=CrawlQueue.objects.filter(id=crawlQueueID).first()
        self.error=False
        self.crawlQueueID=crawlQueueID
        self.attemptCount=cq.attemptCount
        self.startFinYear=cq.startFinYear
        self.endFinYear=getCurrentFinYear()
        self.downloadStage=cq.downloadStage
        self.downloadStep=cq.downloadStep
        self.panchayatCode=cq.panchayat.code
        self.panchayatName=cq.panchayat.name
        self.panchayatID=cq.panchayat.id
        self.pslug=cq.panchayat.slug
        self.panchayatSlug=cq.panchayat.slug
        self.blockCode=cq.panchayat.block.code
        self.blockID=cq.panchayat.block.id
        self.blockName=cq.panchayat.block.name
        self.blockSlug=cq.panchayat.block.slug
        self.districtCode=cq.panchayat.block.district.code
        self.districtName=cq.panchayat.block.district.name
        self.districtSlug=cq.panchayat.block.district.slug
        self.stateCode=cq.panchayat.block.district.state.code
        self.stateName=cq.panchayat.block.district.state.name
        self.stateSlug=cq.panchayat.block.district.state.slug
        self.isNIC=cq.panchayat.block.district.state.isNIC
        self.crawlIP=cq.panchayat.block.district.state.crawlIP
        self.stateShortCode=cq.panchayat.block.district.state.stateShortCode
        self.jobcardPrefix=self.stateShortCode+"-"
        self.panchayat=cq.panchayat
        self.block=cq.panchayat.block
        self.district=cq.panchayat.block.district
        self.state=cq.panchayat.block.district.state
        self.jobcardDict={}
        self.villageDict={}
        self.workerDict={}
        self.musterDict={}
        self.wagelistDict={}
        self.ftoDict={}
        self.jobcardStatDict={}
        self.codeObjDict={}
        self.missingDict={}
        self.workDetailDict={}
        self.crawlerDict={}
        self.panchayatFilepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",self.stateSlug,self.districtSlug,self.blockSlug,self.panchayatSlug,"DATA","NICREPORTS","filename")
        self.blockFilepath="%s/%s/%s/%s/%s/%s/%s" % ("nrega",self.stateSlug,self.districtSlug,self.panchayatSlug,"DATA","NICREPORTS","filename")
        self.JSONFilepath="%s/%s/%s/%s/%s/%s/%s" % ("nrega","JSON",self.stateSlug,self.districtSlug,self.blockSlug,self.pslug,"filename")
        filename="instanceDict.json"
        self.instanceJSONPath=self.JSONFilepath.replace("filename",filename)
        filename="workDetailDict.json"
        self.workDetailJSONPath=self.JSONFilepath.replace("filename",filename)
        filename="missingDict.json"
        self.missingJSONPath=self.JSONFilepath.replace("filename",filename)
        # Create PanchayatStat objects.
        for finyear in range(int(self.startFinYear),int(self.endFinYear)+1):
          try:
            ps=PanchayatStat.objects.create(panchayat=self.panchayat,finyear=finyear)
          except:
            s="already created"
         

def crawlerMain(logger,cq):
    startTime=datetime.datetime.now()
    pobj=PanchayatCrawler(cq.id)
    if pobj.error == False:
      logger.info("No Error Found")
      try:
        if pobj.stateCode == telanganaStateCode:
          error=crawlFullPanchayatTelangana(logger,pobj)
        else:
          error=crawlFullPanchayat(logger,pobj)
      except:
        error = traceback.format_exc()
      logger.info("Finished CrawlQueue ID %s with error %s " % (pobj.crawlQueueID,error))
      
      cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
      endTime=datetime.datetime.now()
      if error is None:
        attemptCount=0
        crawlDuration=cq.crawlDuration+(endTime-startTime).seconds
      else:
        attemptCount=(pobj.attemptCount)+1
        crawlDuration=cq.crawlDuration
      cq.error=error
      cq.inProgress=False
      cq.attemptCount=attemptCount
      cq.crawlDuration=crawlDuration
      cq.attemptCount=attemptCount
      cq.crawlAttempDate=timezone.now()
      cq.save()


    else:
      logger.info('Multple panchayats found')

def createCodeObjDict(logger,pobj,codeType=None):
  pobj.jobcardStatDict=makehash()
  pobj.CodeObjDict=makehash()
  if codeType == None:
    codeType="instance"

  if codeType=="instance":
    objs=Jobcard.objects.filter(panchayat=pobj.panchayat)
    for obj in objs:
      pobj.codeObjDict[obj.code]=obj
    logger.info("Finished dict of jobcards")

    myWorkers=Worker.objects.filter(jobcard__panchayat=pobj.panchayat)
    for eachWorker in myWorkers:
      pobj.codeObjDict[eachWorker.code]=eachWorker
    logger.info("Finished Dict of Workers")

    myFTOs=FTO.objects.filter(block=pobj.block)
    for eachFTO in myFTOs:
      pobj.codeObjDict[eachFTO.code]=eachFTO
    logger.info("Finished Dict of FTOs")

    myWagelists=Wagelist.objects.filter(block=pobj.block)
    for eachWagelist in myWagelists:
      pobj.codeObjDict[eachWagelist.code]=eachWagelist
    logger.info("finished dict of Wagelits")

    myMusters=Muster.objects.filter(panchayat=pobj.panchayat)
    for eachMuster in myMusters:
      pobj.codeObjDict[eachMuster.code]=eachMuster
    logger.info("Finished Dict of Musters")

  if codeType == "transaction":
    wts=WagelistTransaction.objects.filter(worker__jobcard__panchayat=pobj.panchayat)
    for wt in wts:
      if wt.worker is not None:
        code="%s_%s" % (wt.worker.code,wt.wagelist.code)
        pobj.codeObjDict[code]=wt
    logger.info("Finished Dict of WagelistTransactions")


def getProcessedObjects(logger,pobj,modelName,finyear):
  if modelName == "Jobcard":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (panchayat=pobj.panchayat) )
  elif modelName == "FTO":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,block=pobj.block) )
  else:
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,panchayat=pobj.panchayat) )

  return myobjs

def getDownloadObjects(logger,pobj,modelName,finyear):
  if modelName=="Jobcard":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (panchayat=pobj.panchayat) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  elif modelName == "FTO":
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,isComplete=False,block=pobj.block) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  else:
    myobjs=getattr(nregamodels,modelName).objects.filter( Q (finyear=finyear,isComplete=False,panchayat=pobj.panchayat) & Q (  Q ( Q( isDownloaded=False) | Q(downloadDate__lt = crawlerTimeThreshold)  ) & Q ( Q(isError=False) | Q (errorDate__lt = crawlerErrorTimeThreshold)) ))
  return myobjs

def getObjDownloadStat(logger,pobj,modelName,finyear):
  if modelName == "Jobcard":
      total=len(getattr(nregamodels,modelName).objects.filter(panchayat=pobj.panchayat))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,panchayat=pobj.panchayat))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,panchayat=pobj.panchayat))
  elif modelName == "FTO":
      total=len(getattr(nregamodels,modelName).objects.filter(panchayat=pobj.panchayat,finyear=finyear))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,panchayat=pobj.panchayat,finyear=finyear))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,panchayat=pobj.panchayat,finyear=finyear))
  else:
      total=len(getattr(nregamodels,modelName).objects.filter(panchayat=pobj.panchayat,finyear=finyear))
      pending=len(getattr(nregamodels,modelName).objects.filter(isError=True,panchayat=pobj.panchayat,finyear=finyear))
      downloaded=len(getattr(nregamodels,modelName).objects.filter(isDownloaded=True,panchayat=pobj.panchayat,finyear=finyear))
  return total,pending,downloaded

def objectProcessMain(logger,pobj,modelName):
    error=None
    if modelName == "Jobcard":
      startFinYear=getCurrentFinYear()
    else:
      startFinYear=pobj.startFinYear

    for finyear in range(int(startFinYear),int(pobj.endFinYear)+1):
      finyear=str(finyear)
      myobjs=getProcessedObjects(logger,pobj,modelName,finyear)
      n=len(myobjs)
      logger.info("Number of %s to be processed is %s " % (modelName,str(n)))
      downloadNProcess(logger,pobj,myobjs,modelName=modelName,method='process')
    # while n > 0:
    #   downloadNProcess(logger,pobj,myobjs,modelName=modelName,method='process')
    #   myobjs=getProcessedObjects(logger,pobj,modelName,finyear)
    #   n=len(myobjs)
    #   logger.info("Number of %s to be processed is %s " % (modelName,str(n)))
    return error

def objectDownloadMain(logger,pobj,modelName):
    error=None
    total=0
    pending=0
    downloaded=0
    if modelName == "Jobcard":
      startFinYear=getCurrentFinYear()
    else:
      startFinYear=pobj.startFinYear

    for finyear in range(int(startFinYear),int(pobj.endFinYear)+1):
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
    return total,pending,downloaded

def computeJobcardStat(logger,pobj):
    error=None
    wds=WorkDetail.objects.filter(worker__jobcard__panchayat=pobj.panchayat).values("muster__finyear","worker__jobcard__jobcard").annotate(pcount=Sum('daysProvided'),wcount=Sum('daysWorked'))
    for wd in wds:
      jobcard=wd['worker__jobcard__jobcard']
      finyear=wd['muster__finyear']
      daysProvided=wd['pcount']
      daysWorked=wd['wcount']
      js=JobcardStat.objects.filter(finyear=finyear,jobcard__jobcard=jobcard).first()
      if js is None:
        jobcardObj=pobj.codeObjDict['jobcard']
        if jobcardObj is not None:
          js=JobcardStat.objects.create(finyear=finyear,jobcard=jobcardObj)
      if js is not None:
        js.daysProvided=daysProvided
        js.daysWorked=daysWorked
        js.save()
      else:
        error="Jobcard %s not found while doing stat update " % (jobcard)
   
   objs=JobcardStat.objects.filter(jobcard__panchayat=pobj.panchayat).values("finyear").annotate(dcount=Sum('daysProvided'),wcount=Sum('daysWorked'))
   for obj in objs:
     finyear=obj['finyear']
     dcount=obj['dcount']
     wcount=obj['wcount']
     ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
     ps.libtechWorkDaysProvided=dcount
     ps.libtechWorkDays=wcount
     ps.save()
    return error

def jsonconverter(o):
  if isinstance(o, datetime.datetime):
    return o.__str__()

 
def downloadPanchayatStat(logger,pobj):
  reportType="nicStatsHTML"
  finyear=getCurrentFinYear()
  panchayatCode=pobj.panchayatCode
  panchayatName=pobj.panchayatName
  panchayatID=pobj.panchayatID
  pslug=pobj.pslug
  blockCode=pobj.blockCode
  blockName=pobj.blockName
  blockSlug=pobj.blockSlug
  districtCode=pobj.districtCode
  districtName=pobj.districtName
  districtSlug=pobj.districtSlug
  stateCode=pobj.stateCode
  stateName=pobj.stateName
  stateSlug=pobj.stateSlug
  statusURL="%s?panchayat_code=%s&panchayat_name=%s&block_code=%s&block_name=%s&district_code=%s&district_name=%s&state_code=%s&state_name=%s&page=p&fin_year=2014-2015" % (statsURL,panchayatCode,panchayatName,blockCode,blockName,districtCode,districtName,stateCode,stateName)
  logger.info(statusURL) 
  try:
    r  = requests.get(statusURL)
    error=None
  except requests.exceptions.RequestException as e:  # This is the correct syntax
    logger.info(e) 
    error=1
  if error is None:
    myhtml=r.content
    error,statsTable=validateStatsHTML(myhtml)
    if error is None:
      logger.info("Successfully Downloaded")
      outhtml=''
      outhtml+=stripTableAttributes(statsTable,"statsTable")
      title="NIC Panchayat Statistics state:%s District:%s block:%s panchayat: %s  " % (stateName,districtName,blockName,panchayatName)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="nicStats_%s_%s_%s.html" % (pslug,panchayatCode,finyear)
      filepath=pobj.panchayatFilepath.replace("filename",filename)
      savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath)
      htmlsoup=BeautifulSoup(myhtml,"html.parser")
      rows=htmlsoup.findAll("tr")
      nicTotalApplicants=None
      nicTotalJobcards=None
      for row in rows:
        if "Total No. of Workers" in str(row):
          cols=row.findAll("td")
          wArray=[]
          for col in cols:
            wArray.append(col.text.lstrip().rstrip().replace(",",""))
          logger.debug(str(wArray))  
          nicTotalApplicants=wArray[1]
        
        if "Total No. of JobCards issued" in str(row):
          cols=row.findAll("td")
          wArray=[]
          for col in cols:
            wArray.append(col.text.lstrip().rstrip().replace(",",""))
          logger.debug(str(wArray))  
          nicTotalJobcards=wArray[1]
          
        if "II             Progress" in str(row):
          statFound=1
          logger.debug(str(row)) 
          cols=row.findAll("td")
          fArray=[]
          for col in cols:
            fArray.append(col.text.lstrip().rstrip()[-2:])
          logger.debug(str(fArray))  
        if "Persondays Generated so far" in str(row):
          logger.debug(str(row)) 
          cols=row.findAll("td")
          fArrayData=[]
          for col in cols:
            fArrayData.append(col.text.lstrip().rstrip().replace(",",""))
          logger.debug(str(fArrayData))
        if ("Wages(Rs. In Lakhs)" in str(row)) and ("Material" not in str(row)):
          logger.debug(str(row)) 
          cols=row.findAll("td")
          wagesArrayData=[]
          for col in cols:
            wagesArrayData.append(col.text.lstrip().rstrip().replace(",",""))
          logger.debug(str(wagesArrayData))
  
    if statFound == 1:
      for i,finyear in enumerate(fArray):
        if i != 0:
          nicWorkDays=fArrayData[i]
          try:
            nicTotalWages=int(float(wagesArrayData[i])*1000*100)
          except:
            nicTotalWages=0
          code="%s_%s" % (panchayatCode,finyear)
          mps=PanchayatStat.objects.filter(code=code).first()
          if mps is None:
            mps=PanchayatStat.objects.create(panchayat=pobj.panchayat,finyear=finyear)
          mps.nicWorkDays=nicWorkDays
          mps.nicTotalWages=nicTotalWages
          mps.nicJobcardsTotal=nicTotalJobcards
          mps.nicWorkersTotal=nicTotalApplicants
          mps.save()
  return error

def validateStatsHTML(myhtml):
  error=None
  statsTable=None
  htmlsoup=BeautifulSoup(myhtml,"html.parser")
  tables=htmlsoup.findAll('table')
  for table in tables:
    if "Persondays Generated so far" in str(table):
      statsTable=table
  if statsTable is None:
    error="Stats Table not found"
  return error,statsTable

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


def validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear=None,locationType=None,jobcardPrefix=None):
    if locationType==None:
      locationType='panchayat'

    if finyear is None:
      finyearString=''
    else:
      finyearString=finyear

    if locationType == 'panchayat':
      locationName="%s-%s-%s-%s" % (pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName)
      filename="%s_%s_%s_%s.html" % (reportType,pobj.panchayatSlug,pobj.panchayatCode,finyearString)
      filepath=pobj.panchayatFilepath.replace("filename",filename)
    else:
      locationName="%s-%s-%s" % (pobj.stateName,pobj.districtName,pobj.blockName)
      filename="%s_%s_%s_%s.html" % (reportType,pobj.blockSlug,pobj.blockCode,finyearString)
      filepath=pobj.blockFilepath.replace("filename",filename)

    outhtml=''
    outhtml+=getCenterAlignedHeading(locationName)
    if finyear is not None:
      outhtml+=getCenterAlignedHeading("Financial Year: %s " % (getFullFinYear(finyear)))
      
    error,myTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=jobcardPrefix)
    if error is None:
      outhtml+=stripTableAttributes(myTable,"myTable")

      outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      
      savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath)
    return error 
def downloadJobcardStat(logger,pobj):
  error=None
  reportType="nicJobcardStat"
  reportName="NIC Jobcard Stat"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  reportThreshold = datetime.datetime.now() - datetime.timedelta(hours=3)
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps.zeroMusters == False:
      isUpdated=isReportUpdated(logger,pobj,finyear,reportType,reportThreshold=reportThreshold)
      if isUpdated==False:
        if pobj.crawlIP == "nregasp2.nic.in":
          myhtml=getJobcardStat1(logger,pobj,finyear)
        else:
          myhtml=getJobcardStat(logger,pobj,finyear)
        with open("/tmp/%s.html" % (pobj.panchayatSlug),"wb") as f:
          f.write(myhtml)
        error1="Unable to download Jobcard Stat"
        if myhtml is not None:
          error1=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear,locationType=None)
        if error1 is not None:
          if "No Data Found" in str(myhtml):
            error=None
          else:
            error=error1
  return error

def getReportHTML(logger,pobj,reportType,finyear,locationType=None):
  myhtml=None
  if locationType == 'block':
    myReport=Report.objects.filter(block=pobj.block,finyear=finyear,reportType=reportType).first()
  else:
    myReport=Report.objects.filter(panchayat=pobj.panchayat,finyear=finyear,reportType=reportType).first()
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
        error="Count not download %s " % str(reportURL)
    else:
      error="%s Report URL not found %s" % (str(myReport))
  return error,myhtml
def processJobcardStat(logger,pobj):
  error=None
  remarks=''
  reportType="nicJobcardStat"
  reportName="NIC Jobcard Stat"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
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
    try:
      ps=PanchayatStat.objects.create(panchayat=pobj.panchayat,finyear=finyear)
    except:
      ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    ps.nicTotalDaysProvided=totalWorkDays
    ps.save()
  cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
  if cq.remarks is None:
    cq.remarks=remarks
  else:
    cq.remarks=cq.remarks+remarks
  cq.save()
             
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
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
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
          with open("/tmp/c.html" ,"wb") as f:
            f.write(stathtml)
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

def jobcardRegister(logger,pobj):
  reportType="applicationRegister"
  reportName="Application Register"
  finyear=getCurrentFinYear()
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType)
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

def crawlMusters(logger,pobj):
  codeObjDict=pobj.codeObjDict
  musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  musterDict={}
  for finyear in range(int(startFinYear),int(endFinYear)+1):
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
    try:
      r  = requests.get(url)
      error=0
    #except requests.exceptions.RequestException as e:  # This is the correct syntax
    except:  # This is the correct syntax
      error=1
      errorflag=1
    if error==0:
      curtime = time.strftime('%Y-%m-%d %H:%M:%S')
      htmlsource=r.content
#      htmlsource1=re.sub(musterregex,"",htmlsource)
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
              #logger.debug(musterURL)
              code="%s_%s_%s" % (pobj.blockCode,finyear,emusterno)
              codeObjDict[code]=myMuster
              d={}
              d['block']=pobj.blockCode
              d['finyear']=finyear
              d['musterNo']=emusterno
              d['dateFrom']=datefrom
              d['dateTo']=dateto
              d['workCode']=workCode
              d['workName']=workName
              d['musterType']='10'
              d['musterURL']=musterURL
              d['isRequired']=1
              d['panchayat']=pobj.panchayatCode
              musterDict[code]=d
        if musterCount == 0:
          ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
          ps.zeroMusters=True
          ps.save()
  pobj.musterDict=musterDict
  return errorflag



def processJobcardRegister(logger,pobj):
    reportType="applicationRegister"
    finyear=getCurrentFinYear()
    error,myhtml=getReportHTML(logger,pobj,reportType,finyear)
    jobcardDict={}
    workerDict={}
    villageDict={}
    codeObjDict=pobj.codeObjDict
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
          p={}
          p['panchayat']=pobj.panchayatCode
          p['name']=villageName
          villageDict[villageCode] = p
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
            code=jobcard
            d={}
            d['panchayat'] = pobj.panchayatCode
            d['jobcard'] = jobcard
            d['caste'] = caste
            d['headOfHousehold'] = headOfHousehold
            d['village'] = villageCode
            d['issueDate']=issueDate
            d['applicationDate'] = applicationDate
            d['jcNo'] = getjcNumber(jobcard)
            jobcardDict[code]=d
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
          code="%s_%s" %(jobcard,name)
          codeObjDict[code]=myWorker
          d={}
          d['jobcard']=jobcard
          d['name']=name
          d['applicantNo']=applicantNo
          d['gender']=gender
          d['age']=age
          d['fatherHusbandName']=fatherHusbandName
          d['isDeleted']=isDeleted
          d['isDisabled']=isDisabled
          d['isMinority']=isMinority
          d['remarks']=remarks
          workerDict[code]=d
    if len(jobcardDict.keys()) == 0:
      error="No Jobcards Found"
    pobj.jobcardDict=jobcardDict
    pobj.workerDict=workerDict
    pobj.villageDict=villageDict
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

def crawlWagelists(logger,pobj):
  error=None
  errorflag=0
  musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  wagelistDict={}
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps.zeroMusters == False:
      logger.debug("Crawling Wagelist for panchyat %s-%s finyear %s " % (pobj.panchayatCode,pobj.panchayatName,finyear))
      fullFinYear=getFullFinYear(finyear)
      url="http://mnregaweb4.nic.in/netnrega/state_html/emuster_wage_rep1.aspx?type=6&lflag=eng&state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&panchayat_code=%s&fin_year=%s" % (pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.panchayatCode,fullFinYear)
      logger.debug(url)
      try:
        r  = requests.get(url)
        error=0
      #except requests.exceptions.RequestException as e:  # This is the correct syntax
      except:  # This is the correct syntax
        error=1
        errorflag=1
      if error==0:
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
              d={}
              d['finyear'] = finyear
              d['block']=pobj.blockCode
              d['wagelistNo']=wagelistNo
              d['generateDate']=dateObject
              d['panchayat']=pobj.panchayatCode
              wagelistDict[code]=d
  pobj.wagelistDict=wagelistDict
  return errorflag

def crawlFTO(logger,pobj):
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  ftoFound=None
  ftoDict={}
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    logger.debug("Crawling FTO for panchyat %s-%s finyear %s " % (pobj.panchayatCode,pobj.panchayatName,finyear))
    fullFinYear=getFullFinYear(finyear)
    urls=[]
    url="http://%s/netnrega/FTO/fto_sign_detail.aspx?lflag=eng&flg=W&page=p&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&block_code=%s&fin_year=%s&typ=sec_sig&mode=b&rptblk=t&source=&" % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,pobj.blockCode,fullFinYear)
    urls.append(url)
    url="http://%s/netnrega/FTO/fto_sign_detail.aspx?lflag=eng&flg=W&page=p&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&block_code=%s&fin_year=%s&typ=sec_sig&mode=P&rptblk=t&source=&" % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,pobj.blockCode,fullFinYear)
    urls.append(url)
    url="http://%s/netnrega/FTO/fto_sign_detail.aspx?lflag=eng&flg=W&page=p&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&block_code=%s&panchayat_name=%s&panchayat_code=%s&fin_year=%s&typ=sec_sig&mode=b&source=" % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,pobj.blockCode,pobj.panchayatName,pobj.panchayatCode,fullFinYear)
    urls.append(url)
    url="http://%s/netnrega/FTO/fto_sign_detail.aspx?lflag=eng&flg=W&page=p&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&block_code=%s&panchayat_name=%s&panchayat_code=%s&fin_year=%s&typ=sec_sig&mode=P&source=" % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,pobj.blockCode,pobj.panchayatName,pobj.panchayatCode,fullFinYear)
    urls.append(url)
    
    for url in urls:
      if "panchayat_name" in url:
        ftoPanchayatCode=pobj.panchayatCode
      else:
        ftoPanchayatCode=None
      logger.info(url)
      try:
        r  = requests.get(url)
        error=0
      #except requests.exceptions.RequestException as e:  # This is the correct syntax
      except:  # This is the correct syntax
        error=1
        errorflag=1
      if error==0:
        curtime = time.strftime('%Y-%m-%d %H:%M:%S')
        htmlsource=r.content
        htmlsource1=htmlsource #re.sub(musterregex,"",htmlsource)
        htmlsoup=BeautifulSoup(htmlsource1,"lxml")
        try:
          table=htmlsoup.find('table',bordercolor="black")
          rows = table.findAll('tr')
          errorflag=0
        except:
          status=0
          errorflag=1
        if errorflag==0:
          for tr in rows:
            cols = tr.findAll('td')
            if "FTO No" in str(tr):
              logger.debug("Found the header row")
            else:
              ftoNo=cols[1].text.lstrip().rstrip()
              ftoFound=1
              if pobj.stateShortCode in ftoNo:
                ftoRelativeURL=cols[1].find("a")['href']
                ftoURL="http://%s/netnrega/FTO/%s" % (pobj.crawlIP,ftoRelativeURL)
                logger.info(ftoURL)
                paymentMode=cols[2].text.lstrip().rstrip()
                secondSignatoryDateString=cols[3].text.lstrip().rstrip()
                secondSignatoryDate=correctDateFormat(secondSignatoryDateString)
                ftoArray=ftoNo.split("_")
                firstSignatoryDateString=ftoArray[1][:6]
                firstSignatoryDate=correctDateFormat(firstSignatoryDateString,dateFormat='%d%m%y')
                try:
                  myFTO=FTO.objects.create(block=pobj.block,finyear=finyear,ftoNo=ftoNo)
                except:
                  myFTO=FTO.objects.filter(block=pobj.block,finyear=finyear,ftoNo=ftoNo).first()
                if ftoPanchayatCode is not None:
                  myFTO.panchayat=pobj.panchayat
                myFTO.secondSignatoryDate=secondSignatoryDate
                myFTO.firstSignatoryDate=firstSignatoryDate
                myFTO.ftoNo=ftoNo
                myFTO.paymentMode=paymentMode
                myFTO.ftoURL=ftoURL
                myFTO.save() 
                code="%s_%s_%s" % (pobj.blockCode,finyear,ftoNo)
                d={}
                d['finyear'] = finyear
                d['block']=pobj.blockID
                d['panchayat']=pobj.panchayatCode
                d['secondSignatoryDate']=secondSignatoryDate
                d['firstSignatoryDate']=firstSignatoryDate
                d['ftoNo']: ftoNo
                d['paymentMode']: paymentMode
                ftoDict[code]=d
  pobj.ftoDict=ftoDict
  if ftoFound is not None:
    error=None
  else:
    error="No FTO found"
  return error

def musterWagelistCrawl(logger,pobj):
  error=None
  wagelisterror=None
  mustererror=None
  musterDict={}
  wagelistDict={}
  mustererror=crawlMusters(logger,pobj)
  wagelisterror=crawlWagelists(logger,pobj)
  logger.info(mustererror)
  logger.info(wagelisterror)
  if mustererror or wagelisterror:
    error="Error in crawling musters and Wagelist"
  return error




def processPanchayatInstanceJSONData(logger,pobj,myDict):
  codeObjDict={}
  eachPanchayat=Panchayat.objects.filter(id=pobj.panchayatID).first()
  eachBlock=Block.objects.filter(code=pobj.blockCode).first()
  codeObjDict[pobj.panchayatCode]=eachPanchayat
  codeObjDict[pobj.blockCode]=eachBlock
  for modelName,parentDict in myDict.items():
    for code,modelDict in parentDict.items():
      logger.debug(code)
      error,instance,d=createOrUpdate(logger,getattr(nregamodels,modelName),code,modelDict,codeObjDict=codeObjDict)
      if error is not None:
        logger.info(error)
      else:
        codeObjDict[code]=instance
  return codeObjDict
def saveInstance(model_class,code,data,instance,codeObjDict=None):
    error=None
    d={}
    for key,value in data.items():
        try: 
          field = model_class._meta.get_field(key)
        except: 
          field = None  #model_class._meta.get_field(key)
        if not field:
            continue
        if isinstance(field, models.ManyToManyField):
            # can't add m2m until parent is saved
            continue
        elif isinstance(field, models.ForeignKey):
              try:
                obj=codeObjDict[value]
              except:
                obj=getattr(nregamodels,key.title()).objects.filter(code=value).first()
              if obj is not None:
                setattr(instance, key, obj)
              else:
                d[key]=value
                error="error Model %s Code %s Field %s Value %s " % (model_class,code,key,value)
        else:
              setattr(instance, key, value)
    instance.save()
    # now add the m2m relations
    for field in model_class._meta.many_to_many:
        if field.name in data:
          obj=getattr(nregamodels,field.name.title()).objects.filter(code=data[field.name]).first()
          if obj is not None:
            getattr(instance, field.name).add(obj)
    return error,instance,d



def createOrUpdate(logger,model_class,code,data,codeObjDict=None):
  get_or_create_kwargs = {
      'code': code
  }
  try:
    #instance = model_class(**get_or_create_kwargs)
    instance = model_class()
    error,instance,d=saveInstance(model_class,code,data,instance,codeObjDict=codeObjDict)
  except:
    logger.info("Try Failed I am in expect")
    instance = model_class.objects.get(**get_or_create_kwargs)
    error,instance,d=saveInstance(model_class,code,data,instance,codeObjDict=codeObjDict)
  return error,instance,d

def generateReportsComputeStats(logger,pobj):
    eachPanchayat=pobj.panchayat
    startFinYear=pobj.startFinYear
    endFinYear=getCurrentFinYear()
    accuracy=100
    for finyear in range(int(startFinYear),int(endFinYear)+1):
      finyear=str(finyear)
      curAccuracy=computePanchayatStat(logger,pobj,str(finyear))
      createJobcardStatReport(logger,pobj,finyear)
      createDetailWorkPaymentReport(logger,pobj,finyear)
      if curAccuracy <= accuracy:
        accuracy=curAccuracy
    return accuracy

def downloadWorkDemand(logger,pobj):
  error=None
  reportType="workDemandHTML"
  reportName="Work Demand HTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps.zeroMusters == False:
      finyearMinus1=str(finyear-1)
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
  #         'Referer': panchayatURL,
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
def processWorkDemand(logger,pobj):
  error=None
  reportType="workDemandHTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps.zeroMusters == False:
      error1,myhtml=getReportHTML(logger,pobj,reportType,finyear)
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
                  curCount=int(pobj.jobcardStatDict[jobcard][finyear]['daysDemanded'])
                except:
                  curCount=0
                pobj.jobcardStatDict[jobcard][finyear]['daysDemanded']=str(curCount+daysDemanded)

                #logger.info(str(pobj.jobcardStatDict[jobcard]))
                workerCode="%s_%s" % (jobcard,workerName)
                myWorker=Worker.objects.filter(code=workerCode).first()
                if myWorker is not None:
                  try:
                    wd=WorkDemand.objects.create(worker=myWorker,workDemandDate=demandDate,daysDemanded=daysDemanded,finyear=finyear)
                  except:
                    error=1
              #logger.info("Jobcard %s worker %s Demanded Date %s days %s " % (jobcard,workerName,str(demandDate),str(daysDemanded)))
 
  for jobcard,jdict in pobj.jobcardStatDict.items():
    myJobcard=Jobcard.objects.filter(code=jobcard).first()
    for finyear,myDict in jdict.items():
      try:
        js=JobcardStat.objects.create(jobcard=myJobcard,finyear=finyear)
      except:
        js=JobcardStat.objects.filter(jobcard=myJobcard,finyear=finyear).first()
      js.daysDemanded=myDict['daysDemanded']  
      js.save()
  objs=JobcardStat.objects.filter(jobcard__panchayat=pobj.panchayat).values("finyear").annotate(dsum=Sum('daysDemanded'))
  for obj in objs:
    finyear=obj['finyear']
    daysDemanded=obj['dsum']
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps is None:
      ps=PanchayatStat.objects.create(panchayat=pobj.panchayat,finyear=finyear)
    ps.nicDaysDemanded=daysDemanded
    logger.info("finyear %s Days Demanded %s "  % (finyear,str(daysDemanded)))
    ps.save()

  return error
   
   
def downloadWagelist(logger,pobj,obj):
  error=None
  s3url=None
  #Work;ing Wagelist URL
  #http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=&district_code=3405&state_name=JHARKHAND&district_name=PALAMU&block_code=3405008&wg_no=3405008WL060766&short_name=JH&fin_year=2018-2019&mode=wg
  wagelistNo=obj.wagelistNo
  finyear=obj.finyear
  fullFinYear=getFullFinYear(finyear)
  url="http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=%s&district_code=%s&state_name=%s&district_name=%s&block_code=%s&wg_no=%s&short_name=%s&fin_year=%s&mode=wg" % (pobj.stateCode,pobj.districtCode,pobj.stateName,pobj.districtName,pobj.blockCode,wagelistNo,pobj.stateShortCode,fullFinYear)
  logger.info(url)
  r=requests.get(url)
  if r.status_code != 200:
    error="untable to download"
  if r.status_code == 200:
    myhtml=r.content
    error,myTable=validateWagelistHTML(logger,pobj,myhtml)
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
    allFTOFound=True
    for row in rows:
      if "WageList No" not in str(row):
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
          allWorkerFound=False

        try:
          myFTO=pobj.codeObjDict[ftoCode]
          isWagelistFTOAbsent=False
        except:
          myFTO=None
          isWagelistFTOAbsent=True
        logger.info("%s fto code %s isABsent %s " % (wagelistIndex,ftoCode,str(isWagelistFTOAbsent)))
        if (myWorker is not None) and (myFTO is None):
          fts=FTOTransaction.objects.filter(wagelist=obj,jobcard=myWorker.jobcard)
          isFTOSame=False
          ftoNo=None
          for ft in fts:
            ftoObj=ft.fto
            if ftoNo is None:
              isFTOSame=True
              ftoNo=ft.fto.ftoNo
            elif ftoNo != ft.fto.ftoNo:
              isFTOSame=False

          if isFTOSame == True:
            myFTO=ftoObj

        if myFTO is None:
          allFTOFound=False
        #logger.info("myWorker is %s " % str(myWorker))
        #logger.info("myFTO is %s " % str(myFTO))
        try:
          wt=WagelistTransaction.objects.create(wagelist=obj,wagelistIndex=wagelistIndex)
        except:
          wt=WagelistTransaction.objects.filter(wagelist=obj,wagelistIndex=wagelistIndex).first()
        wt.fto=myFTO
        wt.worker=myWorker
        wt.daysWorked=daysWorked
        wt.totalWage=totalWage
        wt.isRegenerated=regenerated
        wt.isWagelistFTOAbsent=isWagelistFTOAbsent
        wt.save()
    obj.allWorkerFound=allWorkerFound
    obj.allFTOFound=allFTOFound
    obj.isDownloaded=True
    obj.downloadDate=timezone.now()
    obj.downloadAttemptCount=0
    obj.save()

def validateWagelistHTML(logger,pobj,myhtml): 
  error=None
  myTable=None
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  #logger.info(jobcardPrefix)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll('table')
  for table in tables:
    if jobcardPrefix in str(table):
       myTable=table
  if myTable is None:
    error="Table not found"
  return error,myTable

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
    logger.info("Queue Size %s Thread %s  ModelName %s finyear %s Object ID %s  code %s " % (str(q.qsize()),name,modelName,finyear,obj.id,obj.code))
    key="%s%s%s" % (method,modelName,statePrefix)
    try:
      #logger.info(key)
      libtechMethodNames[key](logger,pobj,obj)
    except Exception as e:
      logging.info(e, exc_info=True)
    q.task_done()

def downloadMusterNew(logger,pobj,obj):
  error=None
  s3url=None
  datefromstring=obj.dateFrom.strftime("%d/%m/%Y")
  datetostring=obj.dateTo.strftime("%d/%m/%Y")
  workName=obj.workName
  workCode=obj.workCode
  emusterno=obj.musterNo
  fullfinyear=getFullFinYear(obj.finyear)
  #logger.info(datefromstring)
  '''
  Karnataka WOrking URL
  url="http://%s/citizen_html/musternew.asp?state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&workcode=%s&panchayat_code=%s&msrno=%s&finyear=%s&dtfrm=%s&dtto=%s&wn=%s&id=1" %  (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,workCode,pobj.panchayatCode,emusterno,fullfinyear,datefromstring,datetostring,workName.replace(" ","+"))
  '''
  musterURL="http://%s/citizen_html/musternew.asp?lflag=&id=1&state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&block_code=%s&msrno=%s&finyear=%s&workcode=%s&dtfrm=%s&dtto=%s&wn=%s" % (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.blockCode,emusterno,fullfinyear,workCode,datefromstring,datetostring,workName.replace(" ","+"))
  
  #logger.info(musterURL)
  r=requests.get(obj.newMusterURL)
  #logger.info(obj.newMusterURL)
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
      ('fy', fullfinyear),
      ('q', '%s----' % (emusterno)),
      ('sh', pobj.stateShortCode),
      ('t', 'D'),
      ('wc', '%s$$$' % (workCode)),
     )
    url2="http://%s/citizen_html/msrLogic.asp?q=%s----&t=D&fy=%s&bc=%s&sh=%s&wc=%s$$$&sn=&dn=&bn=&pn=" % (pobj.crawlIP,emusterno,fullfinyear,pobj.panchayatCode[:7],pobj.stateShortCode,workCode)
    #logger.info(url2)
    #response = requests.get('http://%s/citizen_html/msrLogic.asp' % (pobj.crawlIP), headers=headers, params=params, cookies=cookies)
    response = requests.get(url2, headers=headers, cookies=cookies)
    myhtml=response.content
    error,musterSummaryTable,musterTable=validateMusterHTML(logger,pobj,myhtml,workCode)
    #logger.info(error)
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
      #logger.info(s3url)
      #logger.debug("Save muster %s " % str(obj.id))
  else:
    error="could not download muster"

  obj=Muster.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)


def downloadMuster(logger,pobj,obj):
  error=None
  s3url=None
  datefromstring=obj.dateFrom.strftime("%d/%m/%Y")
  datetostring=obj.dateTo.strftime("%d/%m/%Y")
  workName=obj.workName
  workCode=obj.workCode
  emusterno=obj.musterNo
  fullfinyear=getFullFinYear(obj.finyear)
  #logger.info(datefromstring)
  '''
  Karnataka WOrking URL
  url="http://%s/citizen_html/musternew.asp?state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&workcode=%s&panchayat_code=%s&msrno=%s&finyear=%s&dtfrm=%s&dtto=%s&wn=%s&id=1" %  (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,workCode,pobj.panchayatCode,emusterno,fullfinyear,datefromstring,datetostring,workName.replace(" ","+"))
  '''
  musterURL="http://%s/citizen_html/musternew.asp?lflag=&id=1&state_name=%s&district_name=%s&block_name=%s&panchayat_name=%s&block_code=%s&msrno=%s&finyear=%s&workcode=%s&dtfrm=%s&dtto=%s&wn=%s" % (pobj.crawlIP,pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.blockCode,emusterno,fullfinyear,workCode,datefromstring,datetostring,workName.replace(" ","+"))
  
  #logger.info(musterURL)
  r=requests.get(obj.newMusterURL)
  logger.info(obj.newMusterURL)
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

    params = (
      ('q', '%s--%s--%s' % (emusterno,datefromstring,datetostring)),
      ('t', 'D'),
      ('fy', fullfinyear),
      ('bc', pobj.blockCode),
      ('sh', pobj.stateShortCode),
      ('wc', '%s$$$%s' % (workCode,workName)),
      ('sn', pobj.stateName),
      ('dn', pobj.districtName),
      ('bn', pobj.blockName),
      ('pn', ''),
     )

    response = requests.get('http://%s/citizen_html/msrLogic.asp' % (pobj.crawlIP), headers=headers, params=params, cookies=cookies)
    myhtml=response.content
    error,musterSummaryTable,musterTable=validateMusterHTML(logger,pobj,myhtml,workCode)
    #logger.info(error)
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
      #logger.info(s3url)
      #logger.debug("Save muster %s " % str(obj.id))
  else:
    error="could not download muster"

  obj=Muster.objects.filter(id=obj.id).first()
  updateObjectDownload(logger,obj,error,s3url)

def validateMusterHTML(logger,pobj,myhtml,workCode):
  error=None
  musterSummaryTable=None
  musterTable=None
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll('table')
  for table in tables:
    if jobcardPrefix in str(table):
      musterTable=table
    elif workCode in str(table):
      musterSummaryTable=table
  if musterSummaryTable is None:
    error="Muster Summary Table not found"
  elif musterTable is None:
    error="Muster Table not found"
  return error,musterSummaryTable,musterTable

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
          if (creditedDate is not None) and (dateTo is not None):
            diffDays=(creditedDate-dateTo).days
            if diffDays > 30:
              isDelayedPayment = True
            else:
              isDelayedPayment = False
          elif (creditedDate is None) and (dateTo is not None):
            diffDays=(datetime.datetime.today() - dateTo).days
            if diffDays > 30:
              isDelayedPayment = True
            else:
              isDelayedPayment = False
          else:
              isDelayedPayment=False
          pr.isDelayedPayment=isDelayedPayment
          if payOrderDate is not None:
            datetimeObject=payOrderDate#datetime.datetime.strptime(epayOrderDate, '%Y-%m-%d')
            if datetimeObject.month <= 3:
              finyear=str(datetimeObject.year)[2:]
            else:
              finyear=str(datetimeObject.year+1)[2:]
            pr.finyear=finyear
          #logger.debug("The PR id is %s " % str(pr.id))
          pr.save()


def processMuster(logger,pobj,obj):
  myhtml=None
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    logger.info("ProcessMuster %s " % obj)
    wdDict={}
    remarks=''
    finyear=obj.finyear
    allWorkerFound=True
    allWagelistFound=True
    allWTFound=True
    #logger.info("Proessing Muster")
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    #Finding teh payment Date
    ptds=htmlsoup.find_all("td", text=re.compile("Payment Date"))
    paymentDate=None
    if len(ptds) == 1:
      ptdText=ptds[0].text
      paymentDateString=ptdText.split(":")[1].lstrip().rstrip()
      paymentDate=getDateObj(paymentDateString)
    mytable=htmlsoup.find('table',id="musterDetails")
    tr_list = mytable.findAll('tr')
    reMatchString="%s-" % (pobj.stateShortCode)
    for i in range(len(tr_list)):
      wdRemarks=''
      cols=tr_list[i].findAll("td")
      if len(cols) > 7:
        nameandjobcard=cols[1].text.lstrip().rstrip()
        if pobj.stateShortCode in nameandjobcard:
          nameandjobcard=nameandjobcard.replace('\n',' ')
          nameandjobcard=nameandjobcard.replace("\\","")
          nameandjobcardarray=re.match(r'(.*)'+reMatchString+'(.*)',nameandjobcard)
          name_relationship=nameandjobcardarray.groups()[0]
          name=name_relationship.split("(")[0].lstrip().rstrip()
          jobcard=reMatchString+nameandjobcardarray.groups()[1].lstrip().rstrip()
          totalWage=cols[24].text.lstrip().rstrip()
          accountNo=cols[25].text.lstrip().rstrip()
          dayWage=cols[21].text.lstrip().rstrip()
          daysWorked=cols[20].text.lstrip().rstrip()
          wagelistNo=cols[29].text.lstrip().rstrip()
    #      accountNo=cols[26].text.lstrip().rstrip()
          bankName=cols[26].text.lstrip().rstrip()
          branchName=cols[27].text.lstrip().rstrip()
          branchCode=cols[28].text.lstrip().rstrip()
          creditedDateString=cols[30].text.lstrip().rstrip()
          creditedDate=getDateObj(creditedDateString)
          #logger.info("name %s jobcard %s " % (name,jobcard))
          musterIndex=cols[0].text.lstrip().rstrip()
          daysProvided=0
          for attendanceIndex in range(int(musterStartAttendanceColumn),int(musterEndAttendanceColumn)+1):
            if cols[attendanceIndex].text.lstrip().rstrip() != "":
              daysProvided=daysProvided+1
          workerCode="%s_%s" % (jobcard,name)
          wagelistCode="%s_%s_%s" % (pobj.blockCode,finyear,wagelistNo)

          wt=None
          try:
            myWorker=pobj.codeObjDict[workerCode]
          except:
            myWorker = None
            logger.info("Worker not found %s " % (workerCode))
            remarks+="Worker not found %s " % (workerCode)
            allWorkerFound=False

          try:
            myWagelist=pobj.codeObjDict[wagelistCode]
          except:
            myWagelist=None
            remarks+="Wagelist not found %s " % (wagelistCode)
            allWagelistFound=False
          wtCode="%s_%s" % (workerCode,wagelistCode)
         #try:
         #  wt=pobj.codeObjDict[wtCode]
         #except:
         #  wt=None
         #  remarks+="Wagelist Transaction not found %s " % (wtCode)
         #  allWTFound=None
          #logger.debug("myWorker is %s " % str(myWorker))
          #logger.debug("MyWagelist is %s " % str(myWagelist))
          #logger.debug("myWagelist Transaction is %s " % str(wt))
          try:
            wd=WorkDetail.objects.create(muster=obj,musterIndex=musterIndex)
          except:
            wd=WorkDetail.objects.filter(muster=obj,musterIndex=musterIndex).first()

          isDelayedPayment=False
          isNICDelayedPayment=False
          paymentDelay=None
          nicDelay=None
          nicDelayDays=None
          delayDays=None
          rejectedFlag=False
          if wt is not None:
            if wt.isRegenerated==True:
              rejectedFlag=True
              
          if creditedDate is not None:
            status='credited'
          elif myWagelist is None:
            status='wagelistNotGenerated'
          else:
            status="unknown"
            myWagelist.isRequired=True
            myWagelist.save()

    #       if wt is not None:
    #         if wt.isRegenerated == True:
    #           status="rejected"
    #         else:
    #           if wt.fto is None:
    #             status="ftoNotGenerated"
    #           else:
    #             status="ftoGenerated"
    #             fts=FTOTransaction.objects.filter(fto=wt.fto,wagelist=wt.wagelist,jobcard=myWorker.jobcard)
    #             if len(fts) == 1:
    #               logger.info("no of fts is one")
    #               ft=fts.first()
    #               creditedDate=ft.processDate
    #               wdRemarks+="Credited Date information source is FTO, Muster credited Date is null"
    #               status="credited"
    #           
    #       else:
    #         status="wagelistNotGenerated"
               
          if creditedDate is not None:
            paymentDelay=(creditedDate-obj.dateTo).days
            if paymentDelay > delayPaymentThreshold:
              delayDays=paymentDelay-delayPaymentThreshold
              isDelayedPayment=True
            else:
              delayDays=0
          try:
            #nicDelay=(wt.fto.secondSignatoryDate-obj.dateTo).days
            nicDelay=(paymentDate-obj.dateTo).days
            if nicDelay > delayPaymentThreshold:
              nicDelayDays=nicDelay-delayPaymentThreshold
              isNICDelayedPayment=True
            else:
              nicDelayDays=0
          except:
            nicDelay=None
          wd.isNICDelayAccounted=False
          if myWorker is not None:
            dp=DPTransaction.objects.filter(worker=myWorker,muster=obj).first()
            if dp is not None:
              wd.dpTransaction=dp
              wd.isNICDelayAccounted=True

          wd.curWagelist=myWagelist
          wd.accountNo=accountNo
          wd.bankName=bankName
          wd.branchName=branchName
          wd.branchCode=branchCode
          wd.worker=myWorker
      #    wd.wagelistTransaction=wt
          wd.totalWage=totalWage
          wd.dayWage=dayWage
          wd.daysWorked=daysWorked
          wd.daysProvided=daysProvided
          wd.creditedDate=creditedDate
          wd.ftoDelay=nicDelay
          wd.paymentDelay=paymentDelay
          wd.rejectedFlag=rejectedFlag
          wd.status=status
          wd.nicDelayDays=nicDelayDays
          wd.delayDays=delayDays
          wd.isNICDelayedPayment=isNICDelayedPayment
          wd.isDelayedPayment=isDelayedPayment
          wd.remarks=wdRemarks
          wd.save()
          #logger.info("Save %s " % str(wd.id))
          wdCode="%s_%s" % (obj.code,str(musterIndex))
          d={}
          d['code']=wdCode
          d['wagelist']=wagelistCode
          d['worker']=workerCode
          d['musterIndex']=musterIndex
          d['muster']=obj.code
          d['totalWage']=totalWage
          d['dayWage']=dayWage
          d['daysWorked']=daysWorked
          d['creditedDate']=creditedDate
          d['status']=status
          d['ftoDelay']=nicDelay
          d['paymentDelay']=paymentDelay
          d['rejectedFlag']=rejectedFlag
          d['status']=status
          d['nicDelayDays']=nicDelayDays
          d['delayDays']=delayDays
          d['isNICDelayedPayment']=isNICDelayedPayment
          d['isDelayedPayment']=isDelayedPayment
          wdDict[wdCode]=d
    pobj.workDetailDict['obj.code']=wdDict
    obj.allWorkerFound=allWorkerFound
    obj.allWagelistFound=allWagelistFound
    obj.allWTFound=allWTFound
    obj.paymentDate=paymentDate
    obj.remarks=remarks
    obj.save()

def createJobcardStatReport(logger,pobj,finyear):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',')
  #w = csv.writer(f, newline='',delimiter=',')
  reportType="jobcardStat"
  logger.debug("Createing jobcard Stat Payment report for panchayat: %s panchayatCode: %s ID: %s" % (pobj.panchayatName,pobj.panchayatCode,str(pobj.panchayatID)))
  a=[]
#  workRecords=WorkDetail.objects.filter(id=16082209)
  a.extend(["jobcard","village","caste","name","daysDemanded","nicDaysProvided","daysProvided","daysWorked"])
  w.writerow(a)
  stats=JobcardStat.objects.filter(finyear=finyear,jobcard__panchayat=pobj.panchayat)
  for js in stats:
    try:
      villageName=js.jobcard.village.name
    except:
      villageName=""
    a=[]
    a.extend([js.jobcard.jobcard,villageName,js.jobcard.caste,js.jobcard.headOfHousehold,str(js.daysDemanded),str(js.nicDaysProvided),str(js.daysProvided),str(js.daysWorked)])
    w.writerow(a)
  f.seek(0)
#  with open("/tmp/a.csv","wb") as f1:
#    shutil.copyfileobj(f, f1)
  outcsv=f.getvalue()
  filename=pobj.panchayatSlug+"_"+finyear+"_jsStat.csv"
  filepath=pobj.panchayatFilepath.replace("filename",filename)
  contentType="text/csv"
  savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)

def computePanchayatStat(logger,pobj,finyear):
  eachPanchayat=pobj.panchayat
  mustersTotal=len(Muster.objects.filter(panchayat=eachPanchayat,finyear=finyear))
  mustersDownloaded=len(Muster.objects.filter(isDownloaded=True))
  mustersAllApplicantsFound=len(Muster.objects.filter(panchayat=eachPanchayat,finyear=finyear,isDownloaded=True,allWorkerFound=True))
  mustersMissingApplicant=mustersDownloaded-mustersAllApplicantsFound
  jobcardsTotal=len(Jobcard.objects.filter(panchayat=eachPanchayat)) 
  workersTotal=len(Worker.objects.filter(jobcard__panchayat=eachPanchayat,isDeleted=False))
  libtechWorkDaysPanchayatwise=WorkDetail.objects.filter(muster__panchayat=eachPanchayat,muster__finyear=finyear).aggregate(Sum('daysWorked')).get('daysWorked__sum')
  libtechWorkDays=WorkDetail.objects.filter(worker__jobcard__panchayat=eachPanchayat,muster__finyear=finyear).aggregate(Sum('daysWorked')).get('daysWorked__sum')
  libtechWorkDaysProvided=WorkDetail.objects.filter(worker__jobcard__panchayat=eachPanchayat,muster__finyear=finyear).aggregate(Sum('daysProvided')).get('daysProvided__sum')
  if libtechWorkDays is None:
    libtechWorkDays=0
  if libtechWorkDaysProvided is None:
    libtechWorkDaysProvided=0
  libtechTotalWages=WorkDetail.objects.filter(worker__jobcard__panchayat=eachPanchayat,muster__finyear=finyear).aggregate(Sum('totalWage')).get('totalWage__sum')
  libtechCreditedWages=WorkDetail.objects.filter(worker__jobcard__panchayat=eachPanchayat,muster__finyear=finyear,status="credited").aggregate(Sum('totalWage')).get('totalWage__sum')
  ps=myPanchayatStat=PanchayatStat.objects.filter(panchayat=eachPanchayat,finyear=finyear).first()
  if myPanchayatStat is None:
    ps=PanchayatStat.objects.create(panchayat=eachPanchayat,finyear=finyear)

  nicWorkDays=ps.nicWorkDays
  nicTotalWages=ps.nicTotalWages
  ps.mustersTotal=mustersTotal
  ps.mustersPendingDownload=mustersTotal-mustersDownloaded
  ps.musterMissingApplicants=mustersMissingApplicant
  ps.mustersDownloaded=mustersDownloaded
  ps.jobcardsTotal=jobcardsTotal
  ps.workersTotal=workersTotal
  ps.libtechWorkDays=libtechWorkDays
  ps.libtechWorkDaysProvided=libtechWorkDaysProvided
  ps.libtechTotalWages=libtechTotalWages
  ps.libtechWorkDaysPanchayatwise=libtechWorkDaysPanchayatwise
  wageAccuracy=getAccuracy(ps.nicTotalWages,libtechTotalWages)
  logger.info("nic Wages %s libtechWages %s wageAccuracy %s " % (str(ps.nicTotalWages),str(libtechTotalWages),str(wageAccuracy)))
  workDaysAccuracy=getAccuracy(ps.nicTotalDaysProvided,libtechWorkDaysProvided)
  logger.info("nic workdays %s libtechworkdays %s provided %s  Accuracy %s " % (str(ps.nicTotalDaysProvided),str(libtechWorkDays),str(libtechWorkDaysProvided),str(workDaysAccuracy)))
#  accuracy=max(workDaysAccuracy,wageAccuracy)
  accuracy=workDaysAccuracy
  ps.accuracy=accuracy
  ps.save()
  logger.info("Accuracy %s " % str(accuracy)) 
  return accuracy

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


def createDetailWorkPaymentReport(logger,pobj,finyear):
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',')
  table=[]
  #w = csv.writer(f, newline='',delimiter=',')
  logger.debug("Createing work Payment report for panchayat: %s panchayatCode: %s ID: %s" % (pobj.panchayatName,pobj.panchayatCode,str(pobj.panchayatID)))
  a=[]
#  workRecords=WorkDetail.objects.filter(id=16082209)
  a.extend(["jobcard","name","musterNo","workCode","workName","dateFrom","dateTo","daysWorked","totalWage","accountNo","bankName","branchName","branchCode","paymentDate","status","creditedDate","rejectedFlag","secondSignatoryDate","wagelistNo","ftoNo","nicDelayed","isDelayed","nicDelayDays","delayDays","isNICDelayAccounted","DPDays","DPAmount","DP Status","WDID","WTID","MID","remarks"])
  table.append(a)
  w.writerow(a)
  workRecords=WorkDetail.objects.filter(worker__jobcard__panchayat=pobj.panchayat,muster__finyear=finyear)
  logger.debug("Total Work Records: %s " %str(len(workRecords)))
  for wd in workRecords:
    workName=wd.muster.workName
    applicantName=wd.worker.name
    musterLink='<a href="%s">%s</a>' % (wd.muster.contentFileURL,wd.muster.musterNo)
    if wd.curWagelist is not None:
      wagelist=wd.curWagelist.wagelistNo
      wagelistLink='<a href="%s">%s</a>' % (wd.curWagelist.contentFileURL,wagelist)

    else:
      wagelist=""
    try:
      ftoNo=wd.wagelistTransaction.fto.ftoNo
      secondSignatoryDate=wd.wagelistTransaction.fto.secondSignatoryDate
    except:
      ftoNo=""
      secondSignatoryDate=""
    if wd.wagelistTransaction is not None:
      wtID=wd.wagelistTransaction.id
    else:
      wtID=None
    if wd.dpTransaction is not None:
      delayDays=wd.dpTransaction.delayDays
      delayCompensation=wd.dpTransaction.delayCompensation
      delayStatus=wd.dpTransaction.status
    else:
      delayDays=''
      delayCompensation=''
      delayStatus=''
    fatherHusbandName=wd.worker.fatherHusbandName
    a=[]
    a.extend([wd.worker.jobcard.jobcard,applicantName,wd.muster.musterNo,wd.muster.workCode,workName,str(wd.muster.dateFrom),str(wd.muster.dateTo),str(wd.daysWorked),str(wd.totalWage),wd.accountNo,wd.bankName,wd.branchName,wd.branchCode,str(wd.muster.paymentDate),wd.status,str(wd.creditedDate),str(wd.rejectedFlag),str(secondSignatoryDate),wagelist,ftoNo,wd.isNICDelayedPayment,wd.isDelayedPayment,str(wd.nicDelayDays),str(wd.delayDays),str(wd.isNICDelayAccounted),str(delayDays),str(delayCompensation),delayStatus,str(wd.id),str(wtID),str(wd.muster.id),wd.remarks])
    w.writerow(a)
    a=[]
    a.extend([wd.worker.jobcard.jobcard,applicantName,musterLink,wd.muster.workCode,workName,str(wd.muster.dateFrom),str(wd.muster.dateTo),str(wd.daysWorked),str(wd.totalWage)," ",wd.status,str(wd.creditedDate),str(wd.rejectedFlag),str(secondSignatoryDate),wagelistLink,ftoNo,wd.isNICDelayedPayment,wd.isDelayedPayment,str(wd.nicDelayDays),str(wd.delayDays),str(wd.isNICDelayAccounted)])
    table.append(a)
  f.seek(0)
#  with open("/tmp/a.csv","wb") as f1:
#    shutil.copyfileobj(f, f1)
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

  reportType="detailWorkPayment"
  outcsv=f.getvalue()
  filename=pobj.panchayatSlug+"_"+finyear+"_wpDetailed.csv"
  filepath=pobj.panchayatFilepath.replace("filename",filename)
  contentType="text/csv"
  savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)

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
      with open("/tmp/dp.html","wb") as f:
        f.write(myhtml)
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


def downloadMISDPReport(logger,pobj):
  error=None
  reportName="MIS Delayed Payment Report"
  reportType="MISDPHTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    ps=PanchayatStat.objects.filter(panchayat=pobj.panchayat,finyear=finyear).first()
    if ps.zeroMusters == False:
      fullFinYear=getFullFinYear(finyear)
      key="%s_%s" % (pobj.stateCode,finyear)
      logger.info(key)
      if key in delayURLs:
        logger.info(delayURLs[key])
        stateURL=delayURLs[key]
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

def downloadDelayedPayment(logger,pobj):
  error=None
  reportType="delayedPaymentHTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    isUpdated=isReportUpdated(logger,pobj,finyear,reportType)
    
    if isUpdated == False:
      fullFinYear=getFullFinYear(finyear)
      blockURL="http://%s/netnrega/Progofficer/PoIndexFrame.aspx?flag_debited=D&lflag=eng&District_Code=%s&district_name=%s&state_name=%s&state_Code=%s&finyear=%s&check=1&block_name=%s&Block_Code=%s" % (pobj.crawlIP,pobj.districtCode,pobj.districtName,pobj.stateName,pobj.stateCode,fullFinYear,pobj.blockName,pobj.blockCode)
      r=requests.get(blockURL)
      logger.info(r.status_code)
      pdplinkArray=[]
      if r.status_code==200:
        logger.info("downloaded Successfully")
        blockhtml=r.content
        blocksoup=BeautifulSoup(blockhtml,"lxml")
        allLinks=blocksoup.find_all("a", href=re.compile("delayed_payment.aspx"))
        dpLink=None
        for a in allLinks:
          myLink=a['href'].lstrip("../")
          dpLink="http://%s/netnrega/%s" % (pobj.crawlIP,myLink)
        if dpLink is not None:
          r1=requests.get(dpLink)
          if r1.status_code==200:
            dphtml=r1.content
            dpsoup=BeautifulSoup(dphtml,"lxml")
            dpLinks=dpsoup.find_all("a", href=re.compile("delayed_pay_detail.aspx"))
            for eachLink in dpLinks:
              if pobj.panchayatCode in str(eachLink):
                pdplink="http://%s/netnrega/%s" % (pobj.crawlIP,eachLink['href'])
                logger.info(pdplink)
                pdplinkArray.append(pdplink)
          else:
            error="could not open delay payment page"
        else:
          error="could not locate delay payment link"
      else:
        error="cannot open block page"

      outhtml=''
      title="Delayed Payment state:%s District:%s block:%s panchayat: %s finyear : %s  " % (pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,fullFinYear)
      if error is None:
        for url in pdplinkArray:
          r2=requests.get(url)
          if r2.status_code == 200:
            myhtml=r2.content
            logger.info(url)
            error1,myTable=validateDPReport(logger,pobj,myhtml)
            #logger.info(error1)
            if error1 is not None:
              error=error1
            else:
              outhtml+=stripTableAttributes(myTable,"myTable")

      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      processDPReport(logger,pobj,outhtml,finyear)
      try:
        outhtml=outhtml.encode("UTF-8")
      except:
        outhtml=outhtml
      filename="delayedPayment_%s_%s_%s.html" % (pobj.panchayatSlug,pobj.panchayatCode,finyear)
      filepath=pobj.panchayatFilepath.replace("filename",filename)
      savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath)
  return error

def processMISDPReport(logger,pobj):
  reportType="MISDPHTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
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
         

  
def processDPReport(logger,pobj,outhtml,finyear):
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  htmlsoup=BeautifulSoup(outhtml,"lxml")
  tables=htmlsoup.findAll("table")
  for myTable in tables:
    rows=myTable.findAll('tr')
    for row in rows:
      if jobcardPrefix in str(row): 
        cols=row.findAll('td')
        jobcard=cols[1].text.lstrip().rstrip()
        name=cols[3].text.lstrip().rstrip()
        musterno=cols[5].text.lstrip().rstrip()
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
        dp.save()
         

def validateDPReport(logger,pobj,myhtml):
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  error="table not found"
  myTable=None
  tables=htmlsoup.findAll("table")
  for eachTable in tables:
    if jobcardPrefix in str(eachTable):
      myTable=eachTable
      error=None
  return error,myTable

def validateFTOHTML(logger,pobj,myhtml):
  jobcardPrefix="%s-" % (pobj.stateShortCode)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  error="table not found"
  myTable=None
  tables=htmlsoup.findAll("table")
  for eachTable in tables:
    if jobcardPrefix in str(eachTable):
      myTable=eachTable
      error=None
  return error,myTable

def downloadFTO(logger,pobj,obj):
  error=None
  s3url=None
  finyear=obj.finyear
  fullfinyear=getFullFinYear(finyear)
  r=requests.get(obj.ftoURL)
  if r.status_code==200:
    #logger.info("Downloaded Successfully")
    myhtml=r.content
    error,myTable=validateFTOHTML(logger,pobj,myhtml)
    if error is None:  
      outhtml=''
      outhtml+=getCenterAlignedHeading("FTO Detail Table")
      outhtml+=stripTableAttributes(myTable,"myTable")
      title="FTO: %s state:%s District:%s block:%s  " % (obj.code,pobj.stateName,pobj.districtName,pobj.blockName)
      #logger.info(wdDict)
      outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
      #processFTO(logger,outhtml,pobj,obj)
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

def processRejectedPayment(logger,pobj):
  error=None
  reportType="rejectedPaymentHTML"

  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    myhtml=None
    myReport=Report.objects.filter(block=pobj.block,finyear=finyear,reportType=reportType).first()
    if myReport is not None:
      r=requests.get(myReport.reportURL)
      if r.status_code == 200:
        myhtml=r.content
    if myhtml is None:
      error="Rejected Payment report not found for finyear %s " % (str(finyear))
    else:
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      myTable=htmlsoup.find("table",id="myTable")
      if myTable is not None:
        rows=myTable.findAll("tr")
        for row in rows:
          cols=row.findAll("td")
          ftoNo=cols[1].text.lstrip().rstrip()
          if pobj.stateShortCode not in ftoNo:
            logger.info(ftoNo)
      exit(0)
  return error
def processFTO(logger,pobj,obj):
  myhtml=None
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    logger.info("Processing FTO ")
    reMatchString="%s-" % (pobj.stateShortCode)
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
     #   logger.info(jobcard)
        wagelistNo=cols[6].text.lstrip().rstrip()
        wagelistCode="%s_%s_%s" % (pobj.blockCode,obj.finyear,wagelistNo)
    #    logger.info(wagelistCode)
        try:
          jobcardObj=pobj.codeObjDict[jobcard]
        except:
          jobcardObj=None
#          logger.info("jobcard not found")
        try:
          wagelistObj=pobj.codeObjDict[wagelistCode]
        except:
          wagelistObj=None
        transactionDate=getDateObj(cols[4].text.lstrip().rstrip())
        processDate=getDateObj(cols[13].text.lstrip().rstrip())
        primaryAccountHolder=cols[7].text.lstrip().rstrip()
        status=cols[12].text.lstrip().rstrip()
        ftoIndex=cols[0].text.lstrip().rstrip()
        rejectionReason=cols[16].text.lstrip().rstrip()
        creditedAmount=cols[11].text.lstrip().rstrip()
        try:
          ft=FTOTransaction.objects.create(fto=obj,ftoIndex=ftoIndex)
        except:
          ft=FTOTransaction.objects.filter(fto=obj,ftoIndex=ftoIndex).first()
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
        ft.save()

def downloadRejectedPayment(logger,pobj):
  error=None
  reportType="rejectedPaymentHTML"
  reportName="Rejected Payment HTML"
  startFinYear=pobj.startFinYear
  endFinYear=getCurrentFinYear()
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    isUpdated=isReportUpdated(logger,pobj,finyear,reportType)
    if isUpdated==False:
      finyear=str(finyear)
      fullFinYear=getFullFinYear(finyear)
      url="http://%s/netnrega/FTO/FTOReport.aspx?page=d&mode=B&lflag=&flg=W&state_name=%s&state_code=%s&district_name=%s&district_code=%s&fin_year=%s&dstyp=B&source=national&" %(pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,fullFinYear)
      logger.info(url)
      r=requests.get(url)
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      allLinks=htmlsoup.find_all("a", href=re.compile("typ=R"))
      for a in allLinks:
        if pobj.blockCode in a['href']:
          rejectedLink="http://%s/netnrega/FTO/%s" % (pobj.crawlIP,a['href'])
      logger.info("Rejected Payment link is %s " % (rejectedLink))
      r=requests.get(rejectedLink)
      myhtml=r.content
      jobcardPrefix="%s%s" % (pobj.stateShortCode,pobj.stateCode)
      error=validateAndSave(logger,pobj,myhtml,reportName,reportType,finyear,locationType='block',jobcardPrefix=jobcardPrefix)
      return
      error,rejectedTable=validateRejectedHTML(logger,pobj,myhtml)
      if error is None:
         
        outhtml=''
        outhtml+=getCenterAlignedHeading("Rejected Table Details")
        baseURL="http://%s/netnrega/FTO/" % (pobj.crawlIP)
        outhtml+=stripTableAttributesPreserveLinks(rejectedTable,"myTable",baseURL)
        title="Rejected Details state:%s District:%s block:%s  " % (pobj.stateName,pobj.districtName,pobj.blockName)
        outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
        try:
          outhtml=outhtml.encode("UTF-8")
        except:
          outhtml=outhtml
        filename="rejectedDetails_%s_%s.html" % (finyear,pobj.blockName)
        filepath="%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,"DATA","NICREPORTS",filename)
        contentType="text/html"
        savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath,locationType='block',contentType=contentType)
  return error
#      s3url=uploadReportAmazon(filepath,outhtml,contentType)
#      logger.info(s3url)
      
def validateRejectedHTML(logger,pobj,myhtml):
  error="Rejected Table not Found"
  rejectedTable=None
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  myTables=htmlsoup.findAll("table")
  jobcardPrefix="%s%s" % (pobj.stateShortCode,pobj.stateCode)
  for myTable in myTables:
    if jobcardPrefix in str(myTable):
      rejectedTable=myTable
      error=None
  return error,rejectedTable
# if error is  None:
#   rows=rejectedTable.findAll('tr')
#   for row in rows:
#     if jobcardPrefix in str(row):
#       cols=row.findAll('td')
#       ftoNo=cols[1].text.lstrip().rstrip()
#       referenceNo=cols[2].text.lstrip().rstrip()
#       a=cols[2].find('a')
#       relativeRefLink=a['href']
#       refLink="http://%s/netnrega/FTO/%s" % (pobj.crawlIP,relativeRefLink)
#       logger.info(refLink)
#       r=requests.get(refLink)
#       refTable=None
#       if r.status_code == 200:
#         refhtml=r.content
#         refsoup=BeautifulSoup(refhtml,"lxml")
#         for table in resoup.findAll('table'):
#           if pobj.jobcardPrefix in str(table):
#             refTable=table
#         if refTable is not None:
#           rows=refTable.findAll("tr")
#           for row in rows:
#             if pobj.jobcardPrefix in str(row):
#               logger.info("validRow")

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

def createWorkPaymentReportAP(logger,pobj,finyear):
  finyear=str(finyear)
  f = BytesIO()
  f.write(u'\ufeff'.encode('utf8'))
  w = csv.writer(f, encoding='utf-8-sig',delimiter=',')
  reportType="workPaymentAP"
  a=[]
  a.extend(["panchayat","tjobcard","heafOfFamily","caste","applicantNo","applicantName","workCode","workName","musterNo","dateTo","daysWorked","accountNo","payorderNo","payorderDate","epayorderno","epayorderDate","payingAgencyDate","creditedDate","disbursedDate","paymentMode","payOrdeAmount","disbursedAmount"])
  w.writerow(a)
  workRecords=APWorkPayment.objects.filter(jobcard__panchayat=pobj.panchayat,finyear=finyear).order_by("jobcard__tjobcard","epayorderDate")
  logger.info("Total Work Records: %s " %str(len(workRecords)))
  for wd in workRecords:
    if wd.jobcard is not None:
      panchayatName=wd.jobcard.panchayat.name
    tjobcard1="~%s" % (wd.jobcard.tjobcard)
    a=[]
    a.extend([panchayatName,tjobcard1,wd.jobcard.headOfHousehold,wd.jobcard.caste,str(wd.applicantNo),wd.name,wd.workCode,wd.workName,wd.musterNo,str(wd.dateTo),str(wd.daysWorked),wd.accountNo,wd.payorderNo,str(wd.payorderDate),wd.epayorderNo,str(wd.epayorderDate),str(wd.payingAgencyDate),str(wd.creditedDate),str(wd.disbursedDate),wd.modeOfPayment,str(wd.payorderAmount),str(wd.disbursedAmount),str(wd.id)])
    w.writerow(a)
  f.seek(0)
  outcsv=f.getvalue()
  filename=pobj.panchayatSlug+"_"+str(finyear)+"_wpAP.csv"
  filepath=pobj.panchayatFilepath.replace("filename",filename)
  contentType="text/csv"
  savePanchayatReport(logger,pobj,finyear,reportType,outcsv,filepath,contentType=contentType)


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
      filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",pobj.stateSlug,pobj.districtSlug,pobj.blockSlug,pobj.pslug,"DATA","JOBCARDS",filename)
      contentType="text/html"
      s3url=uploadReportAmazon(filepath,outhtml,contentType)
      logger.info(s3url)
    obj=Jobcard.objects.filter(id=obj.id).first()
    updateObjectDownload(logger,obj,error,s3url)

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

def validateDataTelanganaJobcardRegister(logger,myhtml):
  error=None
  myTable=None
  htmlsoup=BeautifulSoup(myhtml,"html.parser")
  myTable=htmlsoup.find('table',id='sortable')
  if myTable is None:
    logger.debug("Table not found")
    error="Table not found"
  return error,myTable




libtechMethodNames={
       'downloadJobcardtelangana'   : telanganaJobcardDownload,
       'downloadMuster'             : downloadMusterNew,
       'downloadWagelist'           : downloadWagelist,
       'downloadFTO'                : downloadFTO,
       'processJobcardtelangana'    : telanganaJobcardProcess,
       'processMuster'              : processMuster,
       'processWagelist'            : processWagelist,
       'processFTO'                 : processFTO,

        }
def crawlFullPanchayatTelangana(logger,pobj):
  pinfo=PanchayatCrawlInfo.objects.filter(panchayat__code=pobj.panchayatCode).first()
  if pinfo is None:
    pinfo=PanchayatCrawlInfo.objects.create(panchayat=pobj.panchayat)
  cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
  error=None
  logger.info("Strating to Download Panchayat %s CQID %s " % (cq.panchayat.name,str(cq.id)))
 
  if pobj.downloadStage=="init":
    downloadStage="downloadTelangana1"
    error=saveJobcardRegisterTelangana(logger,pobj)
    if error is not None:
      return error
    error=processJobcardRegisterTelangana(logger,pobj)
    if error is not None:
      return error
    cq.downloadStage=downloadStage
    cq.save()
    return error

  elif pobj.downloadStage == "downloadTelangana1":
    downloadStage="Complete"

    modelName="Jobcard"
    total,pending,downloaded=objectDownloadMain(logger,pobj,modelName)
    if total != 0:
      downloadAccuracy=int(downloaded*100/total)
      if downloadAccuracy < downloadToleranceDict[modelName]:
        error="Unable to download %s %s out of %s  finyear %s" % (modelName,str(total-downloaded),str(total),finyear)
        return error 

    objectProcessMain(logger,pobj,modelName)
    for finyear in range(int(pobj.startFinYear),int(pobj.endFinYear)+1):
      createWorkPaymentReportAP(logger,pobj,finyear)
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.downloadStage=downloadStage
    cq.save()
    return error

  elif pobj.downloadStage == "processData1":
    downloadStage="Complete"
    modelName="Jobcard"
    objectProcessMain(logger,pobj,modelName)
    for finyear in range(int(pobj.startFinYear),int(pobj.endFinYear)+1):
      createWorkPaymentReportAP(logger,pobj,finyear)
    
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.downloadStage=downloadStage
    cq.save()
    return error
def crawlFullPanchayat(logger,pobj):
  pinfo=PanchayatCrawlInfo.objects.filter(panchayat__code=pobj.panchayatCode).first()
  if pinfo is None:
    pinfo=PanchayatCrawlInfo.objects.create(panchayat=pobj.panchayat)
  cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
  error=None
  logger.info("Strating to Download Panchayat %s CQID %s " % (cq.panchayat.name,str(cq.id)))

  if pobj.downloadStage=="init":
    downloadStage="downloadData1"

    error=jobcardRegister(logger,pobj)
    if error is not None:
      return error

    pobj.crawlerDict['Village']=pobj.villageDict
    pobj.crawlerDict['Jobcard'] = pobj.jobcardDict
    pobj.crawlerDict['Worker'] = pobj.workerDict

    error=musterWagelistCrawl(logger,pobj)
    if error is not None:
      return error
    pobj.crawlerDict['Muster']=pobj.musterDict
    pobj.crawlerDict['Wagelist']=pobj.wagelistDict
   # error=crawlFTO(logger,pobj)
   # if error is not None:
   #   return error
   # pobj.crawlerDict['FTO']=pobj.ftoDict
    contentType="application/json"
    filedata=json.dumps(pobj.crawlerDict)
    s3INSURL=uploadReportAmazon(pobj.instanceJSONPath,filedata,contentType)
    cq.instanceJSONURL=s3INSURL
    cq.downloadStage=downloadStage
    cq.save()
    pinfo.instanceJSONURL=s3INSURL
    pinfo.save()
    return error

  elif pobj.downloadStage == "downloadData1":
    error=None
    downloadStage="downloadData2"
    createCodeObjDict(logger,pobj)

    error=downloadWorkDemand(logger,pobj)
    if error is not None:
      return error
    processWorkDemand(logger,pobj)

    error=downloadJobcardStat(logger,pobj)
    if error is not None:
      return error
    processJobcardStat(logger,pobj)

   #error=downloadRejectedPayment(logger,pobj)
   #if error is not None:
   #  return error

    for finyear in range(int(pobj.startFinYear),int(pobj.endFinYear)+1):
      finyear=str(finyear)
      createJobcardStatReport(logger,pobj,finyear)
    error=downloadMISDPReport(logger,pobj)
    if error is not None:
      return error
    processMISDPReport(logger,pobj)
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.downloadStage=downloadStage
    cq.save()
    return error
  
  elif pobj.downloadStage == "downloadData2":
    error=None
    downloadStage="downloadData3"
    createCodeObjDict(logger,pobj)
    #modelArray=["Muster","Wagelist","FTO"]
    modelArray=["Muster"]
    downloadAccuracy=None
    for modelName in modelArray:
      total,pending,downloaded=objectDownloadMain(logger,pobj,modelName)
      if total != 0:
        downloadAccuracy=int(downloaded*100/total)
        if downloadAccuracy < downloadToleranceDict[modelName]:
          error="Unable to download %s %s out of %s  " % (modelName,str(total-downloaded),str(total))
          return error 
    modelArray=["Muster"]
    for modelName in modelArray:
      objectProcessMain(logger,pobj,modelName)
    error=computeJobcardStat(logger,pobj)
    accuracy=generateReportsComputeStats(logger,pobj)
    if error is not None:
      return error
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.downloadStage=downloadStage
    cq.accuracy=accuracy
    cq.musterDownloadAccuracy=downloadAccuracy
    cq.save()
    return error

  if pobj.downloadStage=="processData5":
    error=None
    downloadStage="downloadData2"
    createCodeObjDict(logger,pobj)
    processMISDPReport(logger,pobj)
    processJobcardStat(logger,pobj)
    processWorkDemand(logger,pobj)
    modelArray=["FTO","Wagelist"]
    for modelName in modelArray:
      objectProcessMain(logger,pobj,modelName)

    createCodeObjDict(logger,pobj,codeType="transaction")
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.downloadStage=downloadStage
    cq.accuracy=accuracy
    cq.save()
    return None

  elif pobj.downloadStage == "downloadData2":
    error=None
    downloadStage="Complete"
    createCodeObjDict(logger,pobj)
    pobj.jobcardStatDict=makehash()
    error=musterDownloadMain(logger,pobj)
    if error is not None:
      return error
    accuracy=generateReportsComputeStats(logger,pobj)
   
    contentType="application/json"
    filedata=json.dumps(pobj.workDetailDict)
    s3INSURL=uploadReportAmazon(pobj.workDetailJSONPath,filedata,contentType)
    cq=CrawlQueue.objects.filter(id=pobj.crawlQueueID).first()
    cq.workDetailJSONURL=s3INSURL
    cq.downloadStage=downloadStage
    cq.accuracy=accuracy
    cq.downloadStage=downloadStage
    cq.save()
    pinfo.workDetailJSONURL=s3INSURL
    pinfo.accuracy=accuracy
    pinfo.save()
    logger.info("Finishged CrawlQueue ID %s " % str(cq.id))
   
    return error

     
'''
Working Wagelist URl
http://mnregaweb4.nic.in/netnrega/srch_wg_dtl.aspx?state_code=&district_code=3314&state_name=CHHATTISGARH&district_name=JANJGIR-CHAMPA&block_code=3314004&wg_no=3314004WL136524&short_name=CH&fin_year=2017-2018&mode=wg

#Working URL

#OLD URL
http://mnregaweb2.nic.in/netnrega/citizen_html/musternew.aspx?state_name=CHHATTISGARH&district_name=JANJGIR-CHAMPA&block_name=JAIJAIPUR&panchayat_name=RAIPURA&workcode=3314004002/IF/IAY/538638&panchayat_code=3314004002&msrno=25036&finyear=2017-2018&dtfrm=21/03/2018&dtto=27/03/2018&wn=Construction+of+IAY+House+-IAY+REG.+NO.+CH1397030&id=1


#Working Muster URL for Jharkhand
http://nregasp2.nic.in/citizen_html/musternew.asp?lflag=&id=1&state_name=JHARKHAND&district_name=PALAMU&block_name=CHHATARPUR&panchayat_name=MASHIHANI&block_code=3405008&msrno=10439&finyear=2018-2019&workcode=3405008021%2fIF%2fIAY%2f346256&dtfrm=01%2f11%2f2018&dtto=07%2f11%2f2018&wn=Construction+of+IAY+House+-IAY+REG.+NO.+JH1425859&
'''

