import os
import unicodecsv as csv
from bs4 import BeautifulSoup
import re
import random
import sys
import time
import json
import requests
import boto3
import datetime
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
sys.path.insert(0, rootDir)
from config.defines import djangoSettings,logDir
from nrega.crawler.commons.nregaSettings import startFinYear,panchayatCrawlThreshold,panchayatRetryThreshold,telanganaStateCode,panchayatAttemptRetryThreshold,apStateCode,crawlRetryThreshold,crawlProcessTimeThreshold,crawlerTimeThreshold
#from crawlFunctions import crawlPanchayat,crawlPanchayatTelangana,libtechCrawler

from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,table2csv,getFullFinYear,loggerFetch,getDateObj,getCenterAlignedHeading,stripTableAttributesPreserveLinks
from nrega import models  as nregamodels
from nrega.crawler.code import nregaDownload as nregaDownloadFunctions
from nrega.crawler.code import crawlerFunctions as crawlerFunctions
from nrega.models import Jobcard
from commons import savePanchayatReport,uploadReportAmazon,getjcNumber,isReportUpdated
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,DemandWorkDetail,MISReportURL,PanchayatStat,RejectedPayment,FTOTransaction,WorkDetail,CrawlState,CrawlRequest,BlockStat,Location
from crawlerFunctions import crawlerMain
#from nregaDownload import crawlerMain,PanchayatCrawler,computePanchayatStat,downloadMuster,downloadWagelist,createCodeObjDict,createDetailWorkPaymentReport,telanganaJobcardDownload,telanganaJobcardProcess,createWorkPaymentReportAP,processRejectedPayment,downloadRejectedPayment,processWagelist,processMuster,downloadMISDPReport,processMISDPReport,downloadJobcardStat,processJobcardStat,jobcardRegister,objectDownloadMain,downloadMusterNew,processWorkDemand,downloadWorkDemand,downloadJobcardStat,fetchOldMuster,objectProcessMain,computeJobcardStat,downloadJobcard,processJobcard,validateAndSave,getReportHTML,createWorkPaymentJSK,validateNICReport,updateObjectDownload,downloadWagelist,processWagelist,crawlFTORejectedPayment,processBlockRejectedPayment,matchTransactions,getFTOListURLs
from crawlerFunctions import processWagelist,createCodeObjDict,LocationObject,CrawlerObject

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This implements the crawl State Machine')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-e', '--execute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-d', '--debug', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-z', '--zip', help='Zipping the reports', required=False,action='store_const', const=1)
  parser.add_argument('-cqID', '--crawlQueueID', help='Manage Panchayat Crawl Queue', required=False)
  parser.add_argument('-objID', '--objID', help='Manage Panchayat Crawl Queue', required=False)
  parser.add_argument('-mn', '--modelName', help='Manage Panchayat Crawl Queue', required=False)
  parser.add_argument('-fn', '--functionName', help='Manage Panchayat Crawl Queue', required=False)
  parser.add_argument('-lc', '--locationCode', help='Location Code either block or panchayat', required=False)
  parser.add_argument('-st', '--sequenceType', help='SequenceType for the crawlRequest', required=False)
  parser.add_argument('-p', '--populate', help='Populate CrawlQueue', required=False,action='store_const', const=1)
  parser.add_argument('-se', '--singleExecute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-csm', '--crawlStateMachine', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=2)
  parser.add_argument('-t', '--test', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t1', '--test1', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-ti', '--testInput', help='Test Input', required=False)
  parser.add_argument('-sf', '--startFinYear', help='Test Input', required=False)
  parser.add_argument('-ef', '--endFinYear', help='Test Input', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input', required=False)
  parser.add_argument('-f', '--finyear', help='Test Input', required=False)

  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  logger.info("start processing")
  if args['debug']:
    cqID=args['crawlQueueID']
    objID=args['objID']
    modelName=args['modelName']
    funcName=args['functionName']
    finyear=args['finyear']
    locationCode=args['locationCode']
    cq=CrawlRequest.objects.filter(id=cqID).first()
    if cqID is not None:
      cobj=CrawlerObject(cq.id)
#    pobj=LocationObject(locationCode)
    pobj=Location.objects.filter(code=locationCode).first()
    #createCodeObjDict(logger,pobj)
    if modelName is not None:
      if funcName == "dumpDataCSV":
        logger.info("I amhere")
        getattr(crawlerFunctions,funcName)(logger,pobj,finyear=finyear,modelName=modelName)
      elif objID is not None:
        obj=getattr(nregamodels,modelName).objects.filter(id=objID).first()
        getattr(crawlerFunctions,funcName)(logger,pobj,obj)
      else:
        getattr(crawlerFunctions,funcName)(logger,pobj,modelName,finyear)
    elif finyear is not None:
      getattr(crawlerFunctions,funcName)(logger,pobj,finyear)
    else:
      getattr(crawlerFunctions,funcName)(logger,pobj)
  if args['zip']:
    reportType="workPaymentAP"
    blockCodes=["3614005","3614006","3614007","3614008"]
    for eachBlockCode in blockCodes:
      myReports=Report.objects.filter(panchayat__block__code=eachBlockCode,reportType=reportType)
      for eachReport in myReports:
        blockSlug=eachReport.panchayat.block.slug
        panchayatSlug=eachReport.panchayat.slug
        logger.info("Downloading %s-%s-%s" % (blockSlug,panchayatSlug,eachReport.finyear))
        filepath="/tmp/r/%s/" % (blockSlug)
        if not os.path.exists(filepath):
          os.makedirs(filepath)
        filename="%s/%s-workPaymentAP_%s.csv" % (filepath,panchayatSlug,eachReport.finyear)
        r=requests.get(eachReport.reportURL)
        if r.status_code == 200:
          mycsv=r.content
          with open(filename,"wb") as f:
            f.write(mycsv)
  if args['singleExecute']:
    cqID=args['testInput']
   #cq=CrawlRequest.objects.filter(id=cqID).first()
   #if cq is not  None:
   #  stateURL="http://nregasp2.nic.in/netnrega/homestciti.aspx?state_code=34&state_name=JHARKHAND" % (
    crawlerMain(logger,cqID)
  if args['test1']:
    lobjs=Location.objects.filter(locationType='state')
    for lobj in lobjs:
      logger.info(lobj.name)
      for finyear in range(int(17),int(19)+1):
        getattr(crawlerFunctions,"getFTOStat")(logger,lobj,finyear)
        getattr(crawlerFunctions,"processFTOStat")(logger,lobj,finyear)
    exit(0)
  if args['test']:
    funcName='downloadGlance'
    lobjs=Location.objects.filter(locationType = 'state')
    for lobj in lobjs:
      logger.info(lobj) 
      getattr(crawlerFunctions,funcName)(logger,lobj)
    exit(0)
   #lobj=Location.objects.filter(code='2708001').first()
   #getattr(crawlerFunctions,funcName)(logger,lobj)
   #exit(0)
    crs=CrawlRequest.objects.filter(crawlState__name="ddComplete",id__gte=92).order_by("id")
    for cr in crs:
      logger.info(cr.id)
      logger.info(cr.location)
      myBlocks=Location.objects.filter(locationType='block',parentLocation=cr.location)
      for eachBlock in myBlocks:
        getattr(crawlerFunctions,funcName)(logger,eachBlock)

        for finyear in range(int(17),int(19)+1):
          getattr(crawlerFunctions,'processBlockStat')(logger,eachBlock,finyear)
        myPanchayats=Location.objects.filter(locationType='panchayat',parentLocation=eachBlock)
        for eachPanchayat in myPanchayats:
          getattr(crawlerFunctions,funcName)(logger,eachPanchayat)


    exit(0)
    bs=BlockStat.objects.all()
    for obj in bs:
      obj.save()
    exit(0)
    myDistrict=District.objects.all()
    startFinYear='17'
    endFinYear='19'
    sequenceType='dd'
    for obj in myDistrict:
      cr=CrawlRequest.objects.create(district=obj,endFinYear=endFinYear,startFinYear=startFinYear,sequenceType=sequenceType)        
    exit(0)
    objs=Muster.objects.filter(panchayat__block__code='3401005',isDownloaded=True,allWorkerFound=False)
    i=0
    for obj in objs:
      i=i+1
      logger.info(i)
      logger.info(obj.id)
      cq=CrawlRequest.objects.filter(id=1).first()
      cobj=CrawlerObject(cq.id)
      pobj=LocationObject(cobj,code=obj.panchayat.code)
      createCodeObjDict(logger,pobj)
      funcName="processMuster"
      getattr(crawlerFunctions,funcName)(logger,pobj,obj)
    exit(0)
    transactionStartDate=datetime.datetime.strptime('15082017', "%d%m%Y").date()
    transactionEndDate=datetime.datetime.strptime('15082018', "%d%m%Y").date()
    cqID=8241
    pobj=PanchayatCrawler(cqID)
    finyear='18'
    s=''
    s+="wID,daysWorked,totalWage\n"
    wds=WorkDetail.objects.filter(Q(worker__oldID__isnull=False) & Q( Q(daysAllocated__gt=0,muster__dateTo__gte=transactionStartDate,muster__dateTo__lte=transactionEndDate))).values("worker__oldID").annotate(dsum=Sum('daysWorked'),tsum=Sum('totalWage'))
    for wd in wds:
      oldWorkerID=wd['worker__oldID']
      daysWorked=wd['dsum']
      totalWage=int(wd['tsum'])
      s+="%s,%s,%s\n" % (str(oldWorkerID),str(daysWorked),str(totalWage))
    with open("/tmp/aggregate.csv","w") as f:
      f.write(s)
    exit(0)
    wds=WorkDetail.objects.filter(Q(worker__oldID__isnull=False) & Q( Q(daysAllocated__gt=0,muster__dateTo__gte=transactionStartDate,muster__dateTo__lte=transactionEndDate) | Q(daysAllocated=0,workDemandDate__gte=transactionStartDate,workDemandDate__lte=transactionEndDate)))
    filename="test_22feb19_1"
    createDetailWorkPaymentReport(logger,pobj,finyear,reportObjs=wds,filename=filename)
    exit(0)
    myLibtechTag=LibtechTag.objects.filter(id=6).first()
    with open("/tmp/sampledWorkers1.csv") as fp:
      for line in fp:
        lineArray=line.lstrip().rstrip().split(",")
        if len(lineArray) == 3:
          oldID=lineArray[0]
          jobcard=lineArray[1]
          name=lineArray[2]
          myWorker=Worker.objects.filter(jobcard__jobcard=jobcard,name=name).first()
          if myWorker is None:
            logger.info("worker not found %s " % (str(line)))
          else:
            myWorker.oldID=oldID
            myWorker.libtechTag.add(myLibtechTag)
            logger.info(myWorker.id)
            myWorker.save()
  if args['populate']:
    endFinYear=getCurrentFinYear()
    if args['endFinYear']:
      endFinYear=args['endFinYear']
    if args['sequenceType']:
      sequenceType=args['sequenceType']
    else:
      sequenceType='default'
    if args['startFinYear'] is not None:
      startFinYear=args['startFinYear']
    else:
      startFinYear='18'
    code=args['locationCode']
    if len(code) == 10:
      obj=Panchayat.objects.filter(code=code).first()
      cr=CrawlRequest.objects.create(panchayat=obj,startFinYear=startFinYear,sequenceType=sequenceType)
    elif len(code) == 7:
      obj=Block.objects.filter(code=code).first()
      cr=CrawlRequest.objects.create(block=obj,startFinYear=startFinYear,sequenceType=sequenceType)        
    elif len(code) == 4:
      obj=District.objects.filter(code=code).first()
      cr=CrawlRequest.objects.create(district=obj,endFinYear=endFinYear,startFinYear=startFinYear,sequenceType=sequenceType)        
  logger.info("...END PROCESSING") 
  exit(0)
if __name__ == '__main__':
  main()
