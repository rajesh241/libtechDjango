import os
from bs4 import BeautifulSoup
import re
import random
import sys
import time
import json
import requests
import boto3
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
sys.path.insert(0, rootDir)
from config.defines import djangoSettings,logDir
from nrega.crawler.commons.nregaSettings import startFinYear,panchayatCrawlThreshold,panchayatRetryThreshold,telanganaStateCode,panchayatAttemptRetryThreshold,apStateCode,crawlRetryThreshold,crawlProcessTimeThreshold,crawlerTimeThreshold
#from crawlFunctions import crawlPanchayat,crawlPanchayatTelangana,libtechCrawler

from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,table2csv,getFullFinYear,loggerFetch,csv2Table,getCenterAlignedHeading
from nrega import models  as nregamodels
from nrega.models import Jobcard
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()
from nrega.crawler.code.nregaDownload import crawlerMain,PanchayatCrawler,computePanchayatStat,downloadMuster,downloadWagelist,createCodeObjDict,createDetailWorkPaymentReport,telanganaJobcardDownload,telanganaJobcardProcess,createWorkPaymentReportAP,processRejectedPayment,downloadRejectedPayment,processWagelist,processMuster,downloadMISDPReport,processMISDPReport,downloadJobcardStat,processJobcardStat,jobcardRegister,objectDownloadMain,downloadMusterNew,processWorkDemand
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,PanchayatStat
from nrega.crawler.code.commons import uploadReportAmazon
p={
'36':'1',
'06':'1',
'25':'1',
'19':'1',
'10':'20',
'08':'1',
'07':'1',
'01':'11',
'32':'150',
'35':'30',
'31':'140',
'30':'12',
'29':'50',
'28':'40',
'27':'200',
'26':'60',
'24':'130',
'23':'13',
'22':'14',
'21':'15',
'20':'16',
'18':'70',
'17':'120',
'16':'160',
'15':'110',
'34':'190',
'14':'50',
'13':'80',
'12':'90',
'11':'100',
'33':'170',
'05':'180',
'04':'40',
'03':'17',
'02':'1',
}
def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This implements the crawl State Machine')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-e', '--execute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-p', '--populate', help='Populate CrawlQueue', required=False,action='store_const', const=1)
  parser.add_argument('-se', '--singleExecute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-csm', '--crawlStateMachine', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=2)
  parser.add_argument('-d', '--debug', help='Debug Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--test', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-csr', '--crawlStatusReport', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t1', '--test1', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-ti', '--testInput', help='Test Input', required=False)

  args = vars(parser.parse_args())
  return args

def getCrawlStatus():
  myhtml=None
  outhtml=''
  outhtml+=getCenterAlignedHeading("Crawl status")
  cqs=CrawlQueue.objects.all().values("panchayat__block__district__state__slug","priority","panchayat__block__district__state__crawlIP").annotate(pCount=Count("pk")).order_by("-priority")
  s="state,crawlIP,priority,total,init,downloadJobcard,downloadJobcard1,downloadData1,downloadData2,downloadData3,downloadData4\n"
  for cq in cqs:
    stateSlug=cq["panchayat__block__district__state__slug"]
    crawlIP=cq["panchayat__block__district__state__crawlIP"]
    priority=cq['priority']
    total=cq["pCount"]
    stats=CrawlQueue.objects.filter(panchayat__block__district__state__slug=stateSlug).values("downloadStage").annotate(pCount=Count("pk"))
    counts={}
    for stat in stats:
      downloadStage=stat["downloadStage"]
      counts[downloadStage]=stat["pCount"]
    init=counts.get("init",0)
    downloadJobcards=counts.get("downloadJobcards",0)
    downloadJobcards1=counts.get("downloadJobcards1",0)
    downloadData1=counts.get("downloadData1",0)
    downloadData2=counts.get("downloadData2",0)
    downloadData3=counts.get("downloadData3",0)
    downloadData4=counts.get("downloadData4",0)
    s+="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (stateSlug,crawlIP,priority,str(total),str(init),str(downloadJobcards),str(downloadJobcards1),str(downloadData1),str(downloadData2),str(downloadData3),str(downloadData4))
  outhtml+=csv2Table(s)
  
  s="ID,panchayat,downoadStage,processName,modfified\n"
  cqs=CrawlQueue.objects.filter(inProgress=True)
  for cq in cqs:
    s+="%s,%s,%s,%s,%s\n" % (str(cq.id),str(cq.panchayat),cq.downloadStage,cq.processName,str(cq.modified))
  outhtml+=csv2Table(s)
  return outhtml
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  totalSamples=12
  myLibtechTag=LibtechTag.objects.filter(id=1).first()
  additional=LibtechTag.objects.filter(id=2).first()
  jan2019=LibtechTag.objects.filter(id=4).first()
  ltArray=[myLibtechTag]
  ltArray1=[jan2019]
  if args['populate']:
    states=State.objects.filter(isNIC=True)
    for eachState in states:
      panchayats=Panchayat.objects.filter(block__district__state=eachState,libtechTag__in=ltArray1)
      for eachPanchayat in panchayats:
        logger.info(eachPanchayat)
        cq=CrawlQueue.objects.filter(panchayat=eachPanchayat).first()
        if cq is None:
          cq=CrawlQueue.objects.create(panchayat=eachPanchayat)
        if eachState.code in p:
          priority=p[eachState.code]
        else:
          priority=0
        cq.priority=priority
        cq.save()
  if args['test']:
    districts=District.objects.all().order_by("code")
    s="state,district,code,sampledPanchayats\n"
    for d in districts:
      objs=Panchayat.objects.filter(block__district=d,libtechTag__in=ltArray)
      for obj in objs:
        obj.libtechTag.add(jan2019)
        obj.save()
      sampledPanchayats=len(objs)
      pending=0
      if sampledPanchayats < totalSamples:
        pending=totalSamples-sampledPanchayats
      objs=Panchayat.objects.filter( Q(block__district=d) & (~Q(libtechTag__in=ltArray)))
      if len(objs) >= pending:
        selPanchayats=random.sample(list(objs), pending)    
        for eachPanchayat in selPanchayats:
          eachPanchayat.libtechTag.add(jan2019)
          eachPanchayat.libtechTag.add(additional)
          eachPanchayat.save()
      objs=Panchayat.objects.filter(block__district=d,libtechTag__in=ltArray1)
      newTotal=len(objs)

      s+="%s,%s,%s,%s,%s\n" % (d.state.slug,d.slug,d.code,str(sampledPanchayats),str(newTotal))
    filename="samplesPerDistrict.csv"
    filepath="%s/%s/%s" % ("misc","jan2019PressRelease",filename)
    contentType="text/csv"
    s3url=uploadReportAmazon(filepath,s,contentType)
    logger.info(s3url)
    with open("/tmp/samples.csv","w") as f:
      f.write(s)
  if args['crawlStatusReport']:
    ltArray=[]
    myTag=LibtechTag.objects.filter(id=4).first()
    ltArray.append(myTag)
    s=''
    stateCode='34'
    i=1
    #cqs=CrawlQueue.objects.filter(Q(panchayat__libtechTag__in=ltArray) & Q ( Q(downloadStage= "downloadData4") | Q(downloadStage="downloadData3") | Q(downloadStage ="downloadData2"))).order_by("panchayat__block__district__state__slug")
    cqs=CrawlQueue.objects.filter(Q(panchayat__libtechTag__in=ltArray)).order_by("panchayat__block__district__state__slug")
    s="srno,cqID,state,district,block,panchayat,pcode,accuracy,demand18,provi18,ufd18,demand19,provi19,ufd19\n"
    s1="srno,cqID,state,district,block,panchayat,pcode,accuracy,zeroMuster18,demand18,provi18,ufd18,zeroMuster19,demand19,provi19,ufd19\n"
    for cq in cqs:
      pobj=PanchayatCrawler(cq.id)
      ps=PanchayatStat.objects.filter(panchayat=cq.panchayat,finyear='18').first()
      daysDemanded18=ps.nicDaysDemanded
      nicDaysProvided18=ps.nicWorkDays
      zeroMuster18=ps.zeroMusters
      ps=PanchayatStat.objects.filter(panchayat=cq.panchayat,finyear='19').first()
      daysDemanded19=ps.nicDaysDemanded
      nicDaysProvided19=ps.nicWorkDays
      zeroMuster19=ps.zeroMusters
      doInsert=True
      if ( (daysDemanded18 is not None) and (daysDemanded18 != 0) and (nicDaysProvided18 is not None)):
        ufd18=int((daysDemanded18-nicDaysProvided18)*100/daysDemanded18)
      else:
        ufd18=None
        doInsert=False
      if ( (daysDemanded19 is not None) and (daysDemanded19 != 0) and (nicDaysProvided19 is not None)):
        ufd19=int((daysDemanded19-nicDaysProvided19)*100/daysDemanded19)
      else:
        ufd19=None
        doInsert=False
      line="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(i),str(cq.id),pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.panchayatCode,cq.accuracy,str(daysDemanded18),str(nicDaysProvided18),str(ufd18),str(daysDemanded19),str(nicDaysProvided19),str(ufd19))
      line1="%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (str(i),str(cq.id),pobj.stateName,pobj.districtName,pobj.blockName,pobj.panchayatName,pobj.panchayatCode,cq.accuracy,str(zeroMuster18),str(daysDemanded18),str(nicDaysProvided18),str(ufd18),str(zeroMuster19),str(daysDemanded19),str(nicDaysProvided19),str(ufd19))
      if "None" in line:
        s1+=line1
      else:
        i=i+1
        s+=line
      logger.info(cq.panchayat)
    with open("/tmp/crawlErrors.csv","w") as f:
      f.write(s1)
    filename="demandAnanlysis.csv"
    filepath="%s/%s/%s" % ("misc","jan2019PressRelease",filename)
    contentType="text/csv"
    with open("/tmp/delayAnalysis.csv","w") as f:
      f.write(s)
    s3url=uploadReportAmazon(filepath,s,contentType)
    logger.info(s3url)
    filename="crawlErrors.csv"
    filepath="%s/%s/%s" % ("misc","jan2019PressRelease",filename)
    contentType="text/csv"
    s3url=uploadReportAmazon(filepath,s1,contentType)
    logger.info(s3url)
  if args['test1']:
    myhtml=getCrawlStatus(logger)
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
