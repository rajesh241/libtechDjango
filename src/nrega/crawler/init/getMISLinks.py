import os
from os import listdir
from os.path import isfile, join
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

from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,table2csv,getFullFinYear,loggerFetch
from nrega import models  as nregamodels
from nrega.models import Jobcard
from nrega.crawler.code.commons import savePanchayatReport,uploadReportAmazon,getjcNumber
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.contrib.auth.models import User
from django.db.models import F,Q,Sum,Count
from django.db import models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()
from nrega.crawler.code.nregaDownload import crawlerMain,PanchayatCrawler,computePanchayatStat,downloadMuster,downloadWagelist,createCodeObjDict,createDetailWorkPaymentReport,telanganaJobcardDownload,telanganaJobcardProcess,createWorkPaymentReportAP,processRejectedPayment,downloadRejectedPayment,processWagelist,processMuster,downloadMISDPReport,processMISDPReport,downloadJobcardStat,processJobcardStat,jobcardRegister,objectDownloadMain,downloadMusterNew,processWorkDemand,downloadWorkDemand,downloadJobcardStat,fetchOldMuster,objectProcessMain,computeJobcardStat,downloadJobcard,processJobcard
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,DemandWorkDetail,MISReportURL


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
  parser.add_argument('-t1', '--test1', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-ti', '--testInput', help='Test Input', required=False)
  parser.add_argument('-ti2', '--testInput2', help='Test Input', required=False)

  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['execute']:
    urlPrefix="http://mnregaweb4.nic.in/netnrega/"
    objs=MISReportURL.objects.filter(state__code='34',finyear='18')
    objs=MISReportURL.objects.all()
    for obj in objs:
      logger.info(obj.state.slug)
      url=obj.contentFileURL
      if url is not None:
        r=requests.get(url)
        if r.status_code==200:
          myhtml=r.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          a=htmlsoup.find("a", href=re.compile("demregister.aspx"))
          demandLink="%s%s" % (urlPrefix,a['href'])
          logger.info(demandLink)
          a=htmlsoup.find("a", href=re.compile("FTOReport.aspx"))
          ftoLink="%s%s" % (urlPrefix,a['href'])
          logger.info(ftoLink)
          a=htmlsoup.find("a", href=re.compile("delay_comp.aspx"))
          delayLink="%s%s" % (urlPrefix,a['href'])
          logger.info(delayLink)
          obj.demandRegisterURL=demandLink
          obj.ftoURL=ftoLink
          obj.delayPaymentURL=delayLink
          obj.save()
  if args['test']:
    logger.info("Running Tests")
    mypath="./misPages/"
    allFiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for eachFile in allFiles:
      stateCode=eachFile[:2]
      finyear=eachFile[-7:][:2]
      myState=State.objects.filter(code=stateCode).first()
      if myState is not None:
        mru=MISReportURL.objects.filter(state=myState,finyear=finyear).first()
        if mru is None:
          mru=MISReportURL.objects.create(state=myState,finyear=finyear)
        
        with open("%s/%s" % (mypath,eachFile)) as fp:
          myhtml=fp.read()
        filename="%s_%s.html" % (myState.slug,finyear)
        filepath="%s/%s/%s" % ("nrega","MISReportPages",filename)
        contentType="text/html"
        logger.info(filename)
        s3url=uploadReportAmazon(filepath,myhtml,contentType)
        mru.contentFileURL=s3url
        mru.save()
        logger.info(s3url)
        
        
if __name__ == '__main__':
  main()
