import os
import pandas as pd
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
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,PanchayatStat,CrawlRequest,Info
from nrega.crawler.code.commons import uploadReportAmazon
from nrega.crawler.code.crawlerFunctions import crawlerMain,crawlFTORejectedPayment,dumpDataCSV
from nrega.crawler.code.crawlerFunctions import processWagelist,createCodeObjDict,LocationObject,CrawlerObject

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='This implements the crawl State Machine')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-e', '--execute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-p', '--populate', help='Populate CrawlQueue', required=False,action='store_const', const=1)
  parser.add_argument('-dr', '--downloadRejected', help='Populate CrawlQueue', required=False,action='store_const', const=1)
  parser.add_argument('-se', '--singleExecute', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-csm', '--crawlStateMachine', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=2)
  parser.add_argument('-d', '--debug', help='Debug Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--test', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-csr', '--crawlStatusReport', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-t1', '--test1', help='Manage Panchayat Crawl Queue', required=False,action='store_const', const=1)
  parser.add_argument('-ti', '--testInput', help='Test Input', required=False)

  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    code=args['testInput']
    blockCodes=["34","02","27","3403009","3406004","0203020","2721003"]
    df=pd.DataFrame(list(Info.objects.filter(location__code__in = blockCodes).values("location__englishName","slug","name","finyear","value")))
    df.to_csv("/tmp/glance.csv")
    logger.info(df.head())

  if args['downloadRejected']:
    ltTag=LibtechTag.objects.filter(id=7).first()
    ltArray=[]
    ltArray.append(ltTag)
    myBlocks=Block.objects.filter(libtechTag__in=ltArray)
    for eachBlock in myBlocks:
      cq=CrawlRequest.objects.filter(block=eachBlock).order_by("-id").first()
      if cq is not None:
        cobj=CrawlerObject(cq.id)
        pobj=LocationObject(cobj,code=eachBlock.code)
        for finyear in range(int(cobj.startFinYear),int(cobj.endFinYear)+1):
          finyear=str(finyear)
          error=crawlFTORejectedPayment(logger,pobj,finyear) 
          dumpDataCSV(logger,pobj,finyear=finyear,modelName="rejectedTransaction")
      
    exit(0)
    
    blockArray=[]
    with open("../init/jharkhandActivePanchayats.csv") as fp:
      for line in fp:
        pcode=line.lstrip().rstrip()
        logger.info(pcode)
        myPanchayat=Panchayat.objects.filter(code=pcode).first()
        if myPanchayat is not None:
           if myPanchayat.block not in blockArray:
             myPanchayat.block.libtechTag.add(ltTag)
             logger.info(myPanchayat.block.code)
             blockArray.append(myPanchayat.block)
    logger.info(blockArray)
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
