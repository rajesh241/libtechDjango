import os
from queue import Queue
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
from nrega.crawler.commons.nregaSettings import startFinYear,panchayatCrawlThreshold,panchayatRetryThreshold,telanganaStateCode,panchayatAttemptRetryThreshold,apStateCode,crawlRetryThreshold,crawlProcessTimeThreshold,crawlerTimeThreshold,nregaPortalMinHour,nregaPortalMaxHour
#from crawlFunctions import crawlPanchayat,crawlPanchayatTelangana,libtechCrawler
from crawlerFunctions import crawlerMain
from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,savePanchayatReport,table2csv,getFullFinYear,loggerFetch
from nrega import models as nregamodels

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Jobcard,PanchayatCrawlInfo,Worker,PanchayatStat,CrawlRequest


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

  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    cqs=CrawlQueue.objects.filter(downloadStage="musterDownload")
    for cq in cqs:
      logger.info(cq.id)
      crawlerMain(logger,cq)
      cq.downloadStage="downloadMusters"
      cq.save()
    exit(0)
      
    cqID=args['testInput']
    cq=CrawlQueue.objects.filter(id=cqID).first()
    crawlerMain(logger,cq)
  if args['singleExecute']:
    now=datetime.now()
    curhour=now.hour
    cqID=args['inputID']
    downloadStage=args['downloadStage']
    if cqID is not None:
      cq=crawlRequest.objects.filter(id=cqID).first()
    else:
      cq=CrawlRequest.objects.filter( Q(isComplete=False,crawlState__minhour__lte=curhour,crawlState__maxhour__gt=curhour,attemptCount__lte=16,inProgress=False) ).order_by("-priority","-crawlState__sequence","attemptCount","crawlAttemptDate").first()
    if cq is not None:
      logger.info("Starting download for cq %s block %s " % (cq.id,cq.block))
      cq.inProgress=True
      cq.crawlAttemptDate=timezone.now()
      cq.save()
      crawlerMain(logger,cq.id)
    else:
      logger.info("Nothing to process")
      time.sleep(10)
  logger.info("...END PROCESSING") 

if __name__ == '__main__':
  main()
