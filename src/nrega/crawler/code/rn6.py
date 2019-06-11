import os
import csv
import lxml
from selenium import webdriver
import lxml.html
import urllib.request
from bs4 import BeautifulSoup
import re
import random
import sys
import time
import json
import requests
import boto3
import pytesseract
import urllib.request as urllib2
import http.cookiejar as cookielib
from io import BytesIO
from transliterate.base import TranslitLanguagePack, registry
from transliterate import translit
from PIL import Image
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
sys.path.insert(0, rootDir)
from config.defines import djangoSettings,logDir
from nrega.crawler.commons.nregaSettings import startFinYear,panchayatCrawlThreshold,panchayatRetryThreshold,telanganaStateCode,panchayatAttemptRetryThreshold,apStateCode,crawlRetryThreshold,crawlProcessTimeThreshold,crawlerTimeThreshold
#from crawlFunctions import crawlPanchayat,crawlPanchayatTelangana,libtechCrawler
from nrega.crawler.commons.sn import driverInitialize, driverFinalize, displayInitialize, displayFinalize
from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,table2csv,getFullFinYear,loggerFetch,getDateObj,getCenterAlignedHeading,stripTableAttributesPreserveLinks
from nrega import models  as nregamodels
from nrega.models import Jobcard,Location,CrawlRequest,Info
from nrega.models import Jobcard,Location,CrawlRequest
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
from nregaDownload import crawlerMain,PanchayatCrawler,computePanchayatStat,downloadMuster,downloadWagelist,createCodeObjDict,createDetailWorkPaymentReport,telanganaJobcardDownload,telanganaJobcardProcess,createWorkPaymentReportAP,processRejectedPayment,downloadRejectedPayment,processWagelist,processMuster,downloadMISDPReport,processMISDPReport,downloadJobcardStat,processJobcardStat,jobcardRegister,objectDownloadMain,downloadMusterNew,processWorkDemand,downloadWorkDemand,downloadJobcardStat,fetchOldMuster,objectProcessMain,computeJobcardStat,downloadJobcard,processJobcard,validateAndSave,getReportHTML,createWorkPaymentJSK,validateNICReport,updateObjectDownload,downloadWagelist,processWagelist,crawlFTORejectedPayment,processBlockRejectedPayment,matchTransactions,getFTOListURLs
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,DemandWorkDetail,MISReportURL,PanchayatStat,RejectedPayment,FTOTransaction,WorkDetail,PDSLocation
from nrega.crawler.code.languagePacks import hindiLanguagePack

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
  parser.add_argument('-f', '--finyear', help='Test Input', required=False)

  args = vars(parser.parse_args())
  return args
def updateViewStateValidation(logger,myhtml,d,fields):
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  for field in fields:
    try:
      d[field] = htmlsoup.find(id=field).get('value')
    except:
      d[field]=''
  return d
 
def downloadLedger(logger,jobcard):
  r=requests.get("https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx")
  if r.status_code == 200:
    cookies=r.cookies
    myhtml=r.content
    fieldLabels=['__VIEWSTATEGENERATOR','_TSM_HiddenField_','__VIEWSTATE']
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Connection': 'keep-alive',
    'Host': 'bdp.tsonline.gov.in',
    'Referer': 'https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Content-Type': 'application/x-www-form-urlencoded',
     }

    data = {
      '_TSM_HiddenField_': '',
      '__EVENTTARGET': '',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': '',
      '__VIEWSTATEGENERATOR' :'',
      '__VIEWSTATEENCRYPTED': '',
      'ctl00$MainContent$ddlDistrict': '14',
      'ctl00$MainContent$ddlService': 'NREGS',
      'ctl00$MainContent$ACCPEN': 'rbnSSPPEN',
      'ctl00$MainContent$txtSSPPEN': '142000520007010154-02',
      'ctl00$MainContent$btnMakePayment': ''
      }
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    logger.info(data)

    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code == 200:
      myhtml=response.content
      with open("/tmp/l.html","wb") as f:
        f.write(myhtml)

def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    logger.info("Testing it with basic code")
    jobcard=args['testInput']
    jobcard='142000520007010154-02'
    downloadLedger(logger,jobcard)
  logger.info("...END PROCESSING") 
  exit(0)
if __name__ == '__main__':
  main()
