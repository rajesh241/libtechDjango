import os
import pandas as pd
import csv
import lxml
from selenium import webdriver
import lxml.html
import urllib.request
from urllib.parse import urlencode
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
#os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
#django.setup()
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
  filename = '%s/%s_%s_%s_ledger_details.html' % (dirname, block_name, panchayat_name, jobcard_no)
  districtCode='14'
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
      '_TSM_HiddenField_': 'waLRj7BAFGHZZsNrhVsBe3mej2rWHPs3CfbGDuGkLok1',
      '__EVENTTARGET': 'ctl00$MainContent$ddlDistrict',
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': '',
     '__VIEWSTATEGENERATOR': '',
     '__VIEWSTATEENCRYPTED': '',
     'ctl00$MainContent$ddlDistrict': districtCode,
     'ctl00$MainContent$ddlService': 'NREGS',
     'ctl00$MainContent$ACCPEN': 'rbnSSPACC',
     'ctl00$MainContent$txtSSPACC': ''
      }
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    logger.info(data)

    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code != 200:
      return "error"
    myhtml=response.content
    with open("l.html","wb") as f:
      f.write(myhtml)
    data['__EVENTTARGET']='ctl00$MainContent$rbnSSPPEN'
    data['ctl00$MainContent$ACCPEN']='rbnSSPPEN'
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    """
    data = {
  '_TSM_HiddenField_': 'waLRj7BAFGHZZsNrhVsBe3mej2rWHPs3CfbGDuGkLok1',
  '__EVENTTARGET': 'ctl00$MainContent$rbnSSPPEN',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': 'TZT7m24rSR8E2ZczR3GFRiJM5lsED+J9HW5kpolP9mlsbMr3CsoGlZuGBBDDCMBJKaTSQZ91u5W+9op6gafHu8727mYZC1BF0CdPOIc4nKCdDiDsiU3veJtfXnqUnvtvWDcAJIzkWcj5Nh+SybubMultdbCw5ek0r7rKm8H4vnwlZHHYQOb0zCzSB8vAVx/ooJyiq5dwjXD5LJwArTtBx4iG4A18C+8sp5GfnwEBIXwFL1YKYHXWn9yJtfsE6JK6cIWLmfPqaF+caT6Dekbp8sBOxcmnPJ3dBHPQQb7Ho+LZeK21csylSeMuOHPtC9D1C+xB1VFLIcuVQuRRsMYXsnOHrWg9HW+Egy4rUN57bDRVNlM75P2snW+DLdn83m86zoVXkZvTt4GjnaKyPf3BLG8V399qdBmwL+oq8nyZ2MGfnoMD3nQKHnFBp3fZtgGQDhv5VclLASgJFtB3/OEWPF8sHwkRXZpNXQgBoinXmbArLSxTPNfbpxzhfTAU9V7cvywAPIw4c49rIZy4sUNIDiHAkhWAcONWCkhUs5dEG5E4ujGVUrx603BYLQpEVKz+6/J2l4T5vXGbebWhDUxIJonrMcJhyM1oOgQFhzYBX6Z7LjF5OTy8GZ/xHgOm9Zk6WpVObCFh0fursCzeE4pEu8NjjW7Gag9qeu6dqbf9t4cWO5q8HNul8CAhXGU2w6rk7Q3FoVMi96/Zkke5BDMjMnr/Fd8mnccwJm0jAE3YEvyhUIc7ba/MVPVm9Slcsfe136PT6RFq8IF4kDUyajobm75uD+PhURUbjmWm8N/GYA72EHANVBaUlHRgqdBPXlgO4kBhFqVgNfB7xJorUpPzL8oF9J2mn6dt1lHP2WgLu4mtMJAJXITLQeaKmlJqLxHEtYbbTgf1yw3frXrscD8oiOA8zW9uECNOZxljjAecudYWpnfZbJnls34/9Dua3CBUDik/WXZ+7O5jhv6q0/THt2C0ANsUtAPxz0p+VMbRyAemml6Tv/kcVL8TdW3TErGUWJ7qHVpVcZC2yiszC5+WhYg452wHwU0P40vzSR//5SxKD/75ITBLXCEtuIp4x4gRUUcj5w==',
  '__VIEWSTATEGENERATOR': 'FD8D7E63',
  '__VIEWSTATEENCRYPTED': '',
  'ctl00$MainContent$ddlDistrict': '14',
  'ctl00$MainContent$ddlService': 'NREGS',
  'ctl00$MainContent$txtSSPACC': '',
  'ctl00$MainContent$ACCPEN': 'rbnSSPPEN'
}
    """
    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code != 200:
      return "error"
    myhtml=response.content
    with open("l.html","wb") as f:
      f.write(myhtml)
    data['__EVENTTARGET']=''
    data['ctl00$MainContent$txtSSPPEN']='142000520007010154-02'
    data['ctl00$MainContent$btnMakePayment']=''
    """
data = {
  '_TSM_HiddenField_': 'waLRj7BAFGHZZsNrhVsBe3mej2rWHPs3CfbGDuGkLok1',
  '__EVENTTARGET': '',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': 'fn2W6AmKJTCMPoOFaQh8A3PJ2zcFt5ZK1TGILt1yVUlX92NkELfEMsJOhCLfJ6nvWoPc2tq5ptco5gQmS8GFYNs0pSjc4VYe+PWfrgSYWr/paCqKuv7Kp8FDdw2WI+xqwCTp/gNUXRdaE2rOANo0/Iob1KbXuhmPf0iGEmF3XxTDfD01qbqp6LUh9ykCthLHtoxVOdMiwtDvgEGScuOhLUIiPvqKt2xDyLUiMf4k6R/OKl3gQ68cfHKYjYb4/3MlaKoB8Fx1HPtFnmGUYDk4Mw667jkQhR/ebnLaIB7LVCDSU3g1ru0ooKGumhCC27q4fa2Frs76k7jcbzycpbtW+EN5piVfAM9GjVqZeN0GPd81EZ0LxgR2JatDPaXpZcPoTyXv34mOk4AGAi4NJmxWmfTZMVboouvQZo57ZLSZMzhNU588/I71ad9khrpudgNZ8FF49qTWP9Ox8UfTeVr1R1ZyzUKq/jWsd4ErEq/E05PUQ+fRzwo4ThnpBrdaQvnFSH4UeDkc1Kqej848T5D7f/sOFFjQQ3+4xHtDQICVlQQXLecfykFaC9NEE423xFD3Z8pIsiPNAVcDVeVMu6VyAbfb0UmLKfLHlIP3E+/3Jwq1nWAmaMP6sVgdYuAszN0W/YhkLv4TCfszGDfBEwJbD5joVvHOjef6QznJHzA5aCoK/W9EELca+KnsK408hIr/CkyuSk77t7VvUdVfsdZrWZTdpQ8o+oiKznZto/A5w2slAyzKc8ZCodO9OZ5jlPzpyjdHY5S0SSiWRNjLWHLeAz4xtA5GTnLOUIVdYJeOf9AjH3Y+AzebldcbyT3l6/SOkXQMrDSD8x0DKn2C17j/u379tMoLwPK0gcSO7QD7/0xMOBCsYBYFIxUBZnm3IamGLxqjKFhDJUGCRa33V8O9wMypDG47v2/21ZXAKyhGJHPaQ595dIhO9TG88auQkZ1vKux5ftNNIUaqRzvhIFYS8H0Gq5hhCujFldHojhXZIesxEIEhDoNgOV9dfOpX4U2fHEIUifJE5H2/JWnZiLEcpL7kvbqDPnn35t3dtl4Sy7eXzObUiJJuS/qIeN6/8GUEoH65u6tuioWN16LRhcTMRUmNtU8HRbX7mD+kbdN1GWOnqpZXdXH9D9uHOHtDrEFMbCjXIMCCHkuKYEJ9Fiq6zVEpswk=',
  '__VIEWSTATEGENERATOR': 'FD8D7E63',
  '__VIEWSTATEENCRYPTED': '',
  'ctl00$MainContent$ddlDistrict': '14',
  'ctl00$MainContent$ddlService': 'NREGS',
  'ctl00$MainContent$ACCPEN': 'rbnSSPPEN',
  'ctl00$MainContent$txtSSPPEN': '142000520007010154-02',
  'ctl00$MainContent$btnMakePayment': ''
    """
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code != 200:
      return "error"
    myhtml=response.content
    with open("l.html","wb") as f:
      f.write(myhtml)
    data['__EVENTTARGET']='ctl00$MainContent$dgListOfAccounts$ctl02$rbSelect'
    data['ctl00$MainContent$dgListOfAccounts$ctl02$rbSelect']='rbSelect'
    del data['ctl00$MainContent$btnMakePayment']
    """
    data = {
  '_TSM_HiddenField_': 'waLRj7BAFGHZZsNrhVsBe3mej2rWHPs3CfbGDuGkLok1',
  '__EVENTTARGET': 'ctl00$MainContent$dgListOfAccounts$ctl02$rbSelect',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': 'WdXbiRGgsfx40zqjKOeka7hobiYw8P0yWgb13ZjOIuycYu1Auh6h/OW3inh/2/VHCNCT+yVycIhXzNAuLd5xbMbWmHVWSO3T4RJSPcgfX/4BAw8kfAnY9aelRuglM9r1cpocwtHzYbA8A9jIsl/tNSpK+YbzxuseTA9gAfU8hmu5nZgPGbL37XSY2+r9dHEc11CHcLL/2MlACs6qcHvgBQsgDSt5gllggAuAH7dZIJaVVZu84ubp+UN48OAHU2yyx8VJb+JQWoc8AIJL13LoCxQW3fuD2Gx74nz7UMKR//C4ALwXcQCceElsLXNbVzd14pv0bzsbWeJvYD/Zwbx1R8fwtoRKAFRKpAeb1Bk/cYCsjm3VQmlKO8S8eugV59riH+N6+PxA8+wdLJ7P/MA6ITeXrc9NBfBWgQ1RLMEhINbi4r+3WuwJJr28yKhpj21AP1xQxVgFG8X38K812kmtoCJn+ShOuVxAyi+ry4mk/tzIWoet5+en5v02ptqlyUuou1JcbtdchhyXKIAtLK2AK5JPIrtU7ehMg8+fdxP1LgbQAqOb7f1emMddGHtxTBku6LdzY+ZJvW1fcIT2XInEiKk4yt9TFc2I/iISMmMiJTZFkK/9vCM4OSbwMMetwIROBujW0oQBfxKRiIroJSgpuUrGJdpT5XHqf4OiJ70olIEhzI6mRuVS0/CiGF7qvfngKRjSqqpbVvhF2dzSmdFd//LJE0agzOVBxobGmKqmEWRVlUw7yTOceiA0jQOFz+22zBzNrrQDziIy/yCCb+/rg8+bBw5Gm5s2pi0IDHJAZlXcMpuh0KFabDeErqyiv/Hi7PqmPkZODd2AYHOiKxWtGi6HpicbcJ20oZn5Wk8O3go07i2g6jt3z9GS2BcTCaUdfOqDGZjcfNBt5sOjHRspsjn78qff/qgJPuUxhOpRz7viFkFKZKLrI12um9vHb8c12E8IE3t+beXUeRr2q3Gq1lqVP44qh39G0PUt7EdH2tsINDNn3KhSV1dfQ5XVqpgoj1r97deOURQZkvmndDZFgPcwT4T67I4cSXByR3H1RboUznJ3oeUxe53U8z1R0CmP0j/0Tdxb5Zi2HI62gTEQaoEp8+0gIS++QSc0xJCIDM029y18r2o+hG6m0rJ6YH6Dq7ORyUmCVn3BOybB8ytRsmIu4GVqjGQS+P+3AFA77bNlgIxFc+tPC1Jyv0XEQerWdYN+R1LgB5L4IUhyMP0WTdR6UX9TobA4EDIxn48hRzIZ1QjeGzg87AUnXtqGX0PR/eamrlap7/6GzCbloqEEwOLgSSfqYTj89wW6alVpAe5mTYOdY5mZycfayjn6uBouFxqXZvKiKpXRLli+hSoDdqAYmlm9+IIjiAoT3qNJDUhQi3R80Ur0u/WlyVizxqLljU9VRIKltl4aiTCSboCg0YVhMchRAZzggB8GbPquA9c5rJ/icMz5xp576GQK+8eiFvvaZMIZ3vy6JCorpIAtpX5G3k2QGrWORPMNPhXAW+AOCeU2qWo7hqvhqeFihydIwUthp+6CvMr10rOjNmMvISErvR8DpN3wXaGoT2v84RW1+5mZiXs5Xw/ZwHzjxvaGOVvOf4wEPGAYccDTvDsMtHKpSe0GBEIpYXsJQx7BuDAiKAiRV7mvH250LMDbUL8TwE8LcMtu3Nd+aOKaccMY0EBt3I06jVm/QQWnxYVqmbhbDwL9',
  '__VIEWSTATEGENERATOR': 'FD8D7E63',
  '__VIEWSTATEENCRYPTED': '',
  'ctl00$MainContent$ddlDistrict': '14',
  'ctl00$MainContent$ddlService': 'NREGS',
  'ctl00$MainContent$ACCPEN': 'rbnSSPPEN',
  'ctl00$MainContent$txtSSPPEN': '142000520007010154-02',
  'ctl00$MainContent$dgListOfAccounts$ctl02$rbSelect': 'rbSelect'
}
    """
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    logger.info(data)
    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code != 200:
      return "error"
    myhtml=response.content
    with open("l.html","wb") as f:
      f.write(myhtml)
    data['ctl00$MainContent$btnTxDetails']='View Ledger'
    """
data = {
  '_TSM_HiddenField_': 'waLRj7BAFGHZZsNrhVsBe3mej2rWHPs3CfbGDuGkLok1',
  '__EVENTTARGET': '',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': 'u0LJlWil4dN6ozBO94H27KWo2XPuELpdx/L7as79MAqqyVpaKkRa4sB+ztAnw3m2Sx26iTdyRrXoovQLfQr4IBqGIX8uSnePQwxImzDrQwcpJdUpVTKYMurbF9qr9sRvDKnjkpyhEqtym6jLMwAJ0dpdliFVNOvHpEUdTVbf4efvAUTmiqtFTkcpkORE3EmZOneIDXVVAeTUOvTYY8doXyN9kTK0D+JJDaSOF9asIEwuZcSOw1R63EYSeqZMg1IBPRadl7BFLRvRItVbQRpFGVJqm3xK3MbxtLbfzNL4bRnZ0Uu0bt0+EIQLPIiZtNxzmkOqjM6RwzuxDMEN1coCoWbGCAcgfaGGiWxhy+QwGPjBrtcUAG5Xkq0MPUu44FWXON3say1ovK/hjlla/9USsOd34K1Zr3XL/IHzCoo7tVPFwkio+Faj6vRq5IdJwd6ZHyc80rLWXSSfZSOeffUJZ3MuyxdMIaVukMO0fV/ohHswweLsTaUZdQ36m4ne05JffxXjur7mc2N7IHMj/NBKzrnmejM+5DcYfa6QHE+MI67OQDFM0JWUpoMtfIWl7AGEOStkQO/6Gkmr26VC87Eo41SMsjByh5lZMfY3xI1vXijkBaEYGdXPGaHzmCG/NTXYHXCIsQbraP2JzJwpn9h0kMVY1IlH0uo2T9yC+eKLiBnvYsyCBEEpz/jnNedRIu08adTmnyJCgLUPpV/BDGrlMDEpTbvJcPm8ODivY4jUGTjyc8c++czy0r9eOuqpPIISRZdw53M2Jt4BjpznpOPrN3ZoTJUSu6tYKBf+vPFMSiq8iAkVIOhKd5rvPhk4fMpH4cYkMVcLZ/Gs05eGQrw27XMdNWwAg4ZfPfHVlT7O2CeUqUFEh07xinDMNW690CMAuXM5j7ToBH99fOLFa8+vUs5OmXXCxe4qQ1aNlcn/DO6+vDXCre0m04k7CHZNeTDrDxZp4Jrh6lONiJsNEU4PfbbJPQTPqGqy/L7qxpatxDww3qNEzRAkEHThx67+whfWMZKD5lXql2m7YB+alkAtmskeAX/aAMEX0It1NPKEKr6E+kXrQzUPBOcx2Pa17M/jCTHE4a6ZTD+4lTeD0fAYOBMaLotgyTbu9uLgyF+YNY5X/RKVLD7r83IkG5SQEdWbE/XvaQrTeIsrZOLG/eXrx5jEgg4zaLpZIKoh0NesNiCHQzD+aLKC2FJoZIH3v5VMU/QlspQSeEI4bKktfacJ1kDnXPV9voLz68tDQUr82NPu89enkRNiukEWrVWScMy7Ypy2el2dfSsF+gy5t2jRqvHopqyDhesSvzN8C1rjMVUf3cpWHfy5Ie/kXog2nCrjfqFpqohyOk8PFt8TnVegKDppi6fvY0xiKsyscRsdRN1gMmsxFBU5rfd5gFjktLPoSrftnkctCqsKVSzumAIMojSyaHQhj6XsSILDJTbDZass6r//HoONTe5Br5MlWdWgPgQ0YDd6/L6N8L888tJxsXCtGdi4CkgcGILW6Ng5Fu6YeDDdVZscMQqVklVFFtNmdmUkP74YZtPIrc14xpPaPryf+hPtR+04kgN6G1EKNQ8FIcQ9psE2MUMLpDxgC6f8tjndODt66BPS72DKNOgUU93KZUYJDC+02S6OrsOu0xQxyz5eNEs1SwfSc7Ef/Z8QQHBo32uFn2wJieSvnoCA46sqGlxMD0tga+z+sE4o7C/cDLLh2S2Y6oGhJquv5ofoOGYSW0WTlpWHL7hbgAN4G/HpXNo1OIjLSyT3CD/2i3xxBrezFeqngdB3tE3nSv4oOh4ACzlFBHEUzl7lUmAw7iPKmR2bVcHFMZArXvtXAA0lUDbMXr9UfZTR1aumSQXMNmfGeg==',
  '__VIEWSTATEGENERATOR': 'FD8D7E63',
  '__VIEWSTATEENCRYPTED': '',
  'ctl00$MainContent$ddlDistrict': '14',
  'ctl00$MainContent$ddlService': 'NREGS',
  'ctl00$MainContent$ACCPEN': 'rbnSSPPEN',
  'ctl00$MainContent$txtSSPPEN': '142000520007010154-02',
  'ctl00$MainContent$dgListOfAccounts$ctl02$rbSelect': 'rbSelect',
  'ctl00$MainContent$btnTxDetails': 'View Ledger'
}
    """
    data=updateViewStateValidation(logger,myhtml,data,fieldLabels)     
    logger.info(data)
    response = requests.post('https://bdp.tsonline.gov.in/NeFMS_TS/NeFMS/Reports/NeFMS/AccountWiseTransactionReport.aspx', headers=headers, cookies=cookies, data=data)
    if response.status_code != 200:
      return "error"
    myhtml=response.content
    mysoup=BeautifulSoup(myhtml,"lxml")
    myscripts=mysoup.findAll("script")
    params=None
    encodedurl=None
    for eachScript in myscripts:
      text=eachScript.text
      if "window.open" in text:
        logger.info(text)
        textArray=text.split("'")
        url="https://bdp.tsonline.gov.in"+textArray[1]
        encodedurl=urllib.parse.quote(url)
        urlArray=url.split("ViewLedger.aspx?")
        params=urllib.parse.quote(urlArray[1])
    if url is None:
      return error
    logger.info(url)
    logger.info(encodedurl)
    cmd="curl '%s' -H 'Host: bdp.tsonline.gov.in' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' -H 'Accept-Language: en-GB,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br' -H 'Cookie: ASP.NET_SessionId=2lxez2aiie121s45h3hva3nd' -H 'Connection: keep-alive' | gunzip > %s" % (url,filename)
    os.system(cmd)
    data=parse_rn6_report(logger,filename="b.html")
    logger.info(data)
    csv_filename = filename.replace('.html','.csv')
    logger.info('Writing to CSV [%s]' % csv_filename)
    data.to_csv(csv_filename)
def parse_rn6_report(logger, filename=None, panchayat_name=None, village_name=None, jobcard_no=None):
    logger.info('Parse the RN6 HTML file')

    try:
        with open(filename, 'r') as html_file:
            logger.info('Reading [%s]' % filename)
            html_source = html_file.read()
    except Exception as e:
        logger.error('Exception when opening file for jobcard[%s] - EXCEPT[%s:%s]' % (jobcard_no, type(e), e))
        raise e
    data = pd.DataFrame([], columns=['S.No', 'Mandal Name', 'Gram Panchayat', 'Village', 'Job card number/worker ID', 'Name of the wageseeker', 'Credited Date', 'Deposit (INR)', 'Debited Date', 'Withdrawal (INR)', 'Available Balance (INR)', 'Diff. time credit and debit'])
    try:
        df = pd.read_html(filename, attrs = {'id': 'ctl00_MainContent_dgLedgerReport'}, index_col='S.No.', header=0)[0]
        # df = pd.read_html(filename, attrs = {'id': 'ctl00_MainContent_dgLedgerReport'})[0]
    except Exception as e:
        logger.error('Exception when reading transaction table for jobcard[%s] - EXCEPT[%s:%s]' % (jobcard_no, type(e), e))
        return data
    logger.info('The transactions table read:\n%s' % df)
    
    bs = BeautifulSoup(html_source, 'html.parser')

    # tabletop = bs.find(id='ctl00_MainContent_PrintContent')
    # logger.info(tabletop)
    table = bs.find(id='tblDetails')
    logger.debug(table)

    account_no = table.find(id='ctl00_MainContent_lblAccountNo').text.strip()
    logger.debug('account_no [%s]' % account_no)

    bo_name = table.find(id='ctl00_MainContent_lblBOName').text.strip()
    logger.debug('bo_name [%s]' % bo_name)

    jobcard_id = table.find(id='ctl00_MainContent_lblJobcardPensionID').text.strip()
    logger.debug('jobcard_id [%s]' % jobcard_id)

    if jobcard_id != jobcard_no:
        logger.critical('Something went terribly wrong with [%s != %s]!' % (jobcard_id, jobcard_no))

    so_name = table.find(id='ctl00_MainContent_lblSOName').text.strip()
    logger.debug('so_name [%s]' % so_name)

    account_holder_name = table.find(id='ctl00_MainContent_lblName').text.strip()
    logger.debug('account_holder_name [%s]' % account_holder_name)

    mandal_name = table.find(id='ctl00_MainContent_lblMandalName').text.strip()
    logger.debug('mandal_name [%s]' % mandal_name)

    table = bs.find(id='ctl00_MainContent_dgLedgerReport')
    logger.debug(table)
    try:
        tr_list = table.findAll('tr')
    except:
        logger.info('No Transactions')
        return 'SUCCESS'
    logger.debug(tr_list)

    # desired_columns =  [1, ]
    # for row in df.itertuples(index=True, name='Pandas'):
    debit_timestamp = pd.to_datetime(0)

    df = df.iloc[::-1] # Reverse the order for calculating diff time Debit dates are easier to record in this order
    for index, row in df.iterrows():
        logger.debug('%d: %s' % (index, row))

        serial_no = index
        logger.debug('serial_no[%s]' % serial_no)

        transaction_date = row['Transaction Date']
        logger.debug('transaction_date[%s]' % transaction_date)

        transaction_ref = row['Transaction Reference']
        logger.debug('transaction_ref[%s]' % transaction_ref)

        withdrawn_at = row['Withdrawn at']
        logger.debug('withdrawn_at[%s]' % withdrawn_at)

        deposit_inr = row['Deposit (INR)']
        logger.debug('deposit_inr[%s]' % deposit_inr)

        withdrawal_inr = row['Withdrawal (INR)']
        logger.debug('withdrawal_inr[%s]' % withdrawal_inr)

        availalbe_balance = row['Available Balance (INR)']
        logger.debug('availalbe_balance[%s]' % availalbe_balance)

        if deposit_inr == 0:
            (credited_date, debited_date, diff_days, debit_timestamp) = (0, transaction_date, 0, pd.to_datetime(transaction_date, dayfirst=True)) #  datetime.strptime(transaction_date, "%d/%m/%Y").timestamp())
        else:
            (credited_date, debited_date, diff_days) = (transaction_date, 0, (debit_timestamp - pd.to_datetime(transaction_date, dayfirst=True)).days) # datetime.strptime(transaction_date, "%d/%m/%Y").timestamp())
        logger.info('credited_date[%s]' % credited_date)
        logger.info('debited_date[%s]' % debited_date)
        logger.info('diff_days[%s]' % diff_days)

        if diff_days < 0:
            diff_days = 0
            continue
        logger.info('After Reset diff_days[%s]' % diff_days)
        
        #csv_buffer.append('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' %(serial_no, mandal_name, bo_name, so_name, jobcard_id, account_holder_name, credited_date, debited_date, withdrawal_inr, availalbe_balance, diff_time))
        data = data.append({'S.No': serial_no, 'Mandal Name': mandal_name, 'Gram Panchayat': panchayat_name, 'Village': village_name, 'Job card number/worker ID': jobcard_id, 'Name of the wageseeker': account_holder_name, 'Credited Date': credited_date, 'Deposit (INR)': deposit_inr, 'Debited Date': debited_date, 'Withdrawal (INR)': withdrawal_inr, 'Available Balance (INR)': availalbe_balance, 'Diff. time credit and debit': diff_days}, ignore_index=True)

    data = data.set_index('S.No')
    data = data.iloc[::-1]  # Reverse the order back to normal        
    logger.info('The final table:\n%s' % data)

    return data


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
