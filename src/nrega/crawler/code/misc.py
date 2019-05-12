import os
import urllib.request
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

from nrega.crawler.commons.nregaFunctions import stripTableAttributes,htmlWrapperLocal,getCurrentFinYear,table2csv,getFullFinYear,loggerFetch,getDateObj,getCenterAlignedHeading,stripTableAttributesPreserveLinks
from nrega import models  as nregamodels
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
from nregaDownload import crawlerMain,PanchayatCrawler,computePanchayatStat,downloadMuster,downloadWagelist,createCodeObjDict,createDetailWorkPaymentReport,telanganaJobcardDownload,telanganaJobcardProcess,createWorkPaymentReportAP,processRejectedPayment,downloadRejectedPayment,processWagelist,processMuster,downloadMISDPReport,processMISDPReport,downloadJobcardStat,processJobcardStat,jobcardRegister,objectDownloadMain,downloadMusterNew,processWorkDemand,downloadWorkDemand,downloadJobcardStat,fetchOldMuster,objectProcessMain,computeJobcardStat,downloadJobcard,processJobcard,validateAndSave,getReportHTML,createWorkPaymentJSK,validateNICReport,updateObjectDownload,downloadWagelist,processWagelist,crawlFTORejectedPayment,processBlockRejectedPayment,matchTransactions,getFTOListURLs
from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlQueue,Village,Worker,JobcardStat,Wagelist,WagelistTransaction,DPTransaction,FTO,Report,DemandWorkDetail,MISReportURL,PanchayatStat,RejectedPayment,FTOTransaction,WorkDetail
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

def nicMusterSearch(logger,pobj,obj):
  searchURL="http://mnregaweb4.nic.in/netnrega/nregasearch1.aspx"
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,requestType="GET")
  logger.info(validation)
  logger.info(viewState)
  logger.info(cookies)
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
  'txt_keyword2': '3',
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
  with open("/tmp/a.html","wb") as f:
    f.write(myhtml)
  exit(0)
  if r.status_code == 200:
    myhtml=r.content
    cookies=r.cookies
    logger.info(cookies)
    with open ("/tmp/a.html","wb") as f:
      f.write(myhtml)
  exit(0)
  panchayatPageURL=pobj.panchayatPageURL
  fullFinYear=getFullFinYear(getCurrentFinYear())
  panchayatPageURL=panchayatPageURL.replace("fullfinyear",fullFinYear)
  logger.info(panchayatPageURL)
  r=requests.get(panchayatPageURL)
  if r.status_code == 200:
    myhtml=r.content
    s="Musternew.aspx"
    urlPrefix="http://%s/netnrega/" % (pobj.crawlIP)
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    a=htmlsoup.find("a", href=re.compile(s))
    musterSearchURL="%s%s" % (urlPrefix,a['href'])
    logger.info(musterSearchURL)
    cookies=r.cookies
    logger.info(cookies) 
    r=requests.get(musterSearchURL,cookies=cookies)
    
    if r.status_code==200:
      myhtml=r.content
      if myhtml is not None:
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
       # logger.info(viewState)
        logger.info(validation)
        logger.info(cookies)
        headers = {
    'Host': 'nregasp2.nic.in',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'X-MicrosoftAjax': 'Delta=true',
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
    'Connection': 'keep-alive',
    'Referer': musterSearchURL,
        }


        data = {
  'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$ScriptManager1|ctl00$ContentPlaceHolder1$ddlwork',
  '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlwork',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState,
  '__EVENTVALIDATION': validation, 
  'ctl00$ContentPlaceHolder1$ddlFinYear': '2018-2019',
  'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
  'ctl00$ContentPlaceHolder1$txtSearch': '',
  'ctl00$ContentPlaceHolder1$ddlwork': '3403009001/IF/7080901218975',
  'ctl00$ContentPlaceHolder1$ddlMsrno': '---select---',
  '__ASYNCPOST': 'true',
  '': ''
}


        response = requests.post(musterSearchURL, headers=headers, cookies=cookies, data=data)
        time.sleep(5)
        response = requests.post(musterSearchURL, headers=headers, cookies=cookies, data=data)
        if response.status_code==200:
          logger.info("file downloaded successfully")
          myhtml=response.text
          with open("/tmp/b.html","w") as f:
            f.write(myhtml)
        exit(0)
        data = {
  'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel2|ctl00$ContentPlaceHolder1$ddlMsrno',
  'ctl00$ContentPlaceHolder1$ddlFinYear': '2018-2019',
  'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
  'ctl00$ContentPlaceHolder1$txtSearch': '',
  'ctl00$ContentPlaceHolder1$ddlwork': '3403009001/IF/7080901218975',
  'ctl00$ContentPlaceHolder1$ddlMsrno': '1553~~19/5/2018~~25/5/2018',
  '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlMsrno',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__EVENTVALIDATION': validation,
  '__ASYNCPOST': 'true',
  '': ''
}
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        response = requests.post(musterSearchURL, headers=headers, cookies=cookies, data=data)
        myhtml=response.content
        with open("/tmp/m1.html","wb") as f:
          f.write(myhtml)

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# response = requests.post('http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2018-2019&state_code=34&district_code=3403&block_code=3403009&panchayat_code=3403009009&State_name=JHARKHAND&District_name=GUMLA&Block_name=BASIA&panchayat_name=MORENG&Digest=LTENd+4lncVe3Cd%2fgBlKCQ', headers=headers, cookies=cookies, data=data)
  

#NB. Original query string below. It seems impossible to parse and
#reproduce query strings 100% accurately so the one below is given
#in case the reproduced version is not "correct".
# response = requests.post('http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2018-2019&state_code=34&district_code=3403&block_code=3403009&panchayat_code=3403009009&State_name=JHARKHAND&District_name=GUMLA&Block_name=BASIA&panchayat_name=MORENG&Digest=LTENd+4lncVe3Cd%2fgBlKCQ', headers=headers, cookies=cookies, data=data)
  
    exit(0)
    logger.info(url)
    match = re.findall("demreport.aspx\?lflag=local&Digest=(.*?)\" target", myhtml)
    logger.info(match)
    if len(match) == 1:
      digestString=match[0]
    else:
      return "Digest not found"

  exit(0)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    cookies=r.cookies
    validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
def processRejectedPayment1(logger,pobj,obj):
  myhtml=None
  isComplete=False
  finyear=obj.finyear
  if obj.contentFileURL is not None:
    r=requests.get(obj.contentFileURL)
    if r.status_code==200:
        myhtml=r.content
  if myhtml is not None:
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    table=htmlsoup.find("table",id="myTable")
    if table is not None:
      logger.info("Found the Table")
      rows=table.findAll('tr')
      for row in rows:
        if pobj.jobcardPrefix in str(row):
          cols=row.findAll('td')
          wagelistNo=cols[0].text.lstrip().rstrip()
          jobcard=cols[1].text.lstrip().rstrip()
          name=cols[3].text.lstrip().rstrip()
          musterNo=cols[6].text.lstrip().rstrip()
          referenceNo=cols[7].text.lstrip().rstrip()
          status=cols[8].text.lstrip().rstrip()
          rejectionReason=cols[9].text.lstrip().rstrip()
          processDateString=cols[10].text.lstrip().rstrip()
          ftoNo=cols[11].text.lstrip().rstrip()
          logger.info("fto no is %s " % ftoNo)
          workerCode="%s_%s" % (jobcard,name)
          musterCode="%s_%s_%s" % (pobj.blockCode,finyear,musterNo)
          wagelistCode="%s_%s_%s" % (pobj.blockCode,finyear,wagelistNo)
          try:
            myWorker=pobj.codeObjDict[workerCode]
          except:
            myWorker = None
          
          try:
            myMuster=pobj.codeObjDict[musterCode]
          except:
            myMuster = None
          
          try:
            myWagelist=pobj.codeObjDict[wagelistCode]
          except:
            myWagelist=None
          if (myWorker is not None) and (myWagelist is not None):
            dwd=DemandWorkDetail.objects.filter(worker=myWorker,muster=myMuster).first()
            myFTO=FTO.objects.filter(ftoNo=ftoNo).first()
            wt=None
            if dwd is not None:
              wt=WagelistTransaction.objects.filter(wagelist=myWagelist,dwd=dwd).first()
            if wt is None:
              wts=WagelistTransaction.objects.filter(wagelist=myWagelist,worker=myWorker)
              if len(wts) == 1:
                wt=wts.first()
              elif len(wts) > 1:
                wts=WagelistTransaction.objects.filter(wagelist=myWagelist,worker=myWorker,fto=myFTO,referenceNo=referenceNo)
                if len(wts) == 1:
                  wt=wts.first()
                else:
                  wts=WagelistTransaction.objects.filter(wagelist=myWagelist,worker=myWorker,fto=myFTO)
                  wt=wts.first()
            if wt is not None:
              wt.fto=myFTO
              wt.dwd=dwd
              wt.referenceNo=referenceNo
              wt.status=status
              wt.rejectionReason=rejectionReason
              wt.processDate==getDateObj(processDateString)
              logger.info("Waving wt %s " % str(wt.id))
              wt.save()

              
  
def downloadRejectedPayment1(logger,pobj,obj):
  s3url=None
  url=obj.url
  r=requests.get(url)
  error=None
  finyear=obj.finyear
  fullfinyear=getFullFinYear(finyear)
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

def getFTOListURLs1(logger,pobj,finyear):
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
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        a=htmlsoup.find("a", href=re.compile(s))
        if a is not None:
          bankURL="%s%s" % (urlPrefix,a['href'])
          urls.append(bankURL)
        logger.info(bankURL)
        #Lets get rejected Payment URL
        s="&block_code=%s&fin_year=%s&typ=R" % (pobj.blockCode,fullFinYear)
        a=htmlsoup.find("a", href=re.compile(s))
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
          s="&block_code=%s&fin_year=%s&typ=R" % (pobj.blockCode,fullFinYear)
          a=htmlsoup.find("a", href=re.compile(s))
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
          s="&block_code=%s&fin_year=%s&typ=R" % (pobj.blockCode,fullFinYear)
          a=htmlsoup.find("a", href=re.compile(s))
          if a is not None:
            postURLRejected="%s%s" % (urlPrefix,a['href'])
            urlsRejected.append(postURLRejected)
        logger.info(postURL)

  return urls,urlsRejected

def crawlFTO(logger,pobj,finyear):
  urls,urlsRejected=getFTOListURLs(logger,pobj,finyear)
  logger.info(urls)
  logger.info("Printing Rejected URLs")
  logger.info(urlsRejected)
  reportType="rejectedPaymentHTML"
  reportName="Rejected Payment HTML"
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
        rows=myTable.findAll('tr')
        for row in rows:
          if jobcardPrefix in str(row):
            cols=row.findAll('td')
            referenceNo=cols[2].text.lstrip().rstrip()
            a=cols[2].find('a')
            url="%s%s" %(baseURL,a['href'])
            rp=RejectedPayment.objects.filter(block=pobj.block,finyear=finyear,referenceNo=referenceNo).first()
            if rp is None:
              rp=RejectedPayment.objects.create(block=pobj.block,finyear=finyear,referenceNo=referenceNo)
            rp.url=url
            rp.save()
  outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)
  savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath,locationType='block')
  if len(urls) == 0:
    error="No FTO URL Found for finyear %s " % (finyear)
    return error
  outhtml=""
  outhtml="<html><body><table>"
  outhtml="<tr><th>%s</th><th>%s</th></tr>" %("ftoNo","paymentMode")
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
              ftoRelativeURL=cols[1].find("a")['href']
              urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
              ftoURL="%s%s" % (urlPrefix,ftoRelativeURL)
              logger.info(ftoURL)
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
  reportName="FTO List"
  reportType="ftoList"
  error=validateAndSave(logger,pobj,outhtml,reportName,reportType,finyear=finyear,locationType="block",jobcardPrefix="Financial Institution")
           
def downloadPanchayatStats(logger,pobj,finyear):
  reportType="blockStatHTML"
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
          ps.save()






def getPanchayatStats(logger,pobj,finyear):
  mru=MISReportURL.objects.filter(state__code=pobj.stateCode,finyear=finyear).first()
  urlPrefix="http://mnregaweb4.nic.in/netnrega/citizen_html/"
  reportType="blockStatHTML"
  reportName="Block Statistics"
  error="unable to save report"
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
      
  

def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  p={
      '05':'50',  
      '33':'80',
      '34':'120',
      '15':'70',
      '16':'90',
      '17':'60',
      '24':'15',
      '27':'20',
      '31':'40',
      '32':'30'
    }
 
  if args['populate']:
  # Basic 3403009
  # javaja 2721003
  # Manika   3406004
    blockCode=args['testInput']
#    blockCodes=['3614008','3614007','3614006','3614005']
    blockCodes=[blockCode]
    myPanchayats=Panchayat.objects.filter(block__code__in=blockCodes)
    for eachPanchayat in myPanchayats:
      CrawlQueue.objects.create(panchayat=eachPanchayat,priority=50000,startFinYear='18')
      logger.info(eachPanchayat.name)

  if args['test']:
#   reportType='telanganaMusters'
#   myReports=Report.objects.filter(reportType=reportType)
#   for eachReport in myReports:
#      f=urllib.request.urlopen(eachReport.reportURL)
#      s=f.read()
#      if "***No Data found for the selection" in s.decode("utf-8"):
#   #     eachReport.delete()
#        logger.info("INvalid Report for panchayat %s block %s finyear %s" % (eachReport.panchayat.name,eachReport.panchayat.block.name,eachReport.finyear))
    ltTag=LibtechTag.objects.filter(id=8).first()
    with open('/tmp/tjobcards.csv') as fp:
      for line in fp:
        tjobcard=line.lstrip().rstrip()
        myJobcard=Jobcard.objects.filter(tjobcard=tjobcard).first()
        if myJobcard is not None:
          myJobcard.libtechTag.add(ltTag)
          myJobcard.save()
        else:
          logger.info(tjobcard)

    exit(0)
    objs=CrawlQueue.objects.filter(id__gt=8241)
    cqID=args['testInput']
    inID=args['testInput2']
    finyear=args['finyear']
    cq=CrawlQueue.objects.filter(id=cqID).first()
    pobj=PanchayatCrawler(cq.id)
    createCodeObjDict(logger,pobj)
    createDetailWorkPaymentReport(logger,pobj,finyear)
    exit(0)
    obj=Muster.objects.filter(id=inID).first()
    modelName="Muster"
    objectDownloadMain(logger,pobj,modelName,finyear)
    exit(0)
    downloadMuster(logger,pobj,obj)
    exit(0)
    urls,urlsRejected=getFTOListURLs(logger,pobj,finyear)
    logger.info(urls)
    #crawlFTORejectedPayment(logger,pobj,finyear)
    exit(0)
    obj=Muster.objects.filter(id=inID).first()
    nicMusterSearch(logger,pobj,obj)
    exit(0)
    downloadMusterNew(logger,pobj,obj)
    exit(0)
    obj=RejectedPayment.objects.filter(id=inID).first()
    downloadRejectedPayment(logger,pobj,obj)
    exit(0)
    processBlockRejectedPayment(logger,pobj,finyear)
    exit(0)
    modelName="RejectedPayment"
    crawlFTORejectedPayment(logger,pobj,finyear)
    exit(0)
    createDetailWorkPaymentReport(logger,pobj,finyear)
    exit(0)
    matchTransactions(logger,pobj,finyear)
    exit(0)
    obj=Wagelist.objects.filter(id=inID).first()
    processWagelist(logger,pobj,obj)
    exit(0)
    processBlockRejectedPayment(logger,pobj,finyear)
    exit(0)
    modelName="Muster"
    objectProcessMain(logger,pobj,modelName,finyear)
    exit(0)
    modelName="Jobcard"
    objectProcessMain(logger,pobj,modelName,'19')
    exit(0)
    objs=WorkDetail.objects.all()
    for obj in objs:
      obj.delete()
    exit(0)
    obj=Jobcard.objects.filter(id=inID).first()
    processJobcard(logger,pobj,obj)
    exit(0)
#    obj=Wagelist.objects.filter(id=inID).first()
#    processWagelist(logger,pobj,obj)
#    processRejectedPayment(logger,pobj,obj)
    exit(0)
    finyear='19'
    crawlFTO(logger,pobj,finyear)
    finyear='18'
    crawlFTO(logger,pobj,finyear)
    exit(0)
    processJobcard(logger,pobj,obj)
    computeJobcardStat(logger,pobj)
    exit(0)
    processJobcard(logger,pobj,obj)
    exit(0)
    finyear='18'
    crawlFTO(logger,pobj,finyear)
    exit(0)
    myTag=LibtechTag.objects.filter(id=4).first()
    ltArray=[myTag]
    cqs=CrawlQueue.objects.filter(Q(panchayat__libtechTag__in=ltArray))
    for cq in cqs:
      cq.source="jan2019"
      cq.save()
    exit(0)
    downloadPanchayatStats(logger,pobj,finyear)
    exit(0)
    cqs=CrawlQueue.objects.filter(panchayat__block__district__state__code='16')
    for cq in cqs:
      cq.downloadStage="downloadJobcards"
      cq.attemptCount=0
      cq.save()
    exit(0)
    createCodeObjDict(logger,pobj)
    obj=Jobcard.objects.filter(id=inID).first()
    downloadJobcard(logger,pobj,obj)
    exit(0)
    objs=DemandWorkDetail.objects.filter(finyear='18',worker__jobcard__panchayat=pobj.panchayat).values("worker__jobcard__jobcard").annotate(dsum=Sum('daysDemanded'),asum=Sum('daysAllocated'))
    s="jobcard,dsum,asum,daysDemanded,daysProvided\n"
    for obj in objs:
      jobcard=obj['worker__jobcard__jobcard']
      dsum=obj['dsum']
      asum=obj['asum']
      js=JobcardStat.objects.filter(finyear='18',jobcard__jobcard=jobcard).first()
      daysDemanded=0
      nicDaysProvided=0
      if js is not None:
        daysDemanded=js.daysDemanded
        daysProvided=js.nicDaysProvided

      s+="%s,%s,%s,%s,%s\n" % (jobcard,str(dsum),str(asum),str(daysDemanded),str(daysProvided))
      logger.info(jobcard)
    with open("/tmp/js.csv","w") as f:
      f.write(s)
    exit(0)
 
    objectProcessMain(logger,pobj,"Jobcard")
    exit(0)
  #objs=Jobcard.objects.filter(panchayat=pobj.panchayat)
   #for obj in objs:
   #  obj.isDownloaded=False
   #  obj.save()
    exit(0)
    obj=Jobcard.objects.filter(id=382374).first()
    processJobcard(logger,pobj,obj)
    exit(0)
    finyear='19'
    error=downloadJobcardStat(logger,pobj,finyear)
    processJobcardStat(logger,pobj,finyear)
    exit(0)
    obj=Muster.objects.filter(id=5195156).first()
    downloadMusterNew(logger,pobj,obj)
    exit(0)
    #downloadJobcard(logger,pobj,obj)
    processJobcard(logger,pobj,obj)
    objectProcessMain(logger,pobj,"Jobcard")
   # logger.info("total %s pending %s downloaded %s " % (str(total),str(pending),str(downloaded)))
    exit(0)

    myTag=LibtechTag.objects.filter(id=4).first()
    ltArray=[myTag]
    cqs=CrawlQueue.objects.filter(Q(panchayat__libtechTag__in=ltArray) & Q ( Q(downloadStage="downloadData3") | Q(downloadStage ="downloadData2"))).order_by("panchayat__block__district__state__slug")
    for cq in cqs:
      logger.info(cq.panchayat)
      pobj=PanchayatCrawler(cq.id)
      computeJobcardStat(logger,pobj)
    exit(0)
    createCodeObjDict(logger,pobj)
    #computeJobcardStat(logger,pobj)
    finyear='18'
    processJobcardStat(logger,pobj,finyear)
    computePanchayatStat(logger,pobj,finyear)
    finyear='19'
    processJobcardStat(logger,pobj,finyear)
    computePanchayatStat(logger,pobj,finyear)
    exit(0)
    obj=Muster.objects.filter(id=98253).first()
    obj=Muster.objects.filter(id=97562).first()
    processMuster(logger,pobj,obj)
    modelName="Muster"
    objectProcessMain(logger,pobj,modelName)
    exit(0)
    error=downloadJobcardStat(logger,pobj)
    logger.info(error)
    exit(0)
    finyear='18'
    getJobcardStat(logger,pobj,finyear)
    exit(0)
    cqs=CrawlQueue.objects.filter(attemptCount__gt = 0)
    for cq in cqs:
      logger.info(cq.id)
      cq.attemptCount=0
      cq.save()
    exit(0)
    url="http://mnregaweb2.nic.in/netnrega/homestciti.aspx?state_code=24&state_name=ODISHA"
    r=requests.get(url)
    logger.info(r.cookies)
    exit(0)
    cqs=CrawlQueue.objects.all()
    for cq in cqs:
      stateCode=cq.panchayat.block.district.state.code
      if stateCode in p:
        cq.priority=p[stateCode]
        cq.save()
    exit(0)
    session = requests.session()

    url="http://mnregaweb2.nic.in/netnrega/state_html/empspecifydays.aspx?page=P&lflag=eng&state_name=ODISHA&state_code=24&district_name=NAYAGARH&district_code=2422&block_name=Nuagaon&fin_year=2017-2018&Block_code=2422004&"
    r=session.get(url)
    logger.info(dir(session))
    logger.info(session)
    cookies=requests.utils.dict_from_cookiejar(session.cookies)
    logger.info(cookies)
    logger.info(r.status_code)
    exit(0)
    cqID=args['testInput']
    cq=CrawlQueue.objects.filter(id=cqID).first()
    pobj=PanchayatCrawler(cqID)
    createCodeObjDict(logger,pobj)
    obj=Muster.objects.filter(id=491166).first()
    processMuster(logger,pobj,obj)
    exit(0)
    cqs=CrawlQueue.objects.filter(downloadStage="processData1")
    for cq in cqs:
      logger.info(cq.id)
      cq.attemptCount=0
      cq.downloadStage="downloadData1"
      cq.save()
    exit(0)
    logger.info(pobj.panchayatCode)
    logger.info(pobj.blockCode)
    obj=Muster.objects.filter(id=42146).first()
    downloadMusterNew(logger,pobj,obj) 
    objectDownloadMain(logger,pobj,"Muster")
    exit(0)

    musters=Muster.objects.all().order_by("-id")
    for m in musters:
      logger.info(m.id)
      crawlIP=m.panchayat.block.district.state.crawlIP
      fullFinYear=getFullFinYear(m.finyear)
      newMusterURL="http://%s/citizen_html/musternew.asp?id=1&msrno=%s&finyear=%s&workcode=%s&panchayat_code=%s" % (crawlIP,m.musterNo,fullFinYear,m.workCode,m.panchayat.code)
      m.newMusterURL=newMusterURL
      m.save()
    exit(0)
    cqs=CrawlQueue.objects.all()
    for cq in cqs:
      cq.startFinYear='18'
      cq.save()
    exit(0)
    cqID=args['testInput']
    pobj=PanchayatCrawler(cqID)
    createCodeObjDict(logger,pobj)
    downloadRejectedPayment(logger,pobj)

  if args['test1']:
    #Crawling Links for  MIS Reports
    
    with open('/tmp/ce.csv') as fp:
      for line in fp:
        cqID=line.lstrip().rstrip()
        logger.info(cqID)
        cq=CrawlQueue.objects.filter(id=cqID).first()
        cq.attemptCount=0
        cq.downloadStage='downloadData1'
        cq.save()
    exit(0)

    stateCodes=['04','11','12','13','18','26','29','03','35','14']
    stateCodes=["18"]
    cqs=CrawlQueue.objects.filter(panchayat__block__district__state__code__in=stateCodes)
    for cq in cqs:
      cq.priority=15
      cq.save()
    exit(0)
    myLibtechTag=LibtechTag.objects.filter(id=1).first()
    priority=10
    with open('/tmp/ah.csv') as fp:
      for line in fp:
        panchayatCode=line.lstrip().rstrip()
        logger.info(panchayatCode)
        eachPanchayat=Panchayat.objects.filter(code=panchayatCode).first()
        cq=CrawlQueue.objects.filter(panchayat=eachPanchayat).first()
        if cq is None:
         CrawlQueue.objects.create(panchayat=eachPanchayat,priority=priority)
    exit(0)
 
    exit(0)
    logger.info("...END PROCESSING") 
  exit(0)
if __name__ == '__main__':
  main()
