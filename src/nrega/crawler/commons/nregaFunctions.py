
import re
from bs4 import BeautifulSoup
from config.defines import djangoSettings,crawlerLogDir
import sys
import time
import os
import logging
from logging.handlers import RotatingFileHandler
import datetime
import django
from libtech.settings import AWS_STORAGE_BUCKET_NAME,AWS_S3_REGION_NAME,MEDIA_URL,S3_URL
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()

from nrega.models import State,District,Block,Panchayat,Muster,Report,LanguageDict

def loggerFetch(level=None,filename=None):
  defaultLogLevel="debug"
  logFormat = '%(asctime)s:[%(name)s|%(module)s|%(funcName)s|%(lineno)s|%(levelname)s]: %(message)s' #  %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s"
  if filename is not None:
    logger = logging.getLogger(filename)
  else:
    logger = logging.getLogger(__name__)

  if not level:
    level = defaultLogLevel
  
  if level:
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
      raise ValueError('Invalid log level: %s' % level)
    else:
      logger.setLevel(numeric_level)
  ch = logging.StreamHandler()
  formatter = logging.Formatter(logFormat)
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  if filename is not None:
    filename1="%s/%s/%s" % (crawlerLogDir,"info",filename)
    fh = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
  if filename is not None:
    filename1="%s/%s/%s" % (crawlerLogDir,"debug",filename)
    fhd = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fhd.setFormatter(formatter)
    fhd.setLevel(logging.DEBUG)
    logger.addHandler(fhd)
  return logger


def datetimeDifference(fromDate,toDate=None):
  if toDate is None:
#    toDate=datetime.datetime.now()
    toDate=timezone.now()
  dateDiff=toDate-fromDate
  return dateDiff

def dateDifference(fromDate,toDate=None):
  if toDate is None:
    toDate=datetime.datetime.now().date()
  dateDiff=toDate-fromDate
  return dateDiff.days

def is_ascii(s):
  return all(ord(c) < 128 for c in s)

def savePanchayatReport(logger,eachPanchayat,finyear,reportType,filename,filecontent):
  myReport=Report.objects.filter(panchayat=eachPanchayat,finyear=finyear,reportType=reportType).first()
  if myReport is None:
    Report.objects.create(panchayat=eachPanchayat,finyear=finyear,reportType=reportType)   
    logger.info("Report Created")
  else:
    logger.info("Report Already Exists")
  myReport=Report.objects.filter(panchayat=eachPanchayat,finyear=finyear,reportType=reportType).first()
  myReport.reportFile.save(filename, ContentFile(filecontent))
  myReport.save()

def dateStringToDateObject(dateString):
  datetimeObject=None
  if "-" in dateString:
    try:
      datetimeObject=datetime.datetime.strptime(dateString, '%d-%m-%Y')
    except:
      datetimeObject=None
  return datetimeObject

def getEncodedData(s):
  try:
    s1=s.encode("UTF-8")
  except:
    s1=s
  return s1

def getFinYear(dateObj=None):
  if dateObj is None:
    now = datetime.datetime.now()
  else:
    now=dateObj
  month=now.month
  if now.month > 3:
    year=now.year+1
  else:
    year=now.year
  finyear=str(year % 100)
  if len(finyear) == 1:
    finyear="0"+finyear
  return finyear


def getCurrentFinYear():
  now = datetime.datetime.now()
  month=now.month
  if now.month > 3:
    year=now.year+1
  else:
    year=now.year
  return year% 100

def getTelanganaDate(myDateString,dateType):
  outdate=None
  if (myDateString != '') and (myDateString!= '-'):
    if dateType=="smallYear":
      outdate=datetime.datetime.strptime(myDateString[:9], '%d-%b-%y')
    elif dateType=="bigYear":
      outdate=datetime.datetime.strptime(myDateString[:11], '%d-%b-%Y')
  return outdate

def getDateObj(myDateString,dateFormat=None):
  try:
    if dateFormat is not None:
      myDate=datetime.datetime.strptime(myDateString,dateFormat).date()
    elif "/" in myDateString:
      myDate = datetime.datetime.strptime(myDateString, '%d/%m/%Y').date()
    else:
      myDate = datetime.datetime.strptime(myDateString, '%d-%m-%Y').date()
  except:
    myDate=None
  return myDate

def correctDateFormat(myDateString,dateFormat=None):
  if myDateString != '':
    try:
      if dateFormat is not None:
        myDate=time.strptime(myDateString,dateFormat)
      elif "/" in myDateString:
        myDate = time.strptime(myDateString, '%d/%m/%Y')
      else:
        myDate = time.strptime(myDateString, '%d-%m-%Y')
      myDate = time.strftime('%Y-%m-%d', myDate)
    except:
      myDate=None
  else:
    myDate=None
  return myDate

def getFullFinYear(shortFinYear):
  shortFinYear_1 = int(shortFinYear) -1
  fullFinYear="20%s-20%s" % (str(shortFinYear_1), str(shortFinYear))
  return fullFinYear

def saveBlockReport(logger,eachBlock,finyear,reportType,filename,filecontent):
  myReport=Report.objects.filter(block=eachBlock,finyear=finyear,reportType=reportType).first()
  if myReport is None:
    Report.objects.create(block=eachBlock,finyear=finyear,reportType=reportType)   
    logger.info("Report Created")
  else:
    logger.info("Report Already Exists")
  myReport=Report.objects.filter(block=eachBlock,finyear=finyear,reportType=reportType).first()
  myReport.reportFile.save(filename, ContentFile(filecontent))
  myReport.save()



def saveVillageReport(logger,eachVillage,finyear,reportType,filename,filecontent):
  myReport=VillageReport.objects.filter(village=eachVillage,finyear=finyear,reportType=reportType).first()
  if myReport is None:
    VillageReport.objects.create(village=eachVillage,finyear=finyear,reportType=reportType)   
    logger.info("Report Created")
  else:
    logger.info("Report Already Exists")
  myReport=VillageReport.objects.filter(village=eachVillage,finyear=finyear,reportType=reportType).first()
  myReport.reportFile.save(filename, ContentFile(filecontent))
  myReport.save()

def getjcNumber(jobcard):
  jobcardArray=jobcard.split('/')
#  print(jobcardArray[1])
  if len(jobcardArray) > 1:
    jcNumber=re.sub("[^0-9]", "", jobcardArray[1])
  else:
    jcNumber='0'
  return jcNumber

def getVilCode(jobcard):
  jobcardArray=jobcard.split('/')
  jobcardFirst=jobcardArray[0]
  jlastArray=jobcardFirst.split("-")
  vilCode=jlastArray[-1]
  return vilCode
  

def htmlWrapperLocal(title = None, head = None, body = None):
  html_text = '''
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!-- The above 3 meta tags *must* come first in the head; any other head content must come *after* these tags -->
    
    <title>title_text</title>

    <!-- Bootstrap -->

    <!-- Latest compiled and minified CSS -->
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/static/css/libtech.css">

    <!-- Optional theme -->
    <link rel="stylesheet" href="/%s/static/css/bootstrap-theme.min.css">

    <div align="center">head_text</div>

  </head>
    
  <body>

    body_text
    
    <!-- jQuery (necessary for Bootstrap"s JavaScript plugins) -->
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
    <!-- Include all compiled plugins (below), or include individual files as needed -->

    <!-- Latest compiled and minified JavaScript -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>

  </body>
</html>
''' % (AWS_STORAGE_BUCKET_NAME)
  html_text = html_text.replace('title_text', title)
  html_text = html_text.replace('head_text', head)
  html_text = html_text.replace('body_text', body)

  return html_text
def table2csv(table):
  outcsv=''
  rows=table.findAll('tr')
  for eachRow in rows:
    thCols=eachRow.findAll('th')
    if len(thCols) > 0:
      for eachTD in thCols:
        outcsv+='%s,' % eachTD.text

    tdCols=eachRow.findAll('td')
    if len(tdCols) > 0:
      for eachTD in tdCols:
        outcsv+='%s,' % eachTD.text
    outcsv+='\n'

  return outcsv

def stripTableAttributesOrdered(inhtml,tableID):
  tableHTML=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  rows=inhtml.findAll('tr')
  for eachRow in rows:
    tableHTML+='<tr>'
    thCols=eachRow.findAll(['th','td'])
    if len(thCols) > 1:
      for eachTD in thCols:
        tableHTML+='<td>%s</td>' % eachTD.text
    tableHTML+='</tr>'

  tableHTML+='</table>'
  return tableHTML

def csv2Table(s,tableID=None):
  i=0
  if tableID is None:
    tableID="libtechTable"
  outhtml=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  outhtml+='<table %s>' % classAtt
  for line in s.splitlines():
    outhtml+='<tr>'
    i=i+1
    values=line.split(",")
    for eachValue in values:
      if i==1:
        outhtml+="<th>%s</th>" % eachValue
      else:
        outhtml+="<td>%s</td>" % eachValue
    outhtml+='</tr>'
  outhtml+="</table>"
  return outhtml

def array2HTMLTable(tableArray,tableID=None):
  tableHTML=''
  if tableID is None:
    tableID="libtechTable"
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  isHeader=True
  for row in tableArray:
    tableHTML+='<tr>'
    for col in row:
      if isHeader ==  True:
        tableHTML+='<th>%s</th>' % col
      else:
        tableHTML+='<td>%s</td>' % col
    tableHTML+='</tr>'
    isHeader=False
  tableHTML+='</table>'
  return tableHTML


def stripTableAttributesPreserveLinks(inhtml,tableID,baseURL):
  tableHTML=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  rows=inhtml.findAll('tr')
  for eachRow in rows:
    thCols=eachRow.findAll('th')
    if len(thCols) >= 1:
     tableHTML+='<tr>'
     for eachTD in thCols:
       tableHTML+='<th>%s</th>' % eachTD.text
     tableHTML+='</tr>'

    tdCols=eachRow.findAll('td')
    #print("Length of tdCOls=%s" % (str(len(tdCols))))
    if len(tdCols) >= 1:
      tableHTML+='<tr>'
      for eachTD in tdCols:
        a=eachTD.find("a")
        if a is not None:
          myLink="%s%s" %(baseURL,a['href'])
          tableHTML+='<td><a href="%s">%s</a></td>' % (myLink,eachTD.text)
        else:
          tableHTML+='<td>%s</td>' % eachTD.text
      tableHTML+='</tr>'

  tableHTML+='</table>'
  return tableHTML


def stripTableAttributes(inhtml,tableID):
  tableHTML=''
  classAtt='id = "%s" border=1 class = " table table-striped"' % tableID
  tableHTML+='<table %s>' % classAtt
  rows=inhtml.findAll('tr')
  for eachRow in rows:
    thCols=eachRow.findAll('th')
    if len(thCols) >= 1:
     tableHTML+='<tr>'
     for eachTD in thCols:
       tableHTML+='<th>%s</th>' % eachTD.text
     tableHTML+='</tr>'

    tdCols=eachRow.findAll('td')
    #print("Length of tdCOls=%s" % (str(len(tdCols))))
    if len(tdCols) >= 1:
      tableHTML+='<tr>'
      for eachTD in tdCols:
        tableHTML+='<td>%s</td>' % eachTD.text
      tableHTML+='</tr>'

  tableHTML+='</table>'
  return tableHTML



def getCenterAlignedHeading(text):
  return '<div align="center"><h2>%s</h2></div>' % text

def changeLanguage(logger,lang1,lang2,phrase1):
  d=LanguageDict.objects.filter(lang1=lang1,lang2=lang2,phrase1=phrase1).first()
  if d is not None:
    return d.phrase2
  else:
    return ''
      
