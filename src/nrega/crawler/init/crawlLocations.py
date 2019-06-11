import json
import collections
import os
import sys
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse,parse_qs
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
print(rootDir)
sys.path.insert(0, rootDir)
from config.defines import djangoSettings
from nrega.crawler.commons.nregaFunctions import loggerFetch
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.db.models import F,Q,Sum,Count
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlState,Location
districtTableID="gvdist"
def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='These scripts will initialize the Database for the district and populate relevant details')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-c', '--crawl', help='Crawl Locations', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--test', help='A Test Loop', required=False,action='store_const', const=1)

  args = vars(parser.parse_args())
  return args


def makehash():
  return collections.defaultdict(makehash)

def getQueryField(url, field):
  try:
      valueList=parse_qs(urlparse(url).query)[field]
  except KeyError:
      valueList=[]
  if len(valueList) == 1:
    return valueList[0]
  else:
    return None

def exampleLocationDict():
  p={}
  p['name']=''
  p['stateCode']=''
  p['districtCode']=''
  p['blockCode']=''
  p['panchayatCode']=''
  p['stateName']=''
  p['districtName']=''
  p['blockName']=''
  p['panchayatName']=''
  return p
def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['crawl']:
    logger.info("This subroutine will crawl Locations")
    baseURL="https://nrega.nic.in/Netnrega/stHome.aspx"
    baseURL="http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
    r=requests.get(baseURL)
    locationDict=makehash() 
    exampleDict=exampleLocationDict()
    stateDict={}
    if r.status_code == 200:
      logger.info("page downloaded successfully")
      myhtml=r.content
      mysoup=BeautifulSoup(myhtml,"lxml")
      stateSelect=mysoup.find("select",id="ddl_state")
      if stateSelect is not None:
        p={}
        options=stateSelect.findAll("option")
        for eachOption in options:
          name=eachOption.text
          code=eachOption['value']
          logger.info(f'name {name} code {code}')
          p=exampleDict
          p['name']=name
          p['stateName']=name
#     s="state_code="
#     allStateLinks=mysoup.findAll("a", href=re.compile(s))
#     for eachStateLink in allStateLinks:
#       eachStateURL=eachStateLink["href"]
#       stateCode=getQueryField(eachStateURL,"state_code")        
#       stateName=getQueryField(eachStateURL,"state_name")        
#       logger.info(stateCode+stateName)
#       response=requests.get(eachStateURL+"&lflag=engaaaaaaaaaaa      n   n          x zxsxc sxxxxxxxzx")
#       if response.status_code == 200:
#         stateHTML=response.content
#         stateSoup=BeautifulSoup(stateHTML,"lxml")
#         distTable=stateSoup.find("table",id=districtTableID)
#         if distTable is not None:
#           logger.info("Found District Table")
#           allDistrictLinks=mysoup.findAll("a", href=re.compile("district_code="))
#           for eachDistrictLink in allDistrictLinks:
#             eachDistrictURL=eachDistrictLink["href"]
#         #example U:Rl = https://nrega.nic.in/netnrega/Homedist.aspx?flag_debited=S&is_statefund=Y&lflag=eng&district_code=2602&district_name=AMRITSAR&state_name=PUNJAB&state_Code=26
         
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
