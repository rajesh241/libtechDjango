import logging
from bs4 import BeautifulSoup
import requests

isBihar = False

try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1

def loggerFetch(level=None,filename=None):
  defaultLogLevel="debug"
  logFormat = '%(asctime)s:[%(name)s|%(module)s|%(funcName)s|%(lineno)s|%(levelname)s]: %(message)s' #  %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s"
  if filename is not None:
    logger = logging.getLogger(filename)
  else:
    logger = logging.getLogger(__name__)

  logging.basicConfig()
  logging.getLogger().setLevel(logging.DEBUG)
  requests_log = logging.getLogger("requests.packages.urllib3")
  requests_log.setLevel(logging.DEBUG)
  requests_log.propagate = True

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
    crawlerLogDir = '/tmp/'
    filename = 'run.log'
    filename1="%s/%s/%s" % (crawlerLogDir,"info",filename)
    fh = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fh.setFormatter(formatter)
    #fh.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
  if filename is not None:
    filename1="%s/%s/%s" % (crawlerLogDir,"debug",filename)
    fhd = RotatingFileHandler(filename1, maxBytes=5000000, encoding="utf-8",backupCount=10)
    fhd.setFormatter(formatter)
    fhd.setLevel(logging.DEBUG)
    logger.addHandler(fhd)

  return logger



class PanchayatCrawler:
  def __init__(self, d):
    self.panchayatCode=d['panchayatCode']
    self.blockCode=d['blockCode']
    self.blockName=d['blockName']
    self.districtCode=d['districtCode']
    self.districtName=d['districtName']
    self.stateCode=d['stateCode']
    self.stateName=d['stateName']
    self.crawlIP=d['crawlIP']


def getJobcardStat(logger,pobj,finyear):
  fullFinYear="2017-2018"
  stathtml=None
  url="http://%s/netnrega/state_html/empspecifydays.aspx?page=P&lflag=eng&state_name=%s&state_code=%s&district_name=%s&district_code=%s&block_name=%s&fin_year=%s&Block_code=%s&"  % (pobj.crawlIP,pobj.stateName,pobj.stateCode,pobj.districtName,pobj.districtCode,pobj.blockName,fullFinYear,pobj.blockCode)
  url = 'http://mnregaweb2.nic.in/netnrega/state_html/empspecifydays.aspx?page=P&lflag=eng&state_name=ODISHA&state_code=24&district_name=GAJAPATI&district_code=2424&block_name=MOHONA&fin_year=2017-2018&Block_code=2424004&'

  logger.info(url)
  with requests.Session() as session:
    #session.headers.update({'x-test': 'true'})
    session.headers['user-agent'] = 'Mozilla/5.0'
    r=session.get(url)
    #r=requests.get(url)
    print(str(r))
    if r.status_code ==200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
      #logger.info(validation)
      viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
      #logger.info(viewState)
      cookies=session.cookies
      logger.info('Cookies: ' + str(cookies)) #  + '==' + r.text)
      #logger.info('Request Cookies: [%s]' % str(r.request.cookies))
      logger.info('Request Headers: [%s]' % str(r.request.headers))
      headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
        'Host': pobj.crawlIP,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
        'X-MicrosoftAjax': 'Delta=true',
      } 

      params = (
        ('page', 'P'),
        ('lflag', 'eng'),
        ('state_name', 'ODISHA'),
        ('state_code', '24'),
        ('district_name', 'GAJAPATI'),
        ('district_code', '2424'),
        ('block_name', 'MOHONA'),
        ('fin_year', '2017-2018'),
        ('Block_code', '2424004'),
        ('', ''),
      )

      data = {
        'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$btn_pro',
        'ctl00$ContentPlaceHolder1$ddr_panch': '2424004002',
        'ctl00$ContentPlaceHolder1$ddr_cond': 'gt',
        'ctl00$ContentPlaceHolder1$lbl_days': '0',
        'ctl00$ContentPlaceHolder1$rblRegWorker': 'Y',
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btn_pro',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': viewState,
        '__EVENTVALIDATION': validation,
        '__VIEWSTATEENCRYPTED': '',
        '__ASYNCPOST': 'true',
        '': ''
      }
      url2='http://%s/netnrega/state_html/empspecifydays.aspx' % (pobj.crawlIP)
      #response = session.post(url2, headers=headers, params=params, cookies=cookies, data=data)
      response = session.post(url2, headers=headers, params=params, data=data, allow_redirects=False)
      if response.status_code == 200:
        logger.info("correct")
        cookies=session.cookies
        logger.info('Yippie = [%s]' % cookies)
        #cookies = requests.cookies.RequestsCookieJar()
        #logger.info('Oopsie = [%s]' % cookies)
        outhtml=r.content
        url3='http://%s/netnrega/state_html/empprovdays.aspx?lflag=eng&fin_year=%s&RegWorker=Y' % (pobj.crawlIP,fullFinYear)
        logger.info(url3)
        r1=session.get(url3, headers=headers, cookies=cookies)
        if r1.status_code == 200:
          stathtml=r1.content
          with open("/tmp/c.html" ,"wb") as f:
            f.write(stathtml)
          return stathtml

def main():
  logger = loggerFetch()
  if isBihar:
    d={
      'stateCode' : '34',
      'stateName' : 'Jharkhand',
      'crawlIP' : 'nregasp2.nic.in',
      'districtName' : 'LOHARDAGA',
      'districtCode': '3402',
      'blockCode' : '3402007',
      'blockName' : 'PESHRAR',
      'panchayatCode' : '3402003002',
    }
  else:
    d={
      'stateCode' : '24',
      'stateName' : 'ODISHA',
      'crawlIP' :  'mnregaweb2.nic.in',
      'districtName' : 'GAJAPATI',
      'districtCode': '2424',
      'blockCode' : '2424004',
      'blockName' : 'MOHONA',
      'panchayatCode' : '2424004033',
    }
  pobj=PanchayatCrawler(d)
  #pobj=PanchayatCrawler(d)
  getJobcardStat(logger,pobj,'18')
  logger.info("wow")
  
if __name__ == '__main__':
  main()
