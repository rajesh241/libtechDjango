from django.db import models
from django.conf import settings

# Create your models here.

from django.db.models.signals import pre_save,post_save
from django.utils.text import slugify
from django.core.serializers import serialize
import time
import datetime
import os
import json
telanganaStateCode='36'
#File Uploads



class LibtechTag(models.Model):
  '''
  This Model is for generic Tags, we can define Tags, for example Survey2019 and tag any object, panchayat, jobcard etc with the tag. Then the objects can be retrieved by using the tag
  '''
  name=models.CharField(max_length=256,default='NONE')
  description=models.CharField(max_length=256,blank=True,null=True)
  slug=models.SlugField(blank=True) 
  class Meta:
    db_table = 'libtechTag'
  def __str__(self):
    return self.name

  def serialize(self):
    data={
            "id" : self.id,
            "name": self.name,
            "description": self.description
            }
    json_data = json.dumps(data)
    return json_data

class Location(models.Model):
  name=models.CharField(max_length=256)
  displayName=models.CharField(max_length=2048)
  locationType=models.CharField(max_length=64)
  nameInLocalLanguage=models.BooleanField(default=False)
  englishName=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=20,unique=True,db_index=True)
  parentLocation=models.ForeignKey('self',on_delete=models.SET_NULL,blank=True,null=True)
  slug=models.SlugField(blank=True) 
  crawlIP=models.CharField(max_length=256,null=True,blank=True)
  stateShortCode=models.CharField(max_length=2,null=True,blank=True)
  stateCode=models.CharField(max_length=2,null=True,blank=True)
  districtCode=models.CharField(max_length=4,null=True,blank=True)
  blockCode=models.CharField(max_length=7,null=True,blank=True)
  panchayatCode=models.CharField(max_length=10,null=True,blank=True)
  filepath=models.CharField(max_length=2048,null=True,blank=True)
  isNIC=models.BooleanField(default=True)
  remarks=models.TextField(blank=True,null=True)
  priority=models.PositiveSmallIntegerField(default=0)
  class Meta:
    db_table = 'location'
  def __str__(self):
    return self.code

class Info(models.Model):
  name=models.CharField(max_length=256)
  slug=models.SlugField(blank=True) 
  location=models.ForeignKey('Location',on_delete=models.CASCADE)
  finyear=models.CharField(max_length=2,null=True,blank=True)
  value=models.DecimalField(max_digits=16,decimal_places=4,null=True,blank=True)
  textValue=models.CharField(max_length=2048,null=True,blank=True)
  class Meta:
    db_table = 'info'
  def __str__(self):
    if self.finyear is not None:
      return "%s-%s-%s-%s" % (self.name,self.location,self.finyear,str(self.value))
    else:
      return "%s-%s-%s" % (self.name,self.location,str(self.value))
  
class State(models.Model):
  '''
  This is Model for the States. States are identified with unique code, which is based on code on Nrega Website. Additionally each NIC Nrega state has a different server, which is identified with crawlIP. 
  isNIC field is true for State nrega websites which are hosted on NREGA
  '''
  name=models.CharField(max_length=256)
  nameInLocalLanguage=models.BooleanField(default=False)
  englishName=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=2,unique=True,db_index=True)
  slug=models.SlugField(blank=True) 
  crawlIP=models.CharField(max_length=256,null=True,blank=True)
  stateShortCode=models.CharField(max_length=2)
  isNIC=models.BooleanField(default=True)
  class Meta:
    db_table = 'state'
  def __str__(self):
    return self.name

class District(models.Model):
  state=models.ForeignKey('state',on_delete=models.CASCADE)
  name=models.CharField(max_length=256)
  nameInLocalLanguage=models.BooleanField(default=False)
  englishName=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=4,db_index=True,unique=True)
  slug=models.SlugField(blank=True) 
  tcode=models.CharField(max_length=8,blank=True,null=True)
  isEnumerated=models.BooleanField(default=False)
  class Meta:
    db_table = 'district'
  def __str__(self):
    return self.name

class Block(models.Model):
  district=models.ForeignKey('district',on_delete=models.CASCADE)
  name=models.CharField(max_length=256)
  nameInLocalLanguage=models.BooleanField(default=False)
  englishName=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=7,db_index=True,unique=True)
  libtechTag=models.ManyToManyField('LibtechTag',related_name="blockTag",blank=True)
  slug=models.SlugField(blank=True) 
  nicStatURL=models.URLField(max_length=2048,blank=True,null=True)
  tcode=models.CharField(max_length=7,unique=True,null=True,blank=True)
  class Meta:
    db_table = 'block'
  def __str__(self):
    return self.name

class Panchayat(models.Model):
  block=models.ForeignKey('block',on_delete=models.CASCADE)
  name=models.CharField(max_length=256)
  nameInLocalLanguage=models.BooleanField(default=False)
  englishName=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=10,db_index=True,unique=True)
  slug=models.SlugField(blank=True) 
  tcode=models.CharField(max_length=10,blank=True,null=True)
  libtechTag=models.ManyToManyField('LibtechTag',related_name="panchayatTag",blank=True)
  nicStatURL=models.URLField(max_length=2048,blank=True,null=True)
  remarks=models.CharField(max_length=256,blank=True,null=True)
  lastCrawlDate=models.DateTimeField(null=True,blank=True)
  lastCrawlDuration=models.IntegerField(blank=True,null=True)  #This is Duration that last Crawl took in Minutes
  accuracyIndex=models.IntegerField(blank=True,null=True)  #This is Accuracy Index of Last Financial Year
  accuracyIndexAverage=models.IntegerField(blank=True,null=True)
  isDataAccurate=models.BooleanField(default=False)

  class Meta:
    db_table = 'panchayat'
  def __str__(self):
    return "%s-%s-%s-%s-%s" % (self.code,self.block.district.state.name,self.block.district.name,self.block.name,self.name)

class Village(models.Model):
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,null=True,blank=True)
  name=models.CharField(max_length=256,null=True,blank=True)
  code=models.CharField(max_length=256,db_index=True,null=True,blank=True)  #Field only for compatibility with otherlocations not used for TElangana
  slug=models.SlugField(blank=True) 
  tcode=models.CharField(max_length=12,null=True,blank=True)
  class Meta:
    db_table = 'village'

  def __str__(self):
    return self.name

class MISReportURL(models.Model):
  state=models.ForeignKey('state',on_delete=models.CASCADE)
  finyear=models.CharField(max_length=2)
  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  demandRegisterURL=models.URLField(max_length=2048,blank=True,null=True)
  delayPaymentURL=models.URLField(max_length=2048,blank=True,null=True)
  ftoURL=models.URLField(max_length=2048,blank=True,null=True)
  class Meta:
    db_table = 'misReportURL'
  def __str__(self):
    return "%s-%s" % (self.state.slug,self.finyear)
  
class PanchayatCrawlInfo(models.Model):
  panchayat=models.ForeignKey('panchayat',on_delete=models.CASCADE)
  code=models.CharField(max_length=256,db_index=True,null=True,blank=True)  
  accuracy=models.PositiveSmallIntegerField(default=0)
  instanceJSONURL=models.URLField(max_length=2048,blank=True,null=True)
  workDetailJSONURL=models.URLField(max_length=2048,blank=True,null=True)
  dataDownloadDate=models.DateTimeField(null=True,blank=True)
  missingJSONURL=models.URLField(max_length=2048,blank=True,null=True)
  crawlDuration=models.IntegerField(blank=True,null=True,default=0)
  class Meta:
    db_table = 'panchayatCrawlInfo'
  def __str__(self):
    return "%s-%s-%s-%s" % (self.panchayat.block.district.state.name,self.panchayat.block.district.name,self.panchayat.block.name,self.panchayat.name)

#Models for Reports
#The below Model is used to uploading generic reports like on the fly zip of existing reports etc etc
class GenericReport(models.Model):
  name=models.CharField(max_length=256,default='NONE',null=True,blank=True)
  description=models.CharField(max_length=256,blank=True,null=True)
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,null=True,blank=True)
  libtechTag=models.ForeignKey('LibtechTag',on_delete=models.CASCADE,null=True,blank=True)
  updateDate=models.DateTimeField(auto_now=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  class Meta:
    db_table = 'genericReport'
  def __str__(self):
    if (self.panchayat is not None) and (self.libtechTag is not None):
      return "%s-%s" % (self.panchayat.name,self.libtechTag.name)
    else:
      return str(self.id)
#The below Model is used to store block and panchayat level reports
class Report(models.Model):
  location=models.ForeignKey('Location',on_delete=models.CASCADE,null=True,blank=True)
  district=models.ForeignKey('District',on_delete=models.CASCADE,null=True,blank=True)
  block=models.ForeignKey('Block',on_delete=models.CASCADE,null=True,blank=True)
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,null=True,blank=True)
  reportType=models.CharField(max_length=256)
  reportURL=models.URLField(max_length=2048,blank=True,null=True)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  finyear=models.CharField(max_length=2,blank=True,null=True)
  created=models.DateTimeField(auto_now_add=True)
  modified=models.DateTimeField(auto_now=True)
  class Meta:
    unique_together = ('location','district','block', 'panchayat','reportType','finyear')  
    db_table = 'report'
  def __str__(self):
    if self.location is not None:
      return self.location.code+"_"+self.location.name+"-"+self.reportType
    elif self.district is not None:
      return self.district.name+"-"+self.reportType
    elif self.block is not None:
      return self.block.name+"-"+self.reportType
    elif self.panchayat is not None:
      return self.panchayat.name+"-"+self.reportType
    else:
      return reportType

class CrawlState(models.Model):
  name=models.CharField(max_length=256)
  sequence=models.PositiveSmallIntegerField(default=0)
  minhour=models.PositiveSmallIntegerField(default=6)
  maxhour=models.PositiveSmallIntegerField(default=21)
  runChildLevel=models.BooleanField(default=True)
  isBlockLevel=models.BooleanField(default=False)
  isDistrictLevel=models.BooleanField(default=False)
  needFullBlockData=models.BooleanField(default=False)
  iterateFinYear=models.BooleanField(default=True)
  class Meta:
    db_table = 'crawlState'
  def __str__(self):
    return self.name

class CrawlRequest(models.Model):
  libtechTag=models.ManyToManyField('LibtechTag',related_name="crawlReqeustTag",blank=True)
  location=models.ForeignKey('Location',on_delete=models.CASCADE,null=True,blank=True)
  panchayat=models.ForeignKey('panchayat',on_delete=models.CASCADE,null=True,blank=True)
  block=models.ForeignKey('block',on_delete=models.CASCADE,null=True,blank=True)
  district=models.ForeignKey('district',on_delete=models.CASCADE,null=True,blank=True)
  crawlState=models.ForeignKey('CrawlState',on_delete=models.SET_NULL,null=True,blank=True)
  source=models.CharField(max_length=256,default="test")
  sequenceType=models.CharField(max_length=256,default="default")
  processName=models.CharField(max_length=256,blank=True,null=True)
  priority=models.PositiveSmallIntegerField(default=0)
  startFinYear=models.CharField(max_length=2,default='18')
  endFinYear=models.CharField(max_length=2,blank=True,null=True)
  progress=models.PositiveSmallIntegerField(default=0)
  attemptCount=models.PositiveSmallIntegerField(default=0)
  stepError=models.BooleanField(default=False)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)
  crawlStartDate=models.DateTimeField(null=True,blank=True)
  crawlAttemptDate=models.DateTimeField(null=True,blank=True)
  isComplete=models.BooleanField(default=False)
  inProgress=models.BooleanField(default=False)
  isError=models.BooleanField(default=False)
  error=models.TextField(blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  remarks=models.TextField(blank=True,null=True)
  class Meta:
    db_table = 'crawlRequest'
  def __str__(self):
    if self.location is not None:
      return "%s-%s" % (self.location.code,self.location.displayName)
    elif self.panchayat is not  None:
      return "%s-%s-%s-%s-%s" % (self.panchayat.code,self.panchayat.block.district.state.name,self.panchayat.block.district.name,self.panchayat.block.name,self.panchayat.name)
    elif self.block is not  None:
      return "%s-%s-%s-%s" % (self.block.code,self.block.district.state.name,self.block.district.name,self.block.name)
    elif self.district is not  None:
      return "%s-%s-%s" % (self.district.code,self.district.state.name,self.district.name)
    else:
      return self.id


class CrawlQueue(models.Model):
  CRAWL_MODE_OPTIONS = (
        ('FULL', 'FULL'),
        ('PARTIAL', 'PARTIAL'),
        ('ONLYSTATS','ONLYSTATS')
        )
  user        = models.ForeignKey(settings.AUTH_USER_MODEL,on_delete=models.CASCADE,default=1)
  libtechTag=models.ManyToManyField('LibtechTag',related_name="crawlTag",blank=True)
  panchayat=models.ForeignKey('panchayat',on_delete=models.CASCADE,null=True,blank=True)
  block=models.ForeignKey('block',on_delete=models.CASCADE,null=True,blank=True)
  musterDownloadAccuracy=models.PositiveSmallIntegerField(null=True,blank=True)
  accuracy=models.PositiveSmallIntegerField(default=0)
  crawlMode=models.CharField(max_length=32,choices=CRAWL_MODE_OPTIONS,default='FULL')
  downloadStage=models.CharField(max_length=256,default="A1_init")
  source=models.CharField(max_length=256,default="test")
  processName=models.CharField(max_length=256,blank=True,null=True)
  instanceJSONURL=models.URLField(max_length=2048,blank=True,null=True)
  workDetailJSONURL=models.URLField(max_length=2048,blank=True,null=True)
  priority=models.PositiveSmallIntegerField(default=0)
  downloadStep=models.PositiveSmallIntegerField(default=0)
  startFinYear=models.CharField(max_length=2,default='18')
  progress=models.PositiveSmallIntegerField(default=0)
  attemptCount=models.PositiveSmallIntegerField(default=0)
  stepError=models.BooleanField(default=False)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)
  crawlStartDate=models.DateTimeField(null=True,blank=True)
  crawlAttemptDate=models.DateTimeField(null=True,blank=True)
  dataDownloadDate=models.DateTimeField(null=True,blank=True)
  pending=models.IntegerField(blank=True,null=True,default=0)
  crawlDuration=models.IntegerField(blank=True,null=True,default=0)
  isComplete=models.BooleanField(default=False)
  stepStarted=models.BooleanField(default=False)
  stepCompleted=models.BooleanField(default=False)
  inProgress=models.BooleanField(default=False)
  isError=models.BooleanField(default=False)
  isProcessDriven=models.BooleanField(default=False)
  error=models.TextField(blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  remarks=models.TextField(blank=True,null=True)
  class Meta:
    db_table = 'crawlQueue'
  def __str__(self):
    if self.panchayat is not  None:
      return "%s-%s-%s-%s-%s" % (self.panchayat.code,self.panchayat.block.district.state.name,self.panchayat.block.district.name,self.panchayat.block.name,self.panchayat.name)
    elif self.block is not  None:
      return "%s-%s-%s" % (self.block.district.state.name,self.block.district.name,self.block.name)
    else:
      return self.id
  @property
  def owner(self):
    return self.user

class WorkerStat(models.Model):
  worker=models.ForeignKey('Worker',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  finyear=models.CharField(max_length=2)
  workDays=models.IntegerField(blank=True,null=True)
  nicDaysProvided=models.IntegerField(blank=True,null=True)
  totalCredited=models.IntegerField(blank=True,null=True)
  totalPending=models.IntegerField(blank=True,null=True)
  totalRejected=models.IntegerField(blank=True,null=True)
  totalWages=models.IntegerField(blank=True,null=True)
  class Meta:
    unique_together = ( 'worker','finyear')  
    db_table = 'workerStat'
  def __str__(self):
    if self.worker.jobcard.tjobcard is not None:
      displayJobcard=self.worker.jobcard.tjobcard
    else:
      jobcard=self.worker.jobcard.jobcard
    return displayJobcard+"-"+self.worker.name+"-"+finyear


class JobcardStat(models.Model):
  jobcard=models.ForeignKey('Jobcard',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  finyear=models.CharField(max_length=2)
  nicDaysProvided=models.IntegerField(blank=True,null=True,default=0)  # As per period wise work provided report
  nicDaysDemanded=models.IntegerField(blank=True,null=True,default=0)   # As per demand report on panchayat page
  jobcardDaysDemanded=models.IntegerField(blank=True,null=True,default=0)   # as per jobcard page
  jobcardDaysWorked=models.IntegerField(blank=True,null=True,default=0)     # as per jobcard page
  musterDaysProvided=models.IntegerField(blank=True,null=True,default=0)  # as per muster
  musterDaysWorked=models.IntegerField(blank=True,null=True,default=0)   # as per muster
  totalCredited=models.IntegerField(blank=True,null=True)
  totalPending=models.IntegerField(blank=True,null=True)
  totalRejected=models.IntegerField(blank=True,null=True)
  totalWages=models.IntegerField(blank=True,null=True)
  class Meta:
    unique_together = ( 'jobcard','finyear')  
    db_table = 'jobcardStat'
  def __str__(self):
    if self.jobcard.tjobcard is not None:
      displayJobcard=self.jobcard.tjobcard
    else:
      displayJobcard=self.jobcard.jobcard
    return displayJobcard+"-"+self.finyear

class BlockStat(models.Model):
  block=models.ForeignKey('Block',on_delete=models.CASCADE)
  finyear=models.CharField(max_length=2)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)

  bankTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  bankTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  bankTotalRejected=models.IntegerField(blank=True,null=True,default=0)
  bankTotalInvalid=models.IntegerField(blank=True,null=True,default=0)
  bankTotalProcessed=models.IntegerField(blank=True,null=True,default=0)
  bankRejectedURL=models.URLField(max_length=2048,blank=True,null=True)
  bankInvalidURL=models.URLField(max_length=2048,blank=True,null=True)

  postTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  postTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  postTotalRejected=models.IntegerField(blank=True,null=True,default=0)
  postTotalInvalid=models.IntegerField(blank=True,null=True,default=0)
  postTotalProcessed=models.IntegerField(blank=True,null=True,default=0)
  postRejectedURL=models.URLField(max_length=2048,blank=True,null=True)
  postInvalidURL=models.URLField(max_length=2048,blank=True,null=True)

  coBankTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  coBankTotalTransactions=models.IntegerField(blank=True,null=True,default=0)
  coBankTotalRejected=models.IntegerField(blank=True,null=True,default=0)
  coBankTotalInvalid=models.IntegerField(blank=True,null=True,default=0)
  coBankTotalProcessed=models.IntegerField(blank=True,null=True,default=0)
  coBankRejectedURL=models.URLField(max_length=2048,blank=True,null=True)
  coBankInvalidURL=models.URLField(max_length=2048,blank=True,null=True)

  class Meta:
    unique_together = ( 'block','finyear')  
    db_table = 'blockStat'
  def __str__(self):
    return self.block.name+"-"+self.block.district.name
      
class PanchayatStat(models.Model):
  panchayat=models.ForeignKey('panchayat',on_delete=models.CASCADE)
  finyear=models.CharField(max_length=2)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  zeroMusters=models.BooleanField(default=False)
  isActive=models.BooleanField(default=True)
  isDataAvailable=models.BooleanField(default=False)
  accuracy=models.PositiveSmallIntegerField(blank=True,null=True)
  jobcardsTotal=models.IntegerField(blank=True,null=True)
  workersTotal=models.IntegerField(blank=True,null=True)

  
  nicEmploymentProvided=models.IntegerField(blank=True,null=True)  #This is from report 5.1
  nicJobcardsTotal=models.IntegerField(blank=True,null=True)    
  nicWorkersTotal=models.IntegerField(blank=True,null=True) 
  nicDaysDemanded=models.IntegerField(blank=True,null=True)        #This is as per the dmeand report on panchayat page
  nicDaysProvided=models.IntegerField(blank=True,null=True,default=0)  #This is as per period wise workdays report on panchayat page
  
  
  jobcardDaysDemanded=models.IntegerField(blank=True,null=True)   # As per Jobcard Page
  jobcardDaysWorked=models.IntegerField(blank=True,null=True)  # As per jobcard page
  musterDaysProvided=models.IntegerField(blank=True,null=True)  # As per Musters
  musterDaysWorked=models.IntegerField(blank=True,null=True)    # As per Muster
  
  libtechTotalWages=models.IntegerField(blank=True,null=True)
  libtechPendingWages=models.IntegerField(default=0)
  libtechRejectedWages=models.IntegerField(default=0)
  libtechCreditedWages=models.IntegerField(default=0)
  libtechPendingWagesPCT=models.PositiveSmallIntegerField(default=100)
  libtechRejectedWagesPCT=models.PositiveSmallIntegerField(default=0)
  libtechCreditedWagesPCT=models.PositiveSmallIntegerField(default=0)


  mustersTotal=models.IntegerField(blank=True,null=True)
  mustersPending=models.IntegerField(blank=True,null=True)
  mustersDownloaded=models.IntegerField(blank=True,null=True)
  mustersPendingDownloadPCT=models.PositiveSmallIntegerField(default=100)
  mustersInComplete=models.IntegerField(blank=True,null=True)
  musterMissingApplicants=models.IntegerField(blank=True,null=True)

  wagelistTotal=models.IntegerField(blank=True,null=True)
  wagelistPending=models.IntegerField(blank=True,null=True)
  wagelistDownloaded=models.IntegerField(blank=True,null=True)
  wagelistInComplete=models.IntegerField(blank=True,null=True)
  wagelistPendingDownloadPCT=models.PositiveSmallIntegerField(default=100)
   
  musterDownloadAccuracy=models.PositiveSmallIntegerField(default=0)
  jobcardDownloadAccuracy=models.PositiveSmallIntegerField(default=0)
  wagelistDownloadAccuracy=models.PositiveSmallIntegerField(default=0)

  musterTransactionCSV=models.URLField(max_length=2048,blank=True,null=True)
  class Meta:
    unique_together = ( 'panchayat','finyear')  
    db_table = 'panchayatStat'
  def __str__(self):
    return self.panchayat.name+"-"+self.panchayat.block.name

class Jobcard(models.Model):
  panchayat=models.ForeignKey('Panchayat',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  village=models.ForeignKey('Village',on_delete=models.CASCADE,blank=True,null=True)
  libtechTag=models.ManyToManyField('LibtechTag',related_name="jobcardTag",blank=True)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  tjobcard=models.CharField(max_length=24,null=True,blank=True,db_index=True)
  jobcard=models.CharField(max_length=256,db_index=True,null=True,blank=True)
  jcNo=models.BigIntegerField(blank=True,null=True)
  headOfHousehold=models.CharField(max_length=512,blank=True,null=True)
  surname=models.CharField(max_length=512,blank=True,null=True)
  caste=models.CharField(max_length=64,blank=True,null=True)
  applicationDate=models.DateField(null=True,blank=True,auto_now_add=True)
  isVillageInfoMissing=models.BooleanField(default=False)
  isWorkerTableMissing=models.BooleanField(default=False)
  isPaymentTableMissing=models.BooleanField(default=False)
  allApplicantFound=models.BooleanField(default=False)

  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  isDownloaded=models.BooleanField(default=False)
  downloadDate=models.DateTimeField(null=True,blank=True)
  errorDate=models.DateTimeField(null=True,blank=True)
  isError=models.BooleanField(default=False)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)

  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  
  class Meta:
    unique_together = ( 'jobcard','panchayat')  
    db_table = 'jobcard'
  def __str__(self):
    return self.jobcard
# def serialize(self):
#   json_data = serialize('json',[self])
#   return json_data

class Worker(models.Model):
  jobcard=models.ForeignKey('Jobcard',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  name=models.CharField(max_length=512)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  libtechTag=models.ManyToManyField('LibtechTag',related_name="workerTag",blank=True)
  applicantNo=models.PositiveSmallIntegerField()
  fatherHusbandName=models.CharField(max_length=512,blank=True,null=True)
  relationship=models.CharField(max_length=64,blank=True,null=True)
  gender=models.CharField(max_length=256,blank=True,null=True)
  age=models.PositiveIntegerField(blank=True,null=True)
  isDeleted=models.BooleanField(default=False)
  isDisabled=models.BooleanField(default=False)
  isMinority=models.BooleanField(default=False)
  remarks=models.CharField(max_length=512,blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  is15Days=models.BooleanField(default=False)
  isSample30=models.BooleanField(default=False)
  isSample=models.BooleanField(default=False)
  isExtraSample30=models.BooleanField(default=False)
  isExtraSample=models.BooleanField(default=False)
  oldID=models.IntegerField(blank=True,null=True)  #This is Duration that last Crawl took in Minutes
   
  class Meta:
    unique_together = ('jobcard', 'name','applicantNo')  
    db_table = 'worker'
  def __str__(self):
    return self.code
 
class Muster(models.Model):
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,db_index=True,blank=True,null=True)
  block=models.ForeignKey('block',on_delete=models.CASCADE)
  finyear=models.CharField(max_length=2)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  musterNo=models.CharField(max_length=64,db_index=True)
  musterType=models.CharField(max_length=4)
  workCode=models.CharField(max_length=128)
  workName=models.CharField(max_length=2046)
  dateFrom=models.DateField(default=datetime.date.today)
  dateTo=models.DateField(default=datetime.date.today)
  paymentDate=models.DateField(blank=True,null=True)
  musterURL=models.CharField(max_length=4096)
  newMusterURL=models.CharField(max_length=4096,blank=True,null=True)
  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  allApplicantFound=models.BooleanField(default=False)
  allWorkerFound=models.BooleanField(default=False)
  allWDFound=models.BooleanField(default=False)
  isComplete=models.BooleanField(default=False)
  isJSONProcessed=models.BooleanField(default=False)
  newMusterFormat=models.BooleanField(default=True)

  isDownloaded=models.BooleanField(default=False)
  downloadDate=models.DateTimeField(null=True,blank=True)
  errorDate=models.DateTimeField(null=True,blank=True)
  isError=models.BooleanField(default=False)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)

  remarks=models.TextField(blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('musterNo', 'block', 'finyear')  
        db_table="muster"
  def __str__(self):
    return self.musterNo

class Wagelist(models.Model):
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,db_index=True,blank=True,null=True)
  block=models.ForeignKey('block',on_delete=models.CASCADE,db_index=True)
  wagelistNo=models.CharField(max_length=256)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  wagelistURL=models.URLField(max_length=2048,blank=True,null=True)
  finyear=models.CharField(max_length=2,db_index=True)
  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  code=models.CharField(max_length=256,blank=True,null=True)
  generateDate=models.DateField(blank=True,null=True)
  isComplete=models.BooleanField(default=False)
  isDownloaded=models.BooleanField(default=False)
  allFTOFound=models.BooleanField(default=False)
  allDWDFound=models.BooleanField(default=False)
  allWDFound=models.BooleanField(default=False)
  allWorkerFound=models.BooleanField(default=False)
  multiplePanchayats=models.BooleanField(default=False)
  downloadDate=models.DateTimeField(null=True,blank=True)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)
  errorDate=models.DateTimeField(null=True,blank=True)
  isRequired=models.BooleanField(default=False)
  isError=models.BooleanField(default=False)
  remarks=models.TextField(blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('wagelistNo', 'block', 'finyear') 
        db_table="wagelist"
  def __str__(self):
    return self.wagelistNo

class FTO(models.Model):
  panchayat=models.ForeignKey('Panchayat',on_delete=models.CASCADE,db_index=True,blank=True,null=True)
  block=models.ForeignKey('block',on_delete=models.CASCADE,db_index=True)
  ftoNo=models.CharField(max_length=256,db_index=True)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  ftoURL=models.URLField(max_length=2048,blank=True,null=True)
  paymentMode=models.CharField(max_length=64,blank=True,null=True)
  finyear=models.CharField(max_length=2,db_index=True)
  firstSignatoryDate=models.DateField(null=True,blank=True)
  secondSignatoryDate=models.DateField(null=True,blank=True)
  ftofinyear=models.CharField(max_length=2,blank=True,null=True)
  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  allJobcardFound=models.BooleanField(default=False)
  allWagelistFound=models.BooleanField(default=False)
  allWorkerFound=models.BooleanField(default=False)
  allWDFound=models.BooleanField(default=False)
  isRequired=models.BooleanField(default=False)
  isComplete=models.BooleanField(default=False)

  isDownloaded=models.BooleanField(default=False)
  downloadDate=models.DateTimeField(null=True,blank=True)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)
  errorDate=models.DateTimeField(null=True,blank=True)
  isError=models.BooleanField(default=False)
  remarks=models.TextField(blank=True,null=True)

  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)

  class Meta:
        unique_together = ('ftoNo', 'block', 'finyear')  
        db_table = "fto"
  def __str__(self):
    return self.ftoNo


class DPTransaction(models.Model):
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  muster=models.ForeignKey('Muster',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  jobcard=models.CharField(max_length=256,db_index=True,null=True,blank=True)
  name=models.CharField(max_length=512)
  finyear=models.CharField(max_length=2)
  musterNo=models.CharField(max_length=64,db_index=True)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  paymentDate=models.DateField(null=True,blank=True)
  delayDays=models.PositiveSmallIntegerField(null=True,blank=True)
  delayCompensation=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  status=models.CharField(max_length=4096,null=True,blank=True)
  
  class Meta:
        db_table="DPTransaction"
  def __str__(self):
    return self.name+" "+str(self.musterNo)

class RejectionDetail(models.Model):
  referenceNo=models.CharField(max_length=256,null=True,blank=True)
  finyear=models.CharField(max_length=2)
  block=models.ForeignKey('block',on_delete=models.CASCADE)
  wagelist=models.ForeignKey('Wagelist',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  worker=models.ForeignKey('Worker',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  muster=models.ForeignKey('Muster',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  fto=models.ForeignKey('FTO',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  wd=models.ForeignKey('WorkDetail',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  wp=models.ForeignKey('WorkPayment',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  status=models.CharField(max_length=64,null=True,blank=True)
  processDate=models.DateField(null=True,blank=True)
  rejectionReason=models.CharField(max_length=4096,null=True,blank=True)

  class Meta:
        unique_together = ('referenceNo', 'block', 'finyear')  
        db_table="rejectedDetail"
  def __str__(self):
    return self.referenceNo

class RejectedPayment(models.Model):
  referenceNo=models.CharField(max_length=256)
  finyear=models.CharField(max_length=2)
  block=models.ForeignKey('block',on_delete=models.CASCADE)
  code=models.CharField(max_length=256,db_index=True,blank=True,null=True)
  url=models.CharField(max_length=4096,null=True,blank=True)
  contentFileURL=models.URLField(max_length=2048,blank=True,null=True)
  isComplete=models.BooleanField(default=False)

  
  ftoString=models.CharField(max_length=256,blank=True,null=True)
  wagelistString=models.CharField(max_length=256,blank=True,null=True)
  primaryAccountHolder=models.CharField(max_length=256,blank=True,null=True)
  bankCode=models.CharField(max_length=256,blank=True,null=True)
  ifscCode=models.CharField(max_length=256,blank=True,null=True)
  amount=models.DecimalField(max_digits=10,decimal_places=2,null=True,blank=True)
  rejectionReason=models.CharField(max_length=2048,blank=True,null=True)
  rejectionDate=models.DateField(null=True,blank=True)

  wagelist=models.ForeignKey('Wagelist',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  worker=models.ForeignKey('Worker',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  muster=models.ForeignKey('Muster',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  fto=models.ForeignKey('FTO',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  workDetail=models.ForeignKey('WorkDetail',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  workPayment=models.ForeignKey('WorkPayment',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  status=models.CharField(max_length=64,null=True,blank=True)

  isDownloaded=models.BooleanField(default=False)
  downloadDate=models.DateTimeField(null=True,blank=True)
  errorDate=models.DateTimeField(null=True,blank=True)
  isError=models.BooleanField(default=False)
  downloadAttemptCount=models.PositiveSmallIntegerField(default=0)

  remarks=models.TextField(blank=True,null=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('referenceNo', 'block', 'finyear')  
        db_table="rejectedPayment"
  def __str__(self):
    return self.referenceNo

class PaymentTransaction(models.Model):
  fto=models.ForeignKey('FTO',on_delete=models.CASCADE,db_index=True)
  ftoIndex=models.PositiveSmallIntegerField(db_index=True)
  wagelist=models.ForeignKey('Wagelist',on_delete=models.CASCADE,db_index=True,blank=True,null=True)
  jobcard=models.ForeignKey('Jobcard',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  creditedAmount=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  status=models.CharField(max_length=64,null=True,blank=True)
  referenceNo=models.CharField(max_length=256,null=True,blank=True)
  transactionDate=models.DateField(null=True,blank=True)
  processDate=models.DateField(null=True,blank=True)
  rejectionReason=models.CharField(max_length=4096,null=True,blank=True)
  primaryAccountHolder=models.CharField(max_length=4096,null=True,blank=True)
  favorBankAPB=models.CharField(max_length=256,null=True,blank=True)
  IINBankAPB=models.CharField(max_length=256,null=True,blank=True)
  jobcardRaw=models.CharField(max_length=256,db_index=True,null=True,blank=True)
  class Meta:
        db_table="paymentTransaction"
        unique_together = ('fto','ftoIndex')  
  def __str__(self):
    return self.fto.ftoNo+" "+str(self.ftoIndex)


class FTOTransaction(models.Model):
  fto=models.ForeignKey('FTO',on_delete=models.CASCADE,db_index=True)
  ftoIndex=models.PositiveSmallIntegerField(null=True,blank=True)
  wagelist=models.ForeignKey('Wagelist',on_delete=models.CASCADE,db_index=True,blank=True,null=True)
  jobcard=models.ForeignKey('Jobcard',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  creditedAmount=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  status=models.CharField(max_length=64,null=True,blank=True)
  referenceNo=models.CharField(max_length=256,null=True,blank=True)
  transactionDate=models.DateField(null=True,blank=True)
  processDate=models.DateField(null=True,blank=True)
  rejectionReason=models.CharField(max_length=4096,null=True,blank=True)
  primaryAccountHolder=models.CharField(max_length=4096,null=True,blank=True)
  favorBankAPB=models.CharField(max_length=256,null=True,blank=True)
  IINBankAPB=models.CharField(max_length=256,null=True,blank=True)
  jobcardRaw=models.CharField(max_length=256,db_index=True,null=True,blank=True)
  class Meta:
        db_table="ftoTransaction"
        unique_together = ('fto','referenceNo')  
  def __str__(self):
    return self.fto.ftoNo+" "+str(self.ftoIndex)

 
class WagelistTransaction(models.Model):
  wagelist=models.ForeignKey('Wagelist',on_delete=models.CASCADE,db_index=True)
  wagelistIndex=models.PositiveSmallIntegerField(null=True,blank=True)
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  fto=models.ForeignKey('FTO',on_delete=models.CASCADE,null=True,blank=True)
  workDetail=models.ForeignKey('WorkDetail',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  workPayment=models.ForeignKey('WorkPayment',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  rejectedPayment=models.ForeignKey('RejectedPayment',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  daysWorked=models.PositiveSmallIntegerField(null=True,blank=True)
  totalWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  isRegenerated=models.BooleanField(default=False)
  isWagelistFTOAbsent=models.BooleanField(default=False)
  status=models.CharField(max_length=64,null=True,blank=True)
  referenceNo=models.CharField(max_length=256,null=True,blank=True)
  processDate=models.DateField(null=True,blank=True)
  rejectionReason=models.CharField(max_length=4096,null=True,blank=True)
  class Meta:
        db_table="wagelistTransaction"
        unique_together = ('wagelist','wagelistIndex')  
  def __str__(self):
    if self.worker is not None:
      return self.worker.name+" "+str(self.wagelist.wagelistNo)
    else:
      return self.wagelist.wagelistNo

class WorkPayment(models.Model):
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True)
  workDemandDate=models.DateField(db_index=True,null=True,blank=True)
  workAllocatedDate=models.DateField(db_index=True,null=True,blank=True)
  daysDemanded=models.PositiveSmallIntegerField(null=True,blank=True)
  demandID=models.CharField(max_length=256,null=True,blank=True)
  daysAllocated=models.PositiveSmallIntegerField(default=0)
  demandExists=models.BooleanField(default=True)
  finyear=models.CharField(max_length=2,null=True,blank=True)
  amountDue=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  code=models.CharField(max_length=256,blank=True,null=True)
  muster=models.ForeignKey('Muster',on_delete=models.CASCADE,db_index=True,null=True,blank=True)

  daysProvided=models.PositiveSmallIntegerField(null=True,blank=True)
  daysWorked=models.PositiveSmallIntegerField(null=True,blank=True)
  dayWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  totalWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  musterStatus=models.CharField(max_length=64,null=True,blank=True)
  accountNo=models.CharField(max_length=256,null=True,blank=True)
  bankName=models.CharField(max_length=256,null=True,blank=True)
  branchName=models.CharField(max_length=256,null=True,blank=True)
  branchCode=models.CharField(max_length=256,null=True,blank=True)
  creditedDate=models.DateField(null=True,blank=True)
  curWagelist=models.ForeignKey('Wagelist',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  wagelist=models.ManyToManyField('Wagelist',related_name="wpWagelist",blank=True)
  wagelistTransaction=models.ManyToManyField('WagelistTransaction',related_name="wpWglTrn",blank=True)
  rejectedPayment=models.ManyToManyField('RejectedPayment',related_name="wpRejPay",blank=True)
  dpTransaction=models.ForeignKey('DPTransaction',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  isNICDelayAccounted=models.BooleanField(default=False)

  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('worker', 'workDemandDate','workAllocatedDate')  
        db_table="workPayment"
  def __str__(self):
    return str(self.worker)+" "+str(self.workDemandDate)
 
class WorkDetail(models.Model):
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True)
  workDemandDate=models.DateField(db_index=True,null=True,blank=True)
  workAllocatedDate=models.DateField(db_index=True,null=True,blank=True)
  daysDemanded=models.PositiveSmallIntegerField(null=True,blank=True)
  demandID=models.CharField(max_length=256,null=True,blank=True)
  daysAllocated=models.PositiveSmallIntegerField(default=0)
  demandExists=models.BooleanField(default=True)
  finyear=models.CharField(max_length=2,null=True,blank=True)
  amountDue=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  code=models.CharField(max_length=256,blank=True,null=True)
  muster=models.ForeignKey('Muster',on_delete=models.CASCADE,db_index=True,null=True,blank=True)

  daysProvided=models.PositiveSmallIntegerField(null=True,blank=True)
  daysWorked=models.PositiveSmallIntegerField(null=True,blank=True)
  dayWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  totalWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  musterStatus=models.CharField(max_length=64,null=True,blank=True)
  accountNo=models.CharField(max_length=256,null=True,blank=True)
  bankName=models.CharField(max_length=256,null=True,blank=True)
  branchName=models.CharField(max_length=256,null=True,blank=True)
  branchCode=models.CharField(max_length=256,null=True,blank=True)
  creditedDate=models.DateField(null=True,blank=True)
  curWagelist=models.ForeignKey('Wagelist',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  wagelist=models.ManyToManyField('Wagelist',related_name="wdWagelist",blank=True)
  wagelistTransaction=models.ManyToManyField('WagelistTransaction',related_name="wdWglTrn",blank=True)
  rejectedPayment=models.ManyToManyField('RejectedPayment',related_name="wdRejPay",blank=True)
  dpTransaction=models.ForeignKey('DPTransaction',on_delete=models.SET_NULL,db_index=True,null=True,blank=True)
  isNICDelayAccounted=models.BooleanField(default=False)

  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('worker', 'workDemandDate')  
        db_table="workDetail"
  def __str__(self):
    return str(self.worker)+" "+str(self.workDemandDate)
   
class DemandWorkDetail(models.Model):  
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  workDemandDate=models.DateField()
  daysDemanded=models.PositiveSmallIntegerField(null=True,blank=True)
  demandID=models.CharField(max_length=256,null=True,blank=True)
  workAllocatedDate=models.DateField(null=True,blank=True)
  daysAllocated=models.PositiveSmallIntegerField(default=0)
  demandExists=models.BooleanField(default=True)
  finyear=models.CharField(max_length=2,null=True,blank=True)
  amountDue=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  code=models.CharField(max_length=256,blank=True,null=True)

  muster=models.ForeignKey('Muster',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
#  wagelist=models.ManyToManyField('Wagelist',related_name="dwdWagelist",blank=True)
  musterIndex=models.PositiveSmallIntegerField(null=True,blank=True)
  daysProvided=models.PositiveSmallIntegerField(null=True,blank=True)
  daysWorked=models.PositiveSmallIntegerField(null=True,blank=True)
  dayWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  totalWage=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  musterStatus=models.CharField(max_length=64,null=True,blank=True)
  accountNo=models.CharField(max_length=256,null=True,blank=True)
  bankName=models.CharField(max_length=256,null=True,blank=True)
  branchName=models.CharField(max_length=256,null=True,blank=True)
  branchCode=models.CharField(max_length=256,null=True,blank=True)
  creditedDate=models.DateField(null=True,blank=True)
  curWagelist=models.ForeignKey('Wagelist',on_delete=models.CASCADE,db_index=True,null=True,blank=True)

  dpTransaction=models.ForeignKey('DPTransaction',on_delete=models.CASCADE,db_index=True,null=True,blank=True)

  status=models.CharField(max_length=64,null=True,blank=True)
  rejectedFlag=models.BooleanField(default=False)
  inRejectionTable=models.BooleanField(default=False)
  isCredited=models.BooleanField(default=False)
  isNICDelayedPayment=models.BooleanField(default=False)
  isNICDelayAccounted=models.BooleanField(default=False)
  isDelayedPayment=models.BooleanField(default=False)
  attemptCount=models.PositiveSmallIntegerField(null=True,blank=True)
  nicDelayDays=models.PositiveSmallIntegerField(null=True,blank=True)
  delayDays=models.PositiveSmallIntegerField(null=True,blank=True)
  paymentDelay=models.PositiveSmallIntegerField(null=True,blank=True)
  ftoDelay=models.PositiveSmallIntegerField(null=True,blank=True)
  rejectionReason=models.TextField(blank=True,null=True)
  remarks=models.TextField(blank=True,null=True)

  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('worker', 'workDemandDate')  
        db_table="demandWorkDetail"
  def __str__(self):
    return str(self.worker)+" "+str(self.workDemandDate)


class APWorkPayment(models.Model):
  jobcard=models.ForeignKey('Jobcard',db_index=True,on_delete=models.CASCADE,blank=True,null=True)
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  name=models.CharField(max_length=512,null=True,blank=True)
  finyear=models.CharField(max_length=2,blank=True,null=True)
  ftofinyear=models.CharField(max_length=2,blank=True,null=True)
  applicantNo=models.PositiveSmallIntegerField(db_index=True,null=True,blank=True)
  musterNo=models.CharField(max_length=64,db_index=True,null=True,blank=True)
  workCode=models.CharField(max_length=128,null=True,blank=True)
  workName=models.CharField(max_length=2046,null=True,blank=True)
  dateFrom=models.DateField(null=True,blank=True)
  dateTo=models.DateField(null=True,blank=True)
  daysWorked=models.PositiveSmallIntegerField(null=True,blank=True)
  accountNo=models.CharField(max_length=256,blank=True,null=True)
  modeOfPayment=models.CharField(max_length=256,blank=True,null=True)
  payorderAmount=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  payorderNo=models.CharField(max_length=256,null=True,blank=True)
  payorderDate=models.DateField(null=True,blank=True)
  epayorderNo=models.CharField(db_index=True,max_length=256,null=True,blank=True)
  epayorderDate=models.DateField(null=True,blank=True)
  payingAgencyDate=models.DateField(null=True,blank=True)
  creditedDate=models.DateField(null=True,blank=True)
  disbursedAmount=models.DecimalField(max_digits=10,decimal_places=4,null=True,blank=True)
  disbursedDate=models.DateField(null=True,blank=True)
  isDelayedPayment=models.BooleanField(default=False)
  isMusterRecordPresent=models.BooleanField(default=False)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('jobcard', 'epayorderNo')  
        db_table="apWorkPayment" 
  def __str__(self):
    return str(self.id)

class RN6TransactionDetail(models.Model):
  worker=models.ForeignKey('Worker',on_delete=models.CASCADE,db_index=True,null=True,blank=True)
  name=models.CharField(max_length=256,null=True,blank=True)
  transactionDate=models.DateField(null=True,blank=True)
  transactionReference=models.CharField(max_length=256,null=True,blank=True)
  withdrawnAt=models.CharField(max_length=256,null=True,blank=True)
  deposit=models.DecimalField(max_digits=10,decimal_places=2,null=True,blank=True)
  withdrawal=models.DecimalField(max_digits=10,decimal_places=2,null=True,blank=True)
  balance=models.DecimalField(max_digits=10,decimal_places=2,null=True,blank=True)
  created=models.DateTimeField(null=True,blank=True,auto_now_add=True)
  modified=models.DateTimeField(null=True,blank=True,auto_now=True)
  class Meta:
        unique_together = ('worker', 'transactionDate','transactionReference')  
        db_table="rn6TransactionDetail" 
  def __str__(self):
    return self.transactionReference
  
class LanguageDict(models.Model):
  phrase1=models.CharField(max_length=1024)
  lang1=models.CharField(max_length=1024)
  phrase2=models.CharField(max_length=1024,null=True,blank=True)
  lang2=models.CharField(max_length=1024,null=True,blank=True)
  class Meta:
        db_table="languageDict" 
  def __str__(self):
    return "%s-%s" % (self.phrase1+self.lang1)


def createslug(instance):
  try:
    myslug=slugify(instance.englishName)[:50]
  except:
    myslug=slugify(instance.name)[:50]
  if myslug == '':
    if hasattr(instance, 'code'):
      myslug="%s-%s" % (instance.__class__.__name__ , str(instance.code))
    else:
      myslug="%s-%s" % (instance.__class__.__name__ , str(instance.id))
  return myslug


def location_post_save_receiver(sender,instance,*args,**kwargs):
  myslug=createslug(instance)
  if instance.slug != myslug:
    instance.slug = myslug
    instance.save()

def village_post_save_receiver(sender,instance,*args,**kwargs):
  modified=False
  myslug=slugify(instance.name)[:50]
  if myslug == '':
    myslug="%s-%s" % (instance.__class__.__name__ , str(instance.id))
  if instance.slug != myslug:
    instance.slug = myslug
    modified=True
  code="%s_%s" % (instance.panchayat.code,instance.name)
  if instance.code != code:
    instance.code=code
    modified=True
  if modified == True:
    instance.save()



def blockStat_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s_%s" % (instance.block.district.state.name,instance.block.district.name,instance.block.code)
  if instance.code != code:
    instance.code=code
    instance.save()
def panchayatStat_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s" % (instance.panchayat.code,instance.finyear)
  if instance.code != code:
    instance.code=code
    instance.save()

def jobcard_post_save_receiver(sender,instance,*args,**kwargs):
  modified=False
  if instance.panchayat.block.district.state.code == telanganaStateCode:
    code=instance.tjobcard
  else:
    code=instance.jobcard
  if instance.code != code:
    instance.code=code
    modified=True
  if modified==True:
    instance.save()

def wd_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s_%s_%s" % (instance.muster.block.code,instance.muster.finyear,instance.muster.musterNo,instance.musterIndex)
  if instance.code != code:
    instance.code=code
    instance.save()
def muster_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s_%s" % (instance.block.code,instance.finyear,instance.musterNo)
  if instance.code != code:
    instance.code=code
    instance.save()
def rejectedPayment_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s_%s" % (instance.block.code,instance.finyear,instance.referenceNo)
  if instance.code != code:
    instance.code=code
    instance.save()
def wagelist_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s_%s" % (instance.block.code,instance.finyear,instance.wagelistNo)
  if instance.code != code:
    instance.code=code
    instance.save()

def fto_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s" % (instance.block.code,instance.ftoNo)
  if instance.code != code:
    instance.code=code
    instance.save()
def panchayatCrawlInfo_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s" % (instance.panchayat.code)
  if instance.code != code:
    instance.code=code
    instance.save()


def workDemand_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s" % (instance.worker.code,str(instance.workDemandDate))
  if instance.code != code:
    instance.code=code
    instance.save()

def worker_post_save_receiver(sender,instance,*args,**kwargs):
  code="%s_%s" % (instance.jobcard.jobcard,instance.name)
  if instance.code != code:
    instance.code=code
    instance.save()

def crawlRequest_post_save_receiver(sender,instance,*args,**kwargs):
  if instance.panchayat is not None:
    sc=instance.panchayat.block.district.state.code
  elif instance.block is not None:
    sc=instance.block.district.state.code
  else:
    sc=None
  if instance.crawlState is None:
    if instance.sequenceType == 'pension':
      cs=CrawlState.objects.filter(sequence=300).first()
    elif instance.sequenceType == 'dd':
      cs=CrawlState.objects.filter(sequence=200).first()
    else:
      if sc == telanganaStateCode:
         cs=CrawlState.objects.filter(sequence=100).first()
      else:
         cs=CrawlState.objects.filter(sequence=1).first()
    instance.crawlState=cs
    instance.save() 
     
def report_post_save_receiver(sender,instance,*args,**kwargs):
  if instance.location is not None:
    code="%s_%s_%s" % (instance.location.code,instance.finyear,instance.reportType)
  elif instance.panchayat is not None:
    code="%s_%s_%s" % (instance.panchayat.code,instance.finyear,instance.reportType)
  elif instance.block is not None:
    code="%s_%s_%s" % (instance.block.code,instance.finyear,instance.reportType)
  else:
    code="%s_%s_%s" % (instance.district.code,instance.finyear,instance.reportType)
  if instance.code != code:
    instance.code=code
    instance.save()
#  print(instance.__class__.__name__)
post_save.connect(report_post_save_receiver,sender=Report)
post_save.connect(panchayatStat_post_save_receiver,sender=PanchayatStat)
post_save.connect(blockStat_post_save_receiver,sender=BlockStat)
post_save.connect(worker_post_save_receiver,sender=Worker)
post_save.connect(jobcard_post_save_receiver,sender=Jobcard)
post_save.connect(muster_post_save_receiver,sender=Muster)
post_save.connect(crawlRequest_post_save_receiver,sender=CrawlRequest)
post_save.connect(rejectedPayment_post_save_receiver,sender=RejectedPayment)
post_save.connect(wagelist_post_save_receiver,sender=Wagelist)
post_save.connect(fto_post_save_receiver,sender=FTO)
post_save.connect(location_post_save_receiver,sender=Location)
post_save.connect(location_post_save_receiver,sender=State)
post_save.connect(location_post_save_receiver,sender=District)
post_save.connect(location_post_save_receiver,sender=Block)
post_save.connect(location_post_save_receiver,sender=Panchayat)
post_save.connect(village_post_save_receiver,sender=Village)
post_save.connect(location_post_save_receiver,sender=LibtechTag)
post_save.connect(location_post_save_receiver,sender=Info)
post_save.connect(panchayatCrawlInfo_post_save_receiver,sender=PanchayatCrawlInfo)

