from django.contrib import admin
from django.db.models import F,Q,Sum,Count
from django.utils.safestring import mark_safe

import time
# Register your models here.
from .models import State,District,Block,Panchayat,Muster,Wagelist,FTO,CrawlQueue,Report,Jobcard,LibtechTag,Worker,APWorkPayment,Village,PanchayatCrawlInfo,PanchayatStat,WagelistTransaction,DPTransaction,DemandWorkDetail,MISReportURL,RejectedPayment,WorkDetail,JobcardStat,CrawlState,CrawlRequest,PaymentTransaction,WorkPayment
from .actions import download_reports_zip,setisError,setisProcessedFalse,setisActiveFalse,setisActiveTrue,removeTags,setIsSampleFalse,resetTaskInProgress,resetStepStarted,setInProgress,resetInProgress,resetAttemptCount,export_as_csv_action
from config.defines import hostname,hostport

class stateModelAdmin(admin.ModelAdmin):
  list_display = ["name","stateShortCode","code","crawlIP"]
  class Meta:
    model=State

class districtModelAdmin(admin.ModelAdmin):
  list_display = ["name","code"]
  list_display_links=["name"]
  list_filter=["state"]
  search_fields=["name","code"]
  class Meta:
    model=District

class villageModelAdmin(admin.ModelAdmin):
  list_display = ["id","name","slug","code"]
  list_display_links=["name"]
  list_filter=["panchayat__block__district__state"]
  search_fields=["name","code"]
  readonly_fields=["panchayat"]
class blockModelAdmin(admin.ModelAdmin):
  list_display = ["name","englishName","code"]
  list_display_links=["name"]
  list_filter=["nameInLocalLanguage","district__state"]
  search_fields=["name","code"]
  
  def get_crawlRequest_link(self,obj):
    url="http://%s:%s/api/crawldatarequest/?code=%s" % (hostname,hostport,obj.code)
    myhtml='<a href="%s">Crawl</a>' % url
    return mark_safe(myhtml)
  get_crawlRequest_link.allow_tags = True
  get_crawlRequest_link.description='Download'

  class Meta:
    model=Block

class misReportURLModelAdmin(admin.ModelAdmin):
  list_display = ["id","state","finyear"]
  list_filter=["finyear"]


class panchayatModelAdmin(admin.ModelAdmin):
  list_display = ["__str__","name","code"]
  list_filter=["block__district__state"]
  search_fields=["name","code"]
  readonly_fields=["block","name","remarks","code"]
  def get_crawlRequest_link(self,obj):
    url="http://%s:%s/api/crawldatarequest/?code=%s" % (hostname,hostport,obj.code)
    myhtml='<a href="%s">Crawl</a>' % url
    return mark_safe(myhtml)
  get_crawlRequest_link.allow_tags = True
  get_crawlRequest_link.description='Download'


class panchayatCrawlInfoModelAdmin(admin.ModelAdmin):
  list_display=["id","panchayat"]
  readonly_fields=["panchayat"]
  search_fields=["panchayat__code"]
class panchayatStatModelAdmin(admin.ModelAdmin):
  actions = [export_as_csv_action("CSV Export")]
  list_display=["id","panchayat","finyear","zeroMusters"]
  readonly_fields=["panchayat"]
  search_fields=["panchayat__block__code","panchayat__code"]
class crawlRequestModelAdmin(admin.ModelAdmin):
  actions=[resetAttemptCount,setInProgress,resetInProgress]
  list_display = ["id","__str__","crawlState","inProgress","processName","priority","attemptCount","modified"]
  readonly_fields=["panchayat","block","progress"]
  list_filter=["source","inProgress","crawlState__name","attemptCount","panchayat__block__district__state__name"]
  search_fields=["panchayat__code"]
class crawlStateModelAdmin(admin.ModelAdmin):
  list_display=["name","sequence"]

class crawlQueueModelAdmin(admin.ModelAdmin):
  actions=[resetAttemptCount,resetStepStarted,setInProgress,resetInProgress]
  list_display = ["id","__str__","downloadStage","accuracy","musterDownloadAccuracy","inProgress","processName","priority","attemptCount","modified"]
  readonly_fields=["panchayat","block","startFinYear","progress"]
  list_filter=["source","inProgress","downloadStage","attemptCount","panchayat__block__district__state__name"]
  search_fields=["panchayat__code"]
# def get_queryset(self, request):
#   qs = super(panchayatModelAdmin, self).get_queryset(request)
#   if request.user.is_superuser:
class musterModelAdmin(admin.ModelAdmin):
  actions = [setisError,setisProcessedFalse]
  list_display = ["id","musterNo","finyear","panchayat","modified","workCode","workName"]
  readonly_fields=["block","panchayat"]
  list_filter=["finyear","isDownloaded","newMusterFormat","isComplete","downloadAttemptCount","allWorkerFound","allWDFound"]
  search_fields=["id","musterNo","code","block__code","panchayat__code","workCode"]
#  list_filter=["isPanchayatNull","panchayat__crawlRequirement","finyear","allWorkerFound","isDownloaded","isProcessed","allApplicantFound","block__district__state"]
class wagelistModelAdmin(admin.ModelAdmin):
  list_display=["id","wagelistNo","block"]
  readonly_fields=["block","panchayat"]
  search_fields=["wagelistNo","panchayat__code"]
  list_filter=["isDownloaded","isComplete","allWorkerFound","allWDFound","allFTOFound","isRequired","isError"]

class FTOModelAdmin(admin.ModelAdmin):
  list_display=["id","ftoNo","block"]
  readonly_fields=["block","panchayat"]
  search_fields=["block__code","ftoNo"]
  list_filter=["finyear","allWagelistFound","isDownloaded","downloadAttemptCount","isError"]

class workDetailModelAdmin(admin.ModelAdmin):
  actions = [export_as_csv_action("CSV Export", fields=['worker','finyear','workDemandDate','daysDemanded','daysAllocated','demandExists','workAllocatedDate'])]
  list_display=["id","worker","workDemandDate","workAllocatedDate","daysDemanded","daysAllocated","demandExists"]
  readonly_fields=["muster","worker","wagelist","curWagelist","rejectedPayment","wagelistTransaction","dpTransaction"]
  search_fields=["code","worker__jobcard__jobcard"]
  list_filter=["demandExists","finyear"]
class workPaymentModelAdmin(admin.ModelAdmin):
  actions = [export_as_csv_action("CSV Export")]
  list_display=["id","worker","workDemandDate","workAllocatedDate","daysDemanded","daysAllocated","demandExists"]
  readonly_fields=["muster","worker","wagelist","curWagelist","rejectedPayment","wagelistTransaction","dpTransaction"]
  search_fields=["code","worker__jobcard__jobcard"]
  list_filter=["demandExists","finyear"]


class dwdModelAdmin(admin.ModelAdmin):
  actions = [export_as_csv_action("CSV Export", fields=['worker','finyear','workDemandDate','daysDemanded','daysAllocated','demandExists'])]
  list_display=["id","worker","workDemandDate","workAllocatedDate","daysDemanded","daysAllocated","demandExists"]
  readonly_fields=["muster","worker","curWagelist","dpTransaction"]
  search_fields=["code","worker__jobcard__jobcard"]
  list_filter=["demandExists","finyear"]

class libtechTagModelAdmin(admin.ModelAdmin):
  list_display=["id","name"]

class jobcardModelAdmin(admin.ModelAdmin):
  list_display=["id","__str__"]
  readonly_fields=["panchayat","village","applicationDate"]
  list_filter=["isDownloaded","isVillageInfoMissing","isWorkerTableMissing","isPaymentTableMissing"]
  search_fields=["panchayat__block__code","panchayat__code","tjobcard","jobcard"]
class workerModelAdmin(admin.ModelAdmin):
  actions = [removeTags,setIsSampleFalse]
  list_display=["id","jobcard","name","applicantNo"]
  readonly_fields=["jobcard"]
  search_fields=["jobcard__id","jobcard__jobcard","jobcard__panchayat__code","jobcard__tjobcard"]
  list_filter=["is15Days","isSample","libtechTag"]
 #def get_reportFile(self,obj):
 #  if obj.reportFile.filename is not None:
 #  return mark_safe("<a href='%s'>Download</a>" % obj.reportFile.url)
 #get_reportFile.allow_tags = True
 #get_reportFile.description='Download'


class reportModelAdmin(admin.ModelAdmin):
  actions = [download_reports_zip]
  list_display=["__str__","finyear","get_reportFile","modified","get_block"]
  readonly_fields=["panchayat","block","finyear","reportType"]
  list_filter=["finyear","reportType"]
  search_fields=["panchayat__name","panchayat__block__name","panchayat__code"]
  def get_block(self, obj):
    if obj.block is None:
      return obj.panchayat.block.name
    else:
      return obj.block.name
  def get_reportFile(self,obj):
    return mark_safe("<a href='%s'>Download</a>" % obj.reportURL)
  get_reportFile.allow_tags = True
  get_reportFile.description='Download'

  def get_queryset(self, request):
    qs = super(reportModelAdmin, self).get_queryset(request)
    if request.user.is_superuser:
      return qs
    elif request.user.username == "jsk":
     return qs.filter( Q(panchayat__block__district__state__code='34') & (Q(reportType='pendingPayment') | Q(reportType='workPaymentJSK') | Q(reportType='inValidPayment') | Q(reportType="jobcardStat") |  Q(reportType='applicationRegister') | Q(reportType="extendedRPReport") | Q(reportType="detailWorkPayment")))
    else:
      return qs
class apWorkPaymentModelAdmin(admin.ModelAdmin):
  actions = [setisError,setisProcessedFalse]
  list_display=["id","jobcard","payorderDate","name",'payorderAmount']
  readonly_fields=["jobcard","worker",'payorderDate']
  search_fields=["jobcard__tjobcard"]

class wagelistTransactionModelAdmin(admin.ModelAdmin):
  list_display=["id","wagelistIndex","fto","worker","wagelist","isWagelistFTOAbsent","isRegenerated"]
  list_filter=["isRegenerated","isWagelistFTOAbsent"]
  search_fields=["wagelist__wagelistNo","wagelist__panchayat__code"]
  readonly_fields=["worker","wagelist","fto","workDetail","workPayment","rejectedPayment"]
class rejectedPaymentModelAdmin(admin.ModelAdmin):
  list_display=["id","referenceNo","finyear","block"]
  readonly_fields=["block","worker","wagelist","muster","fto","workPayment","workDetail"]
  search_fields=["referenceNo","block__code"]
  list_filter=["finyear","isDownloaded"]
class paymentTransactionModelAdmin(admin.ModelAdmin):
  actions = [export_as_csv_action("CSV Export")]
  list_display=["id","fto","ftoIndex","jobcardRaw","referenceNo","status"]
  readonly_fields=["wagelist","fto","jobcard"]
  search_fields=["fto__ftoNo","wagelist__wagelistNo","jobcard__jobcard"]
class dpTransactionModelAdmin(admin.ModelAdmin):
  list_display=["id","worker","muster"]
  readonly_fields=["worker","muster"]

class jobcardStatModelAdmin(admin.ModelAdmin):
  list_display=["id","jobcard","finyear","jobcardDaysDemanded","jobcardDaysWorked","musterDaysWorked"]
  readonly_fields=["jobcard"]
  search_fields=["jobcard__jobcard","jobcard__panchayat__code"]

admin.site.register(State,stateModelAdmin)
admin.site.register(District,districtModelAdmin)
admin.site.register(Block,blockModelAdmin)
admin.site.register(Panchayat,panchayatModelAdmin)
admin.site.register(Muster,musterModelAdmin)
admin.site.register(Wagelist,wagelistModelAdmin)
admin.site.register(FTO,FTOModelAdmin)
admin.site.register(CrawlQueue,crawlQueueModelAdmin)
admin.site.register(Report,reportModelAdmin)
admin.site.register(Jobcard,jobcardModelAdmin)
admin.site.register(LibtechTag,libtechTagModelAdmin)
admin.site.register(Worker,workerModelAdmin)
admin.site.register(APWorkPayment,apWorkPaymentModelAdmin)
admin.site.register(Village,villageModelAdmin)
admin.site.register(PanchayatCrawlInfo,panchayatCrawlInfoModelAdmin)
admin.site.register(PanchayatStat,panchayatStatModelAdmin)
admin.site.register(WagelistTransaction,wagelistTransactionModelAdmin)
admin.site.register(DPTransaction,dpTransactionModelAdmin)
admin.site.register(DemandWorkDetail,dwdModelAdmin)
admin.site.register(MISReportURL,misReportURLModelAdmin)
admin.site.register(RejectedPayment,rejectedPaymentModelAdmin)
admin.site.register(WorkDetail,workDetailModelAdmin)
admin.site.register(JobcardStat,jobcardStatModelAdmin)
admin.site.register(CrawlState,crawlStateModelAdmin)
admin.site.register(CrawlRequest,crawlRequestModelAdmin)
admin.site.register(PaymentTransaction,paymentTransactionModelAdmin)
admin.site.register(WorkPayment,workPaymentModelAdmin)
# Register your models here.
