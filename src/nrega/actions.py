import unicodecsv
from django.http import HttpResponse
import time
import datetime
import os
from django.http import HttpResponseRedirect
from django.conf import settings
#from mysite.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, REGION_NAME
from config.defines  import LIBTECH_AWS_ACCESS_KEY_ID,LIBTECH_AWS_SECRET_ACCESS_KEY
from libtech.settings import AWS_STORAGE_BUCKET_NAME,AWS_S3_REGION_NAME,MEDIA_URL,S3_URL
import boto3
from boto3.session import Session
from botocore.client import Config
def resetAttemptCount(modeladmin,request,queryset):
  for obj in queryset:
    obj.attemptCount=0
    obj.save()
def setInProgress(modeladmin,request,queryset):
  for obj in queryset:
    obj.inProgress=True
    obj.save()
def resetInProgress(modeladmin,request,queryset):
  for obj in queryset:
    obj.inProgress=False
    obj.save()
def resetStepStarted(modeladmin,request,queryset):
  for obj in queryset:
    obj.stepStarted=False
    obj.save()

def resetTaskInProgress(modeladmin,request,queryset):
  for obj in queryset:
    obj.taskInProgress=False
    obj.insertInQueue=False
    obj.inQueue=False
    obj.executionInProgress=False
    obj.save()

def setIsSampleFalse(modeladmin,request,queryset):
  for obj in queryset:
    obj.isSample=False
    obj.save()

def removeTags(modeladmin,request,queryset):
  for obj in queryset:
    mytags=obj.libtechTag.all()
    for eachTag in mytags:
      obj.libtechTag.remove(eachTag)
      
def setisActiveTrue(modeladmin,request,queryset):
  for obj in queryset:
    obj.isActive=True
    obj.save()

def setisActiveFalse(modeladmin,request,queryset):
  for obj in queryset:
    obj.isActive=False
    obj.save()

def setisProcessedFalse(modeladmin,request,queryset):
  for obj in queryset:
    myDM=obj.downloadManager
    myDM.isProcessed=False
    myDM.save()

def setisError(modeladmin,request,queryset):
  for obj in queryset:
    myDM=obj.downloadManager
    myDM.isError=True
    myDM.save()
def download_reports_zip(modeladmin, request, queryset):
    curTimeStamp=str(int(time.time()))
    dateString=datetime.date.today().strftime("%B_%d_%Y_%I_%M")
    baseDir="/tmp/genericReports/%s_%s" % (dateString,curTimeStamp)
    s='' 
    for obj in queryset:
      if obj.panchayat:
        filepath="%s/%s" % (baseDir,obj.panchayat.slug)
      else:
        filepath=baseDir
      if hasattr(obj, 'finyear'):
        filename="%s_%s.csv" % (obj,obj.finyear)
      else:
        filename="%s.csv" % (obj)
      cmd="mkdir -p %s && cd %s && wget -O %s %s " %(filepath,filepath,filename,obj.reportURL) 
      os.system(cmd)
      s+=obj.reportURL
      s+="\n"
    cmd="cd %s && zip -r %s.zip *" % (baseDir,baseDir)
    os.system(cmd)
    in_file = open("%s.zip" % baseDir, "rb") # opening for [r]eading as [b]inary
    zipdata = in_file.read() # if you only wanted to read 512 bytes, do .read(512)
    in_file.close()


    cloudFilename="media/temp/%s_%s.zip" % (dateString,curTimeStamp)
    session = boto3.session.Session(aws_access_key_id=LIBTECH_AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=LIBTECH_AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3',config=Config(signature_version='s3v4'))
    s3.Bucket(AWS_STORAGE_BUCKET_NAME).put_object(ACL='public-read',Key=cloudFilename, Body=zipdata)
    baseURL="https://s3.ap-south-1.amazonaws.com/%s" % (AWS_STORAGE_BUCKET_NAME.lower())
    publicURL="%s/%s" % (baseURL,cloudFilename)
#    with open("/tmp/test.csv","w") as f:
#      f.write(s)
    return HttpResponseRedirect(publicURL)
    
download_reports_zip.short_description = "Download Selected Reports as Zip"
setisError.short_description="Set Error"

def export_as_csv_action(description="Export selected objects as CSV file",
                         fields=None, exclude=None, header=True):
    """
    This function returns an export csv action
    'fields' and 'exclude' work like in django ModelForm
    'header' is whether or not to output the column names as the first row
    """
    def export_as_csv(modeladmin, request, queryset):
        opts = modeladmin.model._meta

        if not fields:
            field_names = [field.name for field in opts.fields]
        else:
            field_names = fields
        filename='%s.csv' % str(opts).replace('.', '_')
      #  response = HttpResponse(contect_type='text/csv')
        response = HttpResponse(filename, content_type='text/csv')
      #  response['Content-Disposition'] = 'attachment; filename=stat-info.csv'
        #response['Content-Disposition'] = 'attachment; filename=%s.csv' % unicode(opts).replace('.', '_')
        response['Content-Disposition'] = 'attachment; filename=%s.csv' % str(opts).replace('.', '_')

        writer = unicodecsv.writer(response, encoding='utf-8')
        if header:
            writer.writerow(field_names)
        for obj in queryset:
            row = [getattr(obj, field)() if callable(getattr(obj, field)) else getattr(obj, field) for field in field_names]
            writer.writerow(row)
        return response
    export_as_csv.short_description = description
    return export_as_csv
