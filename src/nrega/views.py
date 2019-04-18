from django.shortcuts import render
from django.http import HttpResponse
from django.views.generic import View
from .models import CrawlQueue
from nrega.crawler.events.jan2019PressRelease import getCrawlStatus
# Create your views here.

class crawlStatusView(View):
  def get(self,request,*args,**kwargs):
    myhtml=getCrawlStatus()
    return HttpResponse(myhtml)
