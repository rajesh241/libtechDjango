from django.urls import path
from .views import LibtechTagListAPIView,LibtechTagDetailAPIView,GenericReportListAPIView,GenericReportAPIView,GenericReportDetailAPIView,GenericReportUpdateAPIView,GenericReportDeleteAPIView,CrawlQueueAPIView,StateAPIView,DistrictAPIView,BlockAPIView,PanchayatAPIView,ReportAPIView,PanchayatStatAPIView,JobcardAPIView,WorkerAPIView,VillageAPIView,CreateUpdateAPIView,PanchayatCrawlInfoAPIView
#GenericReportDetailAPIView,GenericReportUpdateAPIView,GenericReportCreateAPIView,GenericReportListAPIView

urlpatterns = [ 
                path('libtechTag/list/', LibtechTagListAPIView.as_view()),
                path('libtechTag/detail/<id>/', LibtechTagDetailAPIView.as_view()),
                path('genericReport/list/', GenericReportListAPIView.as_view()),
                path('genericReport/', GenericReportAPIView.as_view()),
                path('crawlQueue/', CrawlQueueAPIView.as_view()),
                path('state/', StateAPIView.as_view()),
                path('district/', DistrictAPIView.as_view()),
                path('block/', BlockAPIView.as_view()),
                path('panchayat/', PanchayatAPIView.as_view()),
                path('report/', ReportAPIView.as_view()),
                path('jobcard/', JobcardAPIView.as_view()),
                path('worker/', WorkerAPIView.as_view()),
                path('village/', VillageAPIView.as_view()),
                path('panchayatCrawlInfo/', PanchayatCrawlInfoAPIView.as_view()),
                path('panchayatStat/', PanchayatStatAPIView.as_view()),
                path('createUpdate/', CreateUpdateAPIView.as_view()),
                path('genericReport/detail/<id>/', GenericReportDetailAPIView.as_view()),
         #       path('genericReport/create/', GenericReportCreateAPIView.as_view()),
               path('genericReport/update/<id>/', GenericReportUpdateAPIView.as_view()),
               path('genericReport/delete/<pk>/', GenericReportDeleteAPIView.as_view()),
        ]

