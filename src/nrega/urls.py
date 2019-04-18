from django.urls import path
from .views import crawlStatusView
#GenericReportDetailAPIView,GenericReportUpdateAPIView,GenericReportCreateAPIView,GenericReportListAPIView

urlpatterns = [ 
                path('crawlStatus/', crawlStatusView.as_view()),
        ]

