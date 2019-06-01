from django.urls import path
from .views import crawlStatusView,autoComplete,stateComparison
#GenericReportDetailAPIView,GenericReportUpdateAPIView,GenericReportCreateAPIView,GenericReportListAPIView
from . import views
urlpatterns = [ 
                path('crawlStatus/', crawlStatusView.as_view()),
                path('autoComplete/', autoComplete.as_view()),
                path('stateComparison/', stateComparison.as_view()),
                path('', views.index, name='index'),
                path('cool_chart/',
                        views.my_cool_chart_view,
                        name='my-cool-chart'
                      ),
        ]

