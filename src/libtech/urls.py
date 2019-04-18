"""libtech URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path,include
from nrega import urls as nregaurls
from nrega.api import urls as apiurls
from accounts.api import urls as authurls
from accounts.api.user import urls as userurls

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include(apiurls)),
    path('auth/', include(authurls)),
    path('api/user/', include( (userurls,'accountsUsers'),namespace='api-user')),
    path('crawler/', include(nregaurls)),
]
