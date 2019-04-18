from django.contrib.auth import authenticate,get_user_model
from django.db.models import Q
from rest_framework.views import APIView
from rest_framework import permissions,generics,pagination
from rest_framework.response import Response
from rest_framework_jwt.settings import api_settings
from .serializers import UserDetailSerializer
from nrega.api.serializers import CrawlQueueInlineUserSerializer
from nrega.models import CrawlQueue
User=get_user_model()

class UserDetailAPIView(generics.RetrieveAPIView):
  queryset =  User.objects.filter(is_active=True)
  serializer_class = UserDetailSerializer
  lookup_field = 'username'
  def get_serializer_context(self):
    return {'request': self.request}
class UserCrawlQueueAPIView(generics.ListAPIView):
  serializer_class = CrawlQueueInlineUserSerializer
  def get_queryset(self,*args,**kwargs):
    username=self.kwargs.get("username",None)
    if username is None:
      return CrawlQueue.objects.none()
    else:
      return CrawlQueue.objects.filter(user__username=username)
