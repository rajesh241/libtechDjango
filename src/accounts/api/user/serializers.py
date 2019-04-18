import datetime
from django.utils import timezone
from nrega.models import CrawlQueue
from django.contrib.auth import get_user_model
from rest_framework import serializers
from rest_framework_jwt.settings import api_settings
from rest_framework.reverse import reverse as api_reverse
from nrega.api.serializers import CrawlQueueInlineUserSerializer
jwt_payload_handler             = api_settings.JWT_PAYLOAD_HANDLER
jwt_encode_handler              = api_settings.JWT_ENCODE_HANDLER
jwt_response_payload_handler    = api_settings.JWT_RESPONSE_PAYLOAD_HANDLER
expire_delta             = api_settings.JWT_REFRESH_EXPIRATION_DELTA
User = get_user_model()

class UserDetailSerializer(serializers.ModelSerializer):
  url=serializers.SerializerMethodField(read_only=True)
  userCrawlQueueList=serializers.SerializerMethodField(read_only=True)
  class Meta:
    model=User
    fields= [
        'id',
        'username',
        'url',
        'userCrawlQueueList'
        ]
  def get_url(self,obj):
    request=self.context.get("request")
    return api_reverse("api-user:detail", kwargs={"username": obj.username}, request=request)

  def get_userCrawlQueueList(self,obj):
    qs=obj.crawlqueue_set.all()
    data={
      "url":self.get_url(obj)+"crawlQueue/",
      "recent" : CrawlQueueInlineUserSerializer(qs[:10],many=True).data
            }
    return data
    


