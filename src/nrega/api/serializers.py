from rest_framework import serializers
from nrega.models import State,District,Block,Panchayat,CrawlQueue,GenericReport,Report,PanchayatStat,Jobcard,Village,Worker,PanchayatCrawlInfo
from accounts.api.serializers import UserPublicSerializer

class CreateUpdateData(serializers.ModelSerializer):
  ndata = serializers.DictField(child=serializers.CharField())
  class Meta:
    fields=[
       'ndata'
            ]


class CrawlQueueInlineUserSerializer(serializers.ModelSerializer):
  class Meta:
    model=CrawlQueue
    fields= [
        'id',
        'panchayat',
        'block',
        'startFinYear'
        ]

  def validate(self,data):
    panchayat=data.get("panchayat",None)
    block=data.get("block",None)
    if block is None and panchayat is None:
      raise serializers.ValidationError("Both Block and Panchayat cannot be empty")
    return data

class StateSerializer(serializers.ModelSerializer):
  class Meta:
    model=State
    fields=[
      'id',
      'name',
      'slug',
      'code',
      'stateShortCode',
      'crawlIP'
      ]

class DistrictSerializer(serializers.ModelSerializer):
  state = StateSerializer(read_only=True)
  class Meta:
    model=State
    fields=[
      'id',
      'name',
      'slug',
      'code',
      'state'
      ]

class BlockSerializer(serializers.ModelSerializer):
  district = DistrictSerializer(read_only=True)
  class Meta:
    model=State
    fields=[
      'id',
      'name',
      'slug',
      'code',
      'district'
      ]



class PanchayatSerializer(serializers.ModelSerializer):
  block = BlockSerializer(read_only=True)
  class Meta:
    model=Panchayat
    fields=[
      'id',
      'name',
      'slug',
      'code',
      'block'
      ]

class PanchayatStatSerializer(serializers.ModelSerializer):
  class Meta:
    model=PanchayatStat
    fields = '__all__'
    extra_kwargs={
                    'panchayat': {'required': False},
                    'finyear': {'required': False}

          }
    validators=[]

class JobcardSerializer(serializers.ModelSerializer):
  class Meta:
    model=Jobcard
    fields = '__all__'
    extra_kwargs={
                    'tjobcard': {'required': False},
                    'jobcard': {'required': False}

          }
    validators=[]

class WorkerSerializer(serializers.ModelSerializer):
  jobcard = JobcardSerializer(read_only=True)
  class Meta:
    model=Worker
    fields = '__all__'
    extra_kwargs={
                    'name': {'required': False},
                    'applicantNo': {'required': False},
                    'jobcard': {'required': False}

          }
    validators=[]



class VillageSerializer(serializers.ModelSerializer):
  class Meta:
    model=Village
    fields = '__all__'
    extra_kwargs={
                    'tjobcard': {'required': False},
                    'jobcard': {'required': False}

          }
    validators=[]

class PanchayatCrawlInfoSerializer(serializers.ModelSerializer):
  class Meta:
    model=PanchayatCrawlInfo
    fields = '__all__'
    extra_kwargs={
                    'panchayat': {'required': False}

          }
    validators=[]


class ReportSerializer(serializers.ModelSerializer):
  class Meta:
    model=Report

    fields= [
        'id',
        'block',
        'panchayat',
        'reportType',
        'finyear',
        'reportURL'
        ]
    extra_kwargs = {
                     'block': {'required': False}, 
                    'panchayat': {'required': False},
                     'reportType': {'required': False}, 
                    'finyear': {'required': False}
                    }
    validators = []

class GenericReportSerializer(serializers.ModelSerializer):
  class Meta:
    model=GenericReport
    fields= [
        'id',
        'name',
        'description',
        'libtechTag',
        'reportFile'
        ]
#    read_only_fields = ['id']
  
  def validate_name(self,value):
    if value =="":
      raise serializers.ValidationError("Name cannot be blank")
    return value

  def validate(self,data):
    description=data.get("description",None)
    name=data.get("name",None)
    if name is None and description is None:
      raise serializers.ValidationError("Both Name and description cannot be empty")
    return data

class CrawlQueueSerializer(serializers.ModelSerializer):
  user = UserPublicSerializer(read_only=True)
  panchayat=PanchayatSerializer(read_only=True)
  class Meta:
    model=CrawlQueue
    fields= [
        'id',
        'user',
        'startFinYear',
        'panchayat',
        'block',
        'stepStarted',
        'stepCompleted',
        'attemptCount',
        'instanceJSONURL',
        'workDetailJSONURL',
        'downloadStage',
        ]
    validators = []

# def validate(self,data):
#   panchayat=data.get("panchayat",None)
#   block=data.get("block",None)
#   if block is None and panchayat is None:
#     raise serializers.ValidationError("Both Block and Panchayat cannot be empty")
#   return data


'''
class CustomSerializer(serializers.Serializer):
  content = serializers.CharField()
  email = serializers.EmailField()

data={'content':'my email','email':'abcd.com'}
create_obj_serializer=CustomSerializer(data=data)
if create_obj_serializer is_valid():
  valid_data=create_obj_serializer.data
  '''
  
