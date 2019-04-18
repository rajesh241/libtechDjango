from nrega.models import State,District,Block,Panchayat,LibtechTag,GenericReport,CrawlQueue,Report,PanchayatStat,Jobcard,Village,Worker,PanchayatCrawlInfo
from nrega.mixins import HttpResponseMixin
from accounts.api.permissions import IsOwnerOrReadOnly,IsAdminOwnerOrReadOnly
from django.views.generic import View
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.authentication import SessionAuthentication
from rest_framework import mixins,generics,permissions
from .mixins import CSRFExemptMixin
from .utils import is_json
from .serializers import StateSerializer,DistrictSerializer,BlockSerializer,PanchayatSerializer,GenericReportSerializer,CrawlQueueSerializer,ReportSerializer,PanchayatStatSerializer,JobcardSerializer,VillageSerializer,WorkerSerializer,CreateUpdateData,PanchayatCrawlInfoSerializer
from nrega.forms import LibtechTagForm
import json

def getID(request):
  urlID=request.GET.get('id',None)
  inputJsonData={}
  if is_json(request.body):
    inputJsonData=json.loads(request.body)
  inputJsonID=inputJsonData.get("id",None)
  inputID = urlID or inputJsonID or None
  return inputID

class CreateUpdateAPIView(HttpResponseMixin,CSRFExemptMixin,View):
  def get(self,request):
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")

  def post(self,request):
    print(request.body)
    data=json.dumps({"message":"Need to specify the ID for this method"})
    return self.render_to_response(d)


class StateAPIView(HttpResponseMixin,
                           mixins.RetrieveModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = StateSerializer
  passedID=None
  inputID=None
  search_fields= ('name')
  ordering_fields=('name','id')
  filter_fields=('name','code')
  queryset=State.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

class DistrictAPIView(HttpResponseMixin,
                           mixins.RetrieveModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = DistrictSerializer
  passedID=None
  inputID=None
  search_fields= ('name')
  ordering_fields=('name','id')
  filter_fields=('name','code')
  queryset=District.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

class BlockAPIView(HttpResponseMixin,
                           mixins.RetrieveModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = BlockSerializer
  passedID=None
  inputID=None
  search_fields= ('name')
  ordering_fields=('name','id')
  filter_fields=('name','code')
  queryset=Block.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

class PanchayatAPIView(HttpResponseMixin,
                           mixins.RetrieveModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = PanchayatSerializer
  passedID=None
  inputID=None
  search_fields= ('name')
  ordering_fields=('name','id')
  filter_fields=('name','code')
  queryset=Panchayat.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

class PanchayatCrawlInfoAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = PanchayatCrawlInfoSerializer
  passedID=None
  inputID=None
  search_fields= ('code')
  ordering_fields=('code','id')
  filter_fields=('id','code')
  queryset=PanchayatCrawlInfo.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)


class VillageAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = VillageSerializer
  passedID=None
  inputID=None
  search_fields= ('name')
  ordering_fields=('name','id')
  filter_fields=('id','code')
  queryset=Village.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)




class WorkerAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = WorkerSerializer
  passedID=None
  inputID=None
  search_fields= ('jobcard')
  ordering_fields=('jobcard','id')
  filter_fields=('id','jobcard','code')
  queryset=Worker.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)


class ReportAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = ReportSerializer
  passedID=None
  inputID=None
  search_fields= ('reportType')
  ordering_fields=('reportType','id')
  filter_fields=('reportType','code','id','finyear')
  queryset=Report.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)

class JobcardAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = JobcardSerializer
  passedID=None
  inputID=None
  search_fields= ('jobcard')
  ordering_fields=('jobcard','id')
  filter_fields=('id','code','jobcard')
  queryset=Jobcard.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)


class PanchayatStatAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = PanchayatStatSerializer
  passedID=None
  inputID=None
  search_fields= ('finyear')
  ordering_fields=('finyear','id')
  filter_fields=('id','code','finyear')
  queryset=PanchayatStat.objects.all()
  def get_object(self):
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      self.check_object_permissions(self.request,obj)
    return obj
  def get(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    self.inputID=getID(request)
    if self.inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)



class CrawlQueueAPIView(HttpResponseMixin,
                           mixins.CreateModelMixin,
                           mixins.DestroyModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly,IsAdminOwnerOrReadOnly]
  serializer_class = CrawlQueueSerializer
  passedID=None
  inputID=None
  search_fields= ('user__username','startFinYear')
  ordering_fields=('user__username','id')
  queryset=CrawlQueue.objects.all()
# def get_queryset(self):
#   print(self.request.user)
#   qs=CrawlQueue.objects.all()
#   query=self.request.GET.get("q")
#   if query is not None:
#     qs=qs.filter(startFinYear__icontains=query)
#   return qs
  def perform_destroy(self,instance):
    if instance is not None:
      return instance.delete()
    return None
  def getInputID(self):
    request=self.request
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.inputID =inputID
    return inputID
  def get_object(self):
    request=self.request
    inputID=self.inputID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      print("Input ID %s " %str(inputID))
      self.check_object_permissions(request,obj)
    return obj

  def get(self,request,*args,**kwargs):
    inputID=self.getInputID()
    print(inputID)
    if inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    inputID=self.getInputID()
    if inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    inputID=self.getInputID()
    if inputID is None:
      data=json.dumps({"message":"Need to specify the ID for this method"})
      return self.render_to_response(data,status="404")
    return self.destroy(request,*args,**kwargs)


class GenericReportDetailAPIView(mixins.DestroyModelMixin,mixins.UpdateModelMixin,generics.RetrieveAPIView):

  permission_classes=[permissions.IsAuthenticatedOrReadOnly]
  serializer_class = GenericReportSerializer
  queryset=GenericReport.objects.all()
  passedID=None 
  lookup_field='id'
# def perform_d estroy(self,instance):
#   if instance  is not None:
#     return in stance.delete()
#   return None
#  def perform_update(self,serializer):
#    pass
  def put(self,request,*args,**kwargs):
    return self.update(request,*args,**kwargs)

  def patch(self,request,*args,**kwargs):
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    return self.destroy(request,*args,**kwargs)

class GenericReportAPIView(mixins.CreateModelMixin,
                           mixins.RetrieveModelMixin,
                           generics.ListAPIView):
  permission_classes=[permissions.IsAuthenticatedOrReadOnly]
#  authentication_classes=[SessionAuthentication]
  serializer_class = GenericReportSerializer
  passedID=None
  def get_queryset(self):
    print(self.request.user)
    qs=GenericReport.objects.all()
    query=self.request.GET.get("q")
    if query is not None:
      qs=qs.filter(description__icontains=query)
    return qs
  def perform_destroy(self,instance):
    if instance is not None:
      return instance.delete()
    return None
  def get_object(self):
    request=self.request
    inputID=request.GET.get('id',None) or self.passedID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      print("Input ID %s " %str(inputID))
      self.check_object_permissions(request,obj)
    return obj

  def get(self,request,*args,**kwargs):
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.passedID =inputID
    print(inputID)
    if inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)


'''
class GenericReportAPIView(mixins.CreateModelMixin,
                           mixins.RetrieveModelMixin,
                           mixins.UpdateModelMixin,
                           mixins.DestroyModelMixin,
                           generics.ListAPIView):
  permissions_classes=[]
  authentication_classes=[]
  serializer_class = GenericReportSerializer
  passedID=None
  def get_queryset(self):
    qs=GenericReport.objects.all()
    query=self.request.GET.get("q")
    if query is not None:
      qs=qs.filter(description__icontains=query)
    return qs
  def perform_destroy(self,instance):
    if instance is not None:
      return instance.delete()
    return None
  def get_object(self):
    request=self.request
    inputID=request.GET.get('id',None) or self.passedID
    queryset=self.get_queryset()
    obj=None
    if inputID is not None:
      obj=get_object_or_404(queryset,id=inputID)
      print("Input ID %s " %str(inputID))
      self.check_object_permissions(request,obj)
    return obj

  def get(self,request,*args,**kwargs):
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.passedID =inputID
    print(inputID)
    if inputID is not None:
      return self.retrieve(request,*args,**kwargs)
    return super().get(request,*args,**kwargs)

  def post(self,request,*args,**kwargs):
    return self.create(request,*args,**kwargs)

  def put(self,request,*args,**kwargs):
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.passedID =inputID
    return self.update(request,*args,**kwargs)

  def patch(self,request,*args,**kwargs):
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.passedID =inputID
    return self.update(request,*args,**kwargs)

  def delete(self,request,*args,**kwargs):
    urlID=request.GET.get('id',None)
    inputJsonData={}
    if is_json(request.body):
      inputJsonData=json.loads(request.body)
    inputJsonID=inputJsonData.get("id",None)
    inputID = urlID or inputJsonID or None
    self.passedID =inputID
    return self.destroy(request,*args,**kwargs)
'''
#class GenericReportCreateAPIView(generics.CreateAPIView):
# permissions_classes=[]
# authentication_classes=[]
# queryset=GenericReport.objects.all()
# serializer_class = GenericReportSerializer

'''
class GenericReportDetailAPIView(generics.RetrieveUpdateDestroyAPIView):
  permissions_classes=[]
  authentication_classes=[]
  queryset=GenericReport.objects.all()
  serializer_class = GenericReportSerializer
'''
'''
Method to use with mixinx
 class GenericReportDetailAPIView(mixins.DestroyModelMixin,mixins.UpdateModelMixin,generics.RetrieveAPIView):
  print("I am in detail view")
  permissions_classes=[]
  authentication_classes=[]
  queryset=GenericReport.objects.all()
  serializer_class = GenericReportSerializer
#  lookup_field='id'

  def put(self,request,*args,**kwargs):
    return self.update(request,*args,**kwargs)
  def delete(self,request,*args,**kwargs):
    return self.destroy(request,*args,**kwargs)
'''
class GenericReportUpdateAPIView(generics.UpdateAPIView):
  print("I am in update view")
  permissions_classes=[]
  authentication_classes=[]
  queryset=GenericReport.objects.all()
  lookup_field='id'
  serializer_class = GenericReportSerializer
class GenericReportDeleteAPIView(generics.DestroyAPIView):
  permissions_classes=[]
  authentication_classes=[]
  queryset=GenericReport.objects.all()
  serializer_class = GenericReportSerializer


class GenericReportListAPIView(APIView):
  permissions_classes=[]
  authentication_classes=[]

  def get(self,request,format=None):
    qs=GenericReport.objects.all()
    serializer=GenericReportSerializer(qs,many=True)
    return Response (serializer.data)
class LibtechTagDetailAPIView(HttpResponseMixin,CSRFExemptMixin,View):
  is_Json=True
  def get_object(self,id=None):
    qs=LibtechTag.objects.filter(id=id)
    if qs.count() == 1:
      return qs.first()
    else:
      return None

  def get(self,request,id,*args,**kwargs):
    obj=self.get_object(id=id)
    if obj is None:
      data=json.dumps({"message":"Libtech Tag does not exists"})
      return self.render_to_response(data,status="404")
    json_data=obj.serialize()
    return self.render_to_response(json_data)

  def post(self,request,id,*args,**kwargs):
    json_data=json.dumps({"message":"Now allowed please use api/LibtechTag/list/ "})
    return self.render_to_response(json_data,status="403")

  def put(self,request,id,*args,**kwargs):
    obj=self.get_object(id=id)
    if obj is None:
      data=json.dumps({"message":"Libtech Tag does not exists"})
      return self.render_to_response(data,status="404")
    valid_json=is_json(request.body)
    if not valid_json:
      data=json.dumps({"message":"Input JSON Data not valid"})
      return self.render_to_response(data,status="400")

    data=json.loads(obj.serialize())
    inputData=json.loads(request.body)
    print(inputData)
    print(data)
    for key,value in inputData.items():
      data[key]=value
    form=LibtechTagForm(data,instance=obj)
    if form.is_valid():
      obj=form.save(commit=True)
      obj_data=obj.serialize()
      return self.render_to_response(obj_data,status=201)
    if form.errors:
      data=json.dumps(form.errors)
      return self.render_to_response(data,status=400)

    print(request.body)
    json_data=obj.serialize()
    return self.render_to_response(json_data)


  def delete(self,request,id,*args,**kwargs):
    obj=self.get_object(id=id)
    if obj is None:
      data=json.dumps({"message":"Libtech Tag does not exists"})
      return self.render_to_response(data,status="404")
    delete_ ,myobj= obj.delete()
    if delete_ == 1:
      data=json.dumps({"message":"Successfully Deleted"})
      return self.render_to_response(data,status=200)
    data=json.dumps({"message":"Could not delete object"})
    return self.render_to_response(data,status=400)

    
class LibtechTagListAPIView(HttpResponseMixin,CSRFExemptMixin,View):
  is_Json=True
  queryset=None

  def get_queryset(self,id=None):
    if id is not None:
      qs=LibtechTag.objects.filter(id=id)
    else:
      qs=LibtechTag.objects.all()
    self.queryset=qs
    return qs

  def get_object(self,id=None):
    qs=self.get_queryset().filter(id=id)
#    qs=LibtechTag.objects.filter(id=id)
    if qs.count() == 1:
      return qs.first()
    else:
      return None


  def get(self,request,*args,**kwargs):
    valid_json=is_json(request.body)
    print("I am in get call")
    if valid_json:
      inputData=json.loads(request.body)
      inputID=inputData.get('id',None)
      if inputID is not None:
        objs=self.get_queryset(id=inputID)
        if len(objs) == 0:
          data=json.dumps({"message":"Libtech Tag does not exists"})
          return self.render_to_response(data,status="404")
      else:
        objs=self.get_queryset()
    else:
      print("Not a valid json")
      objs=self.get_queryset()
    print(objs)
    json_data=objs.serialize()
    return self.render_to_response(json_data)

  def post(self,request,*args,**kwargs):
    valid_json=is_json(request.body)
    if not valid_json:
      data=json.dumps({"message":"Input JSON Data not valid"})
      return self.render_to_response(data,status="400")

    form=LibtechTagForm(json.loads(request.body))
    print(form)
    if form.is_valid():
      obj=form.save(commit=True)
      obj_data=obj.serialize()
      return self.render_to_response(obj_data,status=201)
    if form.errors:
      data=json.dumps(form.errors)
      return self.render_to_response(data,status=400)
    data={"message":"not allowed"}
    return self.render_to_response(data,status=400)



  def put(self,request,*args,**kwargs):
    valid_json=is_json(request.body)
    if not valid_json:
      data=json.dumps({"message":"Input JSON Data not valid"})
      return self.render_to_response(data,status="400")

    inputData=json.loads(request.body)
    inputID=inputData.get('id',None)
    obj=self.get_object(id=inputID)
    if obj is None:
      data=json.dumps({"message":"Libtech Tag does not exists"})
      return self.render_to_response(data,status="404")

    data=json.loads(obj.serialize())
    print(inputData)
    print(data)
    for key,value in inputData.items():
      data[key]=value
    form=LibtechTagForm(data,instance=obj)
    if form.is_valid():
      obj=form.save(commit=True)
      obj_data=obj.serialize()
      return self.render_to_response(obj_data,status=201)
    if form.errors:
      data=json.dumps(form.errors)
      return self.render_to_response(data,status=400)

    print(request.body)
    json_data=obj.serialize()
    return self.render_to_response(json_data)


  def delete(self,request,*args,**kwargs):
    valid_json=is_json(request.body)
    if not valid_json:
      data=json.dumps({"message":"Input JSON Data not valid"})
      return self.render_to_response(data,status="400")

    inputData=json.loads(request.body)
    print(inputData)
    inputID=inputData.get('id',None)
    obj=self.get_object(id=inputID)
    if obj is None:
      data=json.dumps({"message":"Libtech Tag does not exists"})
      return self.render_to_response(data,status="404")
    delete_ ,myobj= obj.delete()
    if delete_ == 1:
      data=json.dumps({"message":"Successfully Deleted"})
      return self.render_to_response(data,status=200)
    data=json.dumps({"message":"Could not delete object"})
    return self.render_to_response(data,status=400)


