from django.contrib.auth import authenticate,get_user_model
from django.db.models import Q
from rest_framework.views import APIView
from rest_framework import permissions,generics
from rest_framework.response import Response
from rest_framework_jwt.settings import api_settings
from .utils import jwt_response_payload_handler
jwt_payload_handler=api_settings.JWT_PAYLOAD_HANDLER
jwt_encode_handler=api_settings.JWT_ENCODE_HANDLER
from .serializers import UserRegisterSerializer
from .permissions import AnonPermissionOnly,IsOwnerOrReadOnly
User=get_user_model()



class AuthView(APIView):
  permission_classes=[permissions.AllowAny]
  def post(self,request,*args,**kwargs):
    if request.user.is_authenticated == True:
      return Response({'detail':'You are already authenticated'})
    data=request.data
    print(data)
    username=data.get('username')
    password=data.get('password')
    qs=User.objects.filter(
            Q(username__iexact=username) |
            Q(email__iexact=username)
                ).distinct()
    if qs.count() == 1:
      user=qs.first()
   # user=authenticate(username=username,password=password)
      payload=jwt_payload_handler(user)
      token = jwt_encode_handler(payload)
      print(user)
#    return Response({'token':token})
      return Response(jwt_response_payload_handler(token,user,request=request))
    else:
      return Response({'message':'No User found'})

class RegisterAPIView(generics.CreateAPIView):
  queryset = User.objects.all()
  serializer_class = UserRegisterSerializer
  permission_classes=[AnonPermissionOnly]

  def get_serializer_context(self, *args, **kwargs):
    return {"request": self.request}
