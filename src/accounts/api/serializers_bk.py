from django.contrib.auth import get_user_model
from rest_framework import serializers

User = get_user_model()

class UserRegisterSerializer(serializers.ModelSerializer):
  password = serializers.CharField(style={'input_type':'password'}, write_only=True)
  password2 = serializers.CharField(style={'input_type':'password'}, write_only=True)
  class Meta:
    model = User
    fields = [
        'username',
        'email',
        'password',
        'password2'
        ];
    extra_kwargs = {'password' : {'write_only' : True } }

  def validate_email(self, value):
    qs = User.objects.filter(email__iexact=value)
    if qs.exists():
        raise serializers.ValidationError("User with this email already exists")
    return value
  
  def validate_username(self, value):
    qs = User.objects.filter(username__iexact=value)
    if qs.exists():
        raise serializers.ValidationError("User with this username already exists")
    return value

  def validate(self,data):
    pw=data.get('password')
    pw2=data.pop('password2')
    if pw != pw2:
      raise serializers.ValidationError("Passwords must Match")
    return data

  def create(self,data):
    username=data.get('username')
    email=data.get('email')
    pw=data.get('password')
    userObj=User(username=username,email=email)
    userObj.set_password(pw)
    userObj.save()

