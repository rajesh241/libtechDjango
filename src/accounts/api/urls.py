from django.urls import path
from rest_framework_jwt.views import obtain_jwt_token,refresh_jwt_token
from .views import AuthView,RegisterAPIView
urlpatterns = [ 
                path('register/', RegisterAPIView.as_view()),
                path('jwt/', obtain_jwt_token),
                path('jwt/refresh/', refresh_jwt_token),
        ]

