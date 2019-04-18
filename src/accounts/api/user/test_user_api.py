from rest_framework.test import APITestCase
from django.contrib.auth import get_user_model
User=get_user_model()
# Create your tests here.

class UserAPITestCase(APITestCase):
  def setUp(self):
    user=User.objects.create(username='cfe',email='hellocfe@cfe.com')
    user.set_password("ohmyGod4$")
    user.save()

  def test_created_user(self):
    qs=User.objects.filter(username='cfe')
    self.assertEqual(qs.count(),1)
