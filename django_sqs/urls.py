from django.conf.urls import patterns
from django.conf.urls import url

from .views import status

urlpatterns = patterns(
    '',
    url(r'^status/$', status, name='sqs_status'),
)
