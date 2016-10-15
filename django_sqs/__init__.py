from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from .registered_queue import RegisteredQueue, TimedOut, RestartLater


# ensure settings are there
if not getattr(settings, 'AWS_ACCESS_KEY_ID'):
    raise ImproperlyConfigured('Missing setting "AWS_ACCESS_KEY_ID"')

if not getattr(settings, 'AWS_SECRET_ACCESS_KEY'):
    raise ImproperlyConfigured('Missing setting "AWS_SECRET_ACCESS_KEY"')

# Try to get regions, otherwise let to DefaultRegionName
# TODO this is bad! never set settings on the fly, better provide an
# app_settings.py with default values
if not getattr(settings, 'AWS_REGION'):
    settings.AWS_REGION = "us-east-1"

PROJECT = 'Django SQS'


# ============
# convenience
# ============
def send(queue_name, message, suffix=None, **kwargs):
    _rq = RegisteredQueue(queue_name, **kwargs)
    _rq.send(message, suffix, **kwargs)
    return _rq
