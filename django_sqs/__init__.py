import importlib

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


# ============
# registry
# ============
queues = {}


# ============
# convenience
# ============
def register(queue_name, receiver, **kwargs):
    _rq = RegisteredQueue(queue_name, _get_func(receiver), **kwargs)
    queues[queue_name] = _rq
    return _rq


def send(queue_name, message, suffix=None, **kwargs):
    _rq = RegisteredQueue(queue_name, **kwargs)
    _rq.send(message, suffix, **kwargs)
    return _rq


def _get_func(func):
    if hasattr(func, '__call__'):
        _func = func
    elif isinstance(func, str):
        _module_string, _func_name = func.split(':')
        print('import ' + settings.BASE_DIR + ':' + _module_string)
        import importlib.util
        _spec = importlib.util.spec_from_file_location(
            _module_string, '%s/%s.py' % (settings.BASE_DIR, _module_string.replace('.', '/')))
        _module = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        _func = getattr(_module, _func_name)
    else:
        raise TypeError('A type of "func" argument is must function or str. '
                        'When put str, it must be full name of function. '
                        'e.g.: func="moduleA.moduleB.function_name"')
    return _func
