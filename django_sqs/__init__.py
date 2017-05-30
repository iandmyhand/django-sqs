from .registered_queue import RegisteredQueue, TimedOut, RestartLater

PROJECT = 'Django SQS'


# ============
# convenience
# ============
def send(queue_alias, receiver, params, suffix=None, **kwargs):
    _queue = RegisteredQueue(queue_alias, **kwargs)
    _queue.send(receiver, params, suffix)
    return _queue


def purge(queue_alias, receipt_handle, suffix=None, **kwargs):
    _queue = RegisteredQueue(queue_alias, **kwargs)
    _queue.delete_message(receipt_handle, suffix=suffix)
    return _queue


def purge_all(queue_alias, suffix=None, **kwargs):
    _queue = RegisteredQueue(queue_alias, **kwargs)
    _queue.purge_all(suffix=suffix)
    return _queue
