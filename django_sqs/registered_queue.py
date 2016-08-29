import logging
import signal
import sys
import time
from warnings import warn

import boto3

from django.conf import settings

try:
    from django.db import connections
    CONNECTIONS = connections.all()
except ImportError:
    from django.db import connection
    CONNECTIONS = (connection, )


DEFAULT_VISIBILITY_TIMEOUT = getattr(
    settings, 'SQS_DEFAULT_VISIBILITY_TIMEOUT', 60)

POLL_PERIOD = getattr(
    settings, 'SQS_POLL_PERIOD', 10)


class TimedOut(Exception):
    """Raised by timeout handler."""
    pass


def sigalrm_handler(signum, frame):
    raise TimedOut()


class RestartLater(Exception):
    """Raised by receivers to stop processing and leave message in queue."""
    pass


class UnknownSuffixWarning(RuntimeWarning):
    """Unknown suffix passed to a registered queue"""
    pass


class RegisteredQueue(object):

    class ReceiverProxy(object):
        """Callable object that sends message via appropriate SQS queue.

        Available attributes:
        - direct - inner (decorated) function
        - registered_queue - a RegisteredQueue instance for this queue
        """
        def __init__(self, registered_queue):
            self.registered_queue = registered_queue
            self.direct = registered_queue.receiver

        def __call__(self, message=None, **kwargs):
            self.registered_queue.send(message, **kwargs)

    def __init__(self, name,
                 receiver=None, visibility_timeout=None,
                 timeout=None, delete_on_start=False, close_database=False,
                 suffixes=(), logger='django_sqs'):
        self._sqs_client = None
        self.name = name
        self.receiver = receiver
        self.visibility_timeout = visibility_timeout or DEFAULT_VISIBILITY_TIMEOUT
        self.queue_urls = {}
        self.timeout = timeout
        self.delete_on_start = delete_on_start
        self.close_database = close_database
        self.suffixes = suffixes

        if self.timeout and not self.receiver:
            raise ValueError("timeout is meaningful only with receiver")

        self.prefix = getattr(settings, 'SQS_QUEUE_PREFIX', None)

        self._logger = logging.getLogger(logger)
        self._logger.info("Using queue %s" % self.full_name())

    def full_name(self, suffix=None):
        name = self.name
        if suffix:
            if suffix not in self.suffixes:
                warn("Unknown suffix %s" % suffix, UnknownSuffixWarning)
            name = '%s__%s' % (name, suffix)
        if self.prefix:
            return '%s__%s' % (self.prefix, name)
        else:
            return name

    def get_sqs_client(self):
        if self._sqs_client is None:
            self._sqs_client = boto3.session.Session(
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION
            ).client(
                'sqs'
            )
        return self._sqs_client

    def get_queue_url(self, suffix=None):
        if suffix not in self.queue_urls:
            self.queue_urls[suffix] = self.get_sqs_client().create_queue(
                QueueName=self.full_name(suffix),
                Attributes={
                    'VisibilityTimeout': str(self.visibility_timeout)
                }
            ).get('QueueUrl')
        return self.queue_urls[suffix]

    def get_receiver_proxy(self):
        return self.ReceiverProxy(self)

    def send(self, message=None, suffix=None, **kwargs):
        _queue_url = self.get_queue_url(suffix)
        return self.get_sqs_client().send_message(
            QueueUrl=_queue_url,
            MessageBody=message
        )

    def receive(self, message_id, receipt_handle, body, attributes, md5_of_body):
        if self.receiver is None:
            raise Exception("Not configured to received messages.")
        if self.timeout:
            signal.alarm(self.timeout)
            signal.signal(signal.SIGALRM, sigalrm_handler)
        if settings.DEBUG:
            self._logger.debug("Message received. message_id:%s, receipt_handle:%s, body:%s, "
                               "attributes:%s, md5_of_body:%s"
                               % (message_id, receipt_handle, body, str(attributes), md5_of_body))
        else:
            self._logger.info("Message received. message_id:%s, body:%s" % (message_id, body))
        try:
            self._logger.debug(type(self.receiver))
            self.receiver(body)
        finally:
            if self.timeout:
                try:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                except TimedOut:
                    # possible race condition if we don't cancel the
                    # alarm in time.  Now there is no race condition
                    # threat, since alarm already rang.
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
            if self.close_database:
                for _connection in CONNECTIONS:
                    self._logger.info("Closing %s" % str(_connection))
                    _connection.close()

    def receive_single(self, suffix=None):
        """Receive single message from the queue.

        This method is here for debugging purposes.  It receives
        single message from the queue, processes it, deletes it from
        queue and returns (message, handler_result_value) pair.
        """
        _queue_url = self.get_queue_url(suffix)
        _message = self._receive_messages(_queue_url)[:1]
        _message_id = _message.get('MessageId')
        _receipt_handle = _message.get('ReceiptHandle')
        _body = _message.get('Body')
        _attributes = _message.get('Attributes')
        _md5_of_body = _message.get('MD5OfBody')

        if self.delete_on_start:
            self.get_sqs_client().delete_message(QueueUrl=_queue_url, ReceiptHandle=_receipt_handle)
        self.receive(_message_id, _receipt_handle, _body, _attributes, _md5_of_body)
        if not self.delete_on_start:
            self.get_sqs_client().delete_message(QueueUrl=_queue_url, ReceiptHandle=_receipt_handle)
        return

    def receive_loop(self, message_limit=None, suffix=None):
        """Run receiver loop.

        If `message_limit' number is given, return after processing
        this number of messages.
        """
        _queue_url = self.get_queue_url(suffix)
        i = 0
        while True:
            if message_limit:
                i += 1
                if i > message_limit:
                    return

            _messages = self._receive_messages(_queue_url)
            if not _messages:
                time.sleep(POLL_PERIOD)
            else:
                try:
                    _message = _messages[0]
                    self._logger.debug("Received message: %s" % str(_message))
                    _message_id = _message.get('MessageId')
                    _receipt_handle = _message.get('ReceiptHandle')
                    _body = _message.get('Body')
                    _attributes = _message.get('Attributes')
                    _md5_of_body = _message.get('MD5OfBody')
                    if self.delete_on_start:
                        self.get_sqs_client().delete_message(QueueUrl=_queue_url, ReceiptHandle=_receipt_handle)
                    from django.db import connection
                    connection.connect()
                    self.receive(_message_id, _receipt_handle, _body, _attributes, _md5_of_body)
                    connection.close()
                    if not self.delete_on_start:
                        self.get_sqs_client().delete_message(QueueUrl=_queue_url, ReceiptHandle=_receipt_handle)
                except KeyboardInterrupt:
                    e = sys.exc_info()[1]
                    raise e
                except RestartLater:
                    self._logger.debug("Restarting message handling")
                except Exception:
                    e = sys.exc_info()[1]
                    self._logger.exception(
                        "Caught exception in receive loop. Received message:%s, exception:%s" % (
                            str(_messages), str(e)))

    def _receive_messages(self, queue_url, number_messages=1):
        _response = self.get_sqs_client().receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=number_messages,
            AttributeNames=['All'])
        _response_metadata = _response.get('ResponseMetadata')
        if 200 != _response_metadata.get('HTTPStatusCode'):
            self._logger.error("SQS Response Metadata: %s" % _response_metadata)
        _messages = _response.get('Messages')
        return _messages
