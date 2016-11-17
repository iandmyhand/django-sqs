import boto3
import django_sqs
import importlib.util
import json
import logging
import random
import signal
import sys
import time

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures import as_completed
from django.conf import settings
from django.db import connection
from logging.handlers import WatchedFileHandler
from warnings import warn

try:
    from django.db import connections
    CONNECTIONS = connections.all()
except ImportError:
    from django.db import connection
    CONNECTIONS = (connection, )


DEFAULT_VISIBILITY_TIMEOUT = getattr(
    settings, 'SQS_DEFAULT_VISIBILITY_TIMEOUT', 60)

POLL_PERIOD = getattr(
    settings, 'SQS_POLL_PERIOD', 5)


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


def get_func(func):
    if hasattr(func, '__call__'):
        _func = func
    elif isinstance(func, str):
        _module_string, _func_name = func.split(':')
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
                 visibility_timeout=None, timeout=None,
                 delete_on_start=False, close_database=False,
                 suffixes=(),
                 std_in_path='/dev/null', std_out_path='django_sqs_output.log', std_err_path='django_sqs_error.log',
                 pid_file_path='django_sqs.pid', pid_file_timeout=5,
                 message_type='json',
                 exception_callback=None):

        self.logger = logging.getLogger(django_sqs.__name__)

        self.kill_now = False

        self.stdin_path = std_in_path
        self.stdout_path = std_out_path
        self.stderr_path = std_err_path
        self.pidfile_path = pid_file_path
        self.pidfile_timeout = pid_file_timeout

        self._sqs_client = None
        self.name = name
        self.visibility_timeout = visibility_timeout or DEFAULT_VISIBILITY_TIMEOUT
        self.queue_urls = {}
        self.timeout = timeout
        self.delete_on_start = delete_on_start
        self.close_database = close_database
        self.suffixes = suffixes
        self.message_type = message_type
        self.exception_callback = get_func(exception_callback) if exception_callback else None

        self.futures = []

        if self.timeout:
            raise ValueError("Timeout is meaningful only with receiver")

        self.prefix = getattr(settings, 'DJANGO_SQS_QUEUE_PREFIX', None)

    def __str__(self):
        return str(self.__class__)

    def full_name(self, suffix=None):
        _name = self.name
        if suffix:
            if suffix not in self.suffixes:
                warn("Unknown suffix %s" % suffix, UnknownSuffixWarning)
            _name = '%s__%s' % (_name, suffix)
        if self.prefix:
            return '%s__%s' % (self.prefix, _name)
        else:
            return _name

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

    def send(self, receiver, message=None, suffix=None):
        _queue_url = self.get_queue_url(suffix)
        if 'json' == self.message_type:
            message['receiver'] = receiver
            _body = json.dumps(message)
        else:
            _body = json.dumps({
                'receiver': receiver,
                'message': str(message)
            })
        return self.get_sqs_client().send_message(
            QueueUrl=_queue_url,
            MessageBody=_body
        )

    def handle_receiver(self, queue_url, receiver, message_id, receipt_handle, body, attributes, md5_of_body):
        self.logger.debug("Receiver starts with message id %s(body: %s)" % (str(message_id), str(body)))
        if self.delete_on_start:
            self.get_sqs_client().delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        connection.connect()
        _result = False
        try:
            _receiver = get_func(receiver)
            _result = _receiver(body)
        except Exception as e:
            self.logger.error(str(e))
            if self.exception_callback:
                self.exception_callback(e)
        connection.close()
        if not self.delete_on_start:
            self.get_sqs_client().delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        self.logger.debug("Receiver ends with message id %s(body: %s)" % (str(message_id), str(body)))
        return _result

    def receive(self, queue_url, message_id, receipt_handle, body, attributes, md5_of_body):
        if self.timeout:
            signal.alarm(self.timeout)
            signal.signal(signal.SIGALRM, sigalrm_handler)
        if settings.DEBUG:
            self.logger.debug("Message received. message_id:%s, receipt_handle:%s, body:%s, "
                              "attributes:%s, md5_of_body:%s"
                              % (message_id, receipt_handle, body, str(attributes), md5_of_body))
        else:
            self.logger.info("Message received. message_id:%s, body:%s" % (message_id, body))
        try:
            _body = json.loads(body)
            _receiver = _body.get('receiver')
            if _receiver is None:
                raise Exception("Not configured for received messages.")
            if 'json' != self.message_type:
                _body = str(body.get('message'))
            with ThreadPoolExecutor(max_workers=5) as executor:
                _params = {
                    'queue_url': queue_url,
                    'receiver': _receiver,
                    'message_id': message_id,
                    'receipt_handle': receipt_handle,
                    'body': _body,
                    'attributes': attributes,
                    'md5_of_body': md5_of_body
                }
                try:
                    _future = executor.submit(self.handle_receiver, **_params)
                    self.futures.append(_future)
                except Exception as e:
                    self.logger.error("An error occurred during submitting receiver to thread pool executor: " + str(e))
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
                    self.logger.info("Closing %s" % str(_connection))
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
        self.receive(_queue_url, _message_id, _receipt_handle, _body, _attributes, _md5_of_body)
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
            if self.kill_now:
                return

            if message_limit:
                i += 1
                if i > message_limit:
                    return

            # Max sub processes count is 5.
            if 5 <= len(self.futures):
                for f in as_completed(self.futures):
                    self.logger.debug("Result of some future: " + str(f.result()))
                self.futures = list()

            _messages = self._receive_messages(_queue_url)
            if not _messages:
                time.sleep(POLL_PERIOD + (random.randint(-10, 10) / 10))
            else:
                try:
                    _message = _messages[0]
                    self.logger.debug("Received message: %s" % str(_message))
                    _message_id = _message.get('MessageId')
                    _receipt_handle = _message.get('ReceiptHandle')
                    _body = _message.get('Body')
                    _attributes = _message.get('Attributes')
                    _md5_of_body = _message.get('MD5OfBody')

                    self.receive(_queue_url, _message_id, _receipt_handle, _body, _attributes, _md5_of_body)

                except KeyboardInterrupt:
                    e = sys.exc_info()[1]
                    raise e
                except RestartLater:
                    self.logger.debug("Restarting message handling")
                except Exception:
                    e = sys.exc_info()[1]
                    self.logger.exception(
                        "Caught exception in receive loop. Received message:%s, exception:%s" % (
                            str(_messages), str(e)))

    def _receive_messages(self, queue_url, number_messages=1):
        _response = self.get_sqs_client().receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=number_messages,
            AttributeNames=['All'])
        _response_metadata = _response.get('ResponseMetadata')
        if 200 != _response_metadata.get('HTTPStatusCode'):
            self.logger.error("SQS Response Metadata: %s" % _response_metadata)
        _messages = _response.get('Messages')
        return _messages

    def exit_gracefully(self, signum, frame):
        _logger = logging.getLogger(django_sqs.__name__)
        if signal.SIGTERM == signum:
            _logger.info('Received termination signal. Prepare to exit...')
            if 1 <= len(self.futures):
                for f in as_completed(self.futures):
                    self.logger.debug("Result of some future: " + str(f.result()))
            self.kill_now = True
        else:
            _logger.info('Received signal %d, but there is no process for this signal.' % signum)

    def run(self):

        self.logger = logging.getLogger(django_sqs.__name__)
        for _h in self.logger.handlers:
            self.logger.debug('before:' + str(_h))
            print('before print:' + str(_h))

        if not self.logger.handlers:

            # Set up logger file handler in daemon process.
            _formatter = logging.Formatter(
                fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                    django_sqs.PROJECT + '] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            _handler = WatchedFileHandler(self.stdout_path)
            _handler.setFormatter(_formatter)
            self.logger.addHandler(_handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.info('Set new logger up.')
            for _h in self.logger.handlers:
                self.logger.debug('after:' + str(_h))
                print('after print:' + str(_h))

        # Set signal handler up.
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.receive_loop()
        self.logger.info(django_sqs.PROJECT + ' is completely exited.')
