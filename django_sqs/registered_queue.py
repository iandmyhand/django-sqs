import boto3
import django_sqs
import importlib
import json
import logging
import random
import signal
import sys
import time

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from django.conf import settings
from logging.handlers import WatchedFileHandler
from warnings import warn


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
        _module = importlib.import_module(_module_string)
        _func = getattr(_module, _func_name)
    else:
        raise TypeError("A type of \"func\" argument is must function or str. "
                        "When put str, it must be full name of function. "
                        "e.g.: func=\"moduleA.moduleB.function_name\"")
    return _func


class DjangoSQSFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%06d" % (t, record.msecs)
        return s


class RegisteredQueue(object):

    queue_alias = None
    options = dict()
    kill_now = False
    queue_url = None
    futures = []
    stdin_path = None
    stdout_path = None
    stderr_path = None
    pidfile_path = None
    pidfile_timeout = None
    exception_callback = None

    _sqs_client = None

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

    def __init__(self, queue_alias, **kwargs):

        self.logger = logging.getLogger(django_sqs.__name__)

        self.queue_alias = queue_alias
        self.options['aws_region'] = kwargs.get(
            'aws_region', settings.DJANGO_SQS_QUEUES[queue_alias]['aws_region'])
        self.options['aws_access_key_id'] = kwargs.get(
            'aws_access_key_id', settings.DJANGO_SQS_QUEUES[queue_alias]['aws_access_key_id'])
        self.options['aws_secret_access_key'] = kwargs.get(
            'aws_secret_access_key', settings.DJANGO_SQS_QUEUES[queue_alias]['aws_secret_access_key'])
        self.options['queue_name'] = kwargs.get(
            'queue_name', settings.DJANGO_SQS_QUEUES[queue_alias]['queue_name'])
        self.options['sqs_visibility_timeout'] = kwargs.get(
            'sqs_visibility_timeout', settings.DJANGO_SQS_QUEUES[queue_alias].get('sqs_visibility_timeout', 120))
        self.options['sqs_polling_interval'] = kwargs.get(
            'sqs_polling_interval', settings.DJANGO_SQS_QUEUES[queue_alias].get('sqs_polling_interval', 5))
        self.options['standard_input_path'] = kwargs.get(
            'standard_input_path', settings.DJANGO_SQS_QUEUES[queue_alias].get('standard_input_path', '/dev/null'))
        self.options['output_log_path'] = kwargs.get(
            'output_log_path', settings.DJANGO_SQS_QUEUES[queue_alias].get('output_log_path', 'django_sqs_output.log'))
        self.options['error_log_path'] = kwargs.get(
            'error_log_path', settings.DJANGO_SQS_QUEUES[queue_alias].get('error_log_path', 'django_sqs_output.log'))
        self.options['pid_file_path'] = kwargs.get(
            'pid_file_path', settings.DJANGO_SQS_QUEUES[queue_alias].get('pid_file_path', 'django_sqs.pid'))
        self.options['pid_file_timeout'] = kwargs.get(
            'pid_file_timeout', settings.DJANGO_SQS_QUEUES[queue_alias].get('pid_file_timeout', 5))
        self.options['timeout'] = kwargs.get(
            'timeout', settings.DJANGO_SQS_QUEUES[queue_alias].get('timeout'))
        self.options['delete_on_start'] = kwargs.get(
            'delete_on_start', settings.DJANGO_SQS_QUEUES[queue_alias].get('delete_on_start', False))
        self.options['close_database'] = kwargs.get(
            'close_database', settings.DJANGO_SQS_QUEUES[queue_alias].get('close_database', True))
        self.options['suffixes'] = kwargs.get(
            'suffixes', settings.DJANGO_SQS_QUEUES[queue_alias].get('suffixes', ()))
        self.options['message_type'] = kwargs.get(
            'message_type', settings.DJANGO_SQS_QUEUES[queue_alias].get('message_type', 'json'))
        self.options['exception_callback'] = kwargs.get(
            'exception_callback', settings.DJANGO_SQS_QUEUES[queue_alias].get('exception_callback'))
        self.options['prefix'] = kwargs.get(
            'prefix', settings.DJANGO_SQS_QUEUES[queue_alias].get('prefix'))

        # Validate options.
        if not self.options['aws_region']:
            raise Exception("Please set aws-region options.")
        if not self.options['aws_access_key_id']:
            raise Exception("Please set aws-access-key-id options.")
        if not self.options['aws_secret_access_key']:
            raise Exception("Please set aws-secret-access-key options.")
        if not self.options['queue_name']:
            raise Exception("Please set queue-name options as a Queue Name.")
        if not self.options['sqs_visibility_timeout']:
            raise Exception("Please set sqs-visibility-timeout options.")
        if not self.options['sqs_polling_interval']:
            raise Exception("Please set sqs-polling-interval options.")
        if not self.options['pid_file_path']:
            raise Exception("Please set pid-file-path option.")
        if not self.options['output_log_path']:
            raise Exception("Please set output-log-path option.")
        if not self.options['error_log_path']:
            raise Exception("Please set error-log-path option.")
        if not self.options['message_type']:
            raise Exception("Please set message-type option.")
        if self.options['message_type'] not in ('str', 'json'):
            raise Exception("A message-type option can be 'str' or 'json' only.")

        if not self.options['standard_input_path'].startswith('/'):
            self.options['standard_input_path'] = \
                '{}/{}'.format(settings.BASE_DIR, self.options['standard_input_path'])
        if not self.options['output_log_path'].startswith('/'):
            self.options['output_log_path'] = \
                '{}/{}'.format(settings.BASE_DIR, self.options['output_log_path'])
        if not self.options['error_log_path'].startswith('/'):
            self.options['error_log_path'] = \
                '{}/{}'.format(settings.BASE_DIR, self.options['error_log_path'])
        if not self.options['pid_file_path'].startswith('/'):
            self.options['pid_file_path'] = \
                '{}/{}'.format(settings.BASE_DIR, self.options['pid_file_path'])

        # Set file paths to daemonize this object.
        self.stdin_path = self.options['standard_input_path']
        self.stdout_path = self.options['output_log_path']
        self.stderr_path = self.options['error_log_path']
        self.pidfile_path = self.options['pid_file_path']
        self.pidfile_timeout = self.options['pid_file_timeout']

        # Prepare an exception callback function.
        self.exception_callback = get_func(self.options['exception_callback']) if \
            self.options['exception_callback'] else None

    def __str__(self):
        return '{}({})'.format(self.queue_alias, self.options.get('queue_name'))

    @property
    def sqs_client(self):
        if self._sqs_client is None:
            self._sqs_client = boto3.session.Session(
                self.options['aws_access_key_id'],
                self.options['aws_secret_access_key'],
                region_name=self.options['aws_region']
            ).client(
                'sqs'
            )
        return self._sqs_client

    def get_queue_full_name(self, suffix=None):
        _queue_name = self.options['queue_name']
        if suffix:
            if suffix not in self.options['suffixes']:
                warn("Unknown suffix %s" % suffix, UnknownSuffixWarning)
            _queue_name = '%s__%s' % (_queue_name, suffix)
        if self.options['prefix']:
            return '%s__%s' % (self.options['prefix'], _queue_name)
        return _queue_name

    def get_queue_url(self, suffix=None):
        if not self.queue_url:
            self.queue_url = self.sqs_client.create_queue(
                QueueName=self.get_queue_full_name(suffix),
                Attributes={
                    'VisibilityTimeout': str(self.options['sqs_visibility_timeout'])
                }
            ).get('QueueUrl')
        return self.queue_url

    def get_receiver_proxy(self):
        return self.ReceiverProxy(self)

    def send(self, receiver, params=None, queue_url=None, suffix=None):
        if not queue_url:
            queue_url = self.get_queue_url(suffix)
        if 'json' == self.options['message_type']:
            _body = json.dumps({
                'receiver': receiver,
                'params': params
            })
        else:
            _body = json.dumps({
                'receiver': receiver,
                'params': {
                    'message': str(params)
                }
            })
        return self.sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=_body
        )

    def purge_all(self, queue_url=None, suffix=None):
        if not queue_url:
            queue_url = self.get_queue_url(suffix)
        if not queue_url:
            self.logger.error("Wrong queue_url.")
            return False
        self.logger.warning("Delete 10 messages...")
        _messages = self._receive_messages(queue_url, 10)
        if not _messages:
            self.logger.warning("Messages are empty. Stop this process.")
            return True
        for _message in _messages:
            _receipt_handle = _message.get('ReceiptHandle')
            if not _receipt_handle:
                self.logger.warning("Empty receipt handle.")
                continue
            self.delete_message(_receipt_handle, queue_url)
        return True

    def delete_message(self, receipt_handle, queue_url=None, suffix=None):
        if not receipt_handle:
            self.logger.error("Empty receipt handle:{}".format(receipt_handle))
            return False
        if not queue_url:
            queue_url = self.get_queue_url(suffix)
        self.logger.warning("Delete receipt handle({}) from queue({}). ".format(receipt_handle, queue_url))
        self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return True

    def handle_receiver(self, queue_url, receiver, message_id, receipt_handle, params, attributes, md5_of_body):
        self.logger.debug("Receiver starts with message id {}(receiver: {}, params: {})".format(
                              message_id, receiver, params))
        from django.db import connections
        connections.close_all()

        try:
            _delete_on_start = params.pop('delete_on_start')
        except KeyError:
            _delete_on_start = self.options['delete_on_start']
        if _delete_on_start:
            self.logger.info("Delete message on start for message id {}"
                             "(receiver:{}, params:{}, receipt_handle:{})".format(
                                  message_id, receiver, params, receipt_handle))
            self.delete_message(receipt_handle, queue_url)
        _result = False
        try:
            _receiver = get_func(receiver)
            _result = _receiver(**params)
        except Exception as e:
            self.logger.error(str(e))
            if self.exception_callback:
                self.exception_callback(e)
        if not _delete_on_start and _result is True:
            self.logger.info("Delete message on end for message id {}"
                             "(receiver:{}, params:{}, receipt_handle:{})".format(
                                  message_id, receiver, params, receipt_handle))
            self.delete_message(receipt_handle, queue_url)
        self.logger.info("Receiver ends with message id {}"
                         "(receiver:{}, params:{}, receipt_handle:{})".format(
                              message_id, receiver, params, receipt_handle))
        return _result

    def receive(self, queue_url, message_id, receipt_handle, body, attributes, md5_of_body):
        if self.options['timeout']:
            signal.alarm(self.options['timeout'])
            signal.signal(signal.SIGALRM, sigalrm_handler)
        if settings.DEBUG:
            self.logger.debug("Message received. message_id:{}, receipt_handle:{}, body:{}, "
                              "attributes:{}, md5_of_body:{}".format(
                                  message_id, receipt_handle, body, str(attributes), md5_of_body))
        else:
            self.logger.info("Message received. message_id:{}, body:{}".format(message_id, body))
        try:
            _body = json.loads(body)
            _receiver = _body.get('receiver')
            if _receiver is None:
                raise Exception("Not configured for received messages.")
            with ThreadPoolExecutor(max_workers=5) as executor:
                _params = {
                    'queue_url': queue_url,
                    'receiver': _receiver,
                    'message_id': message_id,
                    'receipt_handle': receipt_handle,
                    'params': _body.get('params'),
                    'attributes': attributes,
                    'md5_of_body': md5_of_body
                }
                try:
                    _future = executor.submit(self.handle_receiver, **_params)
                    self.futures.append(_future)
                except Exception as e:
                    self.logger.error("An error occurred during submitting receiver "
                                      "to thread pool executor: {}".format(e))
        finally:
            if self.options['timeout']:
                try:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                except TimedOut:
                    # possible race condition if we don't cancel the
                    # alarm in time.  Now there is no race condition
                    # threat, since alarm already rang.
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
            if self.options['close_database']:
                from django.db import connections
                connections.close_all()

    def receive_single(self, suffix=None):
        """Receive single message from the queue.

        This method is here for debugging purposes.  It receives
        single message from the queue, processes it, deletes it from
        queue and returns (message, handler_result_value) pair.

        Args:
            suffix:
        """
        _queue_url = self.get_queue_url(suffix)
        _message = self._receive_messages(_queue_url)[:1]
        _message_id = _message.get('MessageId')
        _receipt_handle = _message.get('ReceiptHandle')
        _body = _message.get('Body')
        _attributes = _message.get('Attributes')
        _md5_of_body = _message.get('MD5OfBody')

        if self.options['delete_on_start']:
            self.delete_message(_receipt_handle, _queue_url)
        self.receive(_queue_url, _message_id, _receipt_handle, _body, _attributes, _md5_of_body)
        if not self.options['delete_on_start']:
            self.delete_message(_receipt_handle, _queue_url)
        return

    def receive_loop(self, message_limit=None, suffix=None):
        """Run receiver loop.

        If `message_limit' number is given, return after processing
        this number of messages.
        Args:
            message_limit:
            suffix:
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
                    self.logger.debug("Result of some future: {}".format(f.result()))
                self.futures = list()

            _messages = self._receive_messages(_queue_url)
            if not _messages:
                time.sleep(self.options['sqs_polling_interval'] + (random.randint(-10, 10) / 10))
            else:
                try:
                    _message = _messages[0]
                    self.logger.debug("Received message: {}".format(_message))
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
                        "Caught exception in receive loop. Received message:{}, exception:{}".format(
                            _messages, e))
                    if self.exception_callback:
                        self.exception_callback(e)

    def _receive_messages(self, queue_url, number_messages=1):
        _response = self.sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=number_messages,
            AttributeNames=['All'])
        _response_metadata = _response.get('ResponseMetadata')
        if 200 != _response_metadata.get('HTTPStatusCode'):
            self.logger.error("SQS Response Metadata: {}".format(_response_metadata))
        _messages = _response.get('Messages')
        return _messages

    def exit_gracefully(self, signum, frame):
        _logger = logging.getLogger(django_sqs.__name__)
        if signal.SIGTERM == signum:
            _logger.info("Received termination signal. Prepare to exit...")
            if 1 <= len(self.futures):
                for f in as_completed(self.futures):
                    self.logger.debug("Result of some future: {}".format(f.result()))
            self.kill_now = True
        else:
            _logger.info("Received signal {}, but there is no process for this signal.".format(signum))

    def run(self):

        self.logger.handlers = list()

        # Set up logger file handler in daemon process.
        _formatter = DjangoSQSFormatter(
            fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                django_sqs.PROJECT + '] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        )
        _handler = WatchedFileHandler(self.stdout_path, encoding='utf8')
        _handler.setFormatter(_formatter)
        self.logger.addHandler(_handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("Django SQS starts!")
        self.logger.info("Set new logger up.")
        for _h in self.logger.handlers:
            self.logger.debug("Added logging handler: {}".format(_h))

        for _logger_name in settings.LOGGING.get('loggers'):
            _logger_setting = settings.LOGGING.get('loggers').get(_logger_name)
            if 'file' in _logger_setting.get('handlers'):
                self.logger.addHandler(_handler)
                self.logger.info("Re-setup for logger [{}] in Django settings.".format(_logger_name))

        self.logger.info('Initiating queue[{}] with these options:'.format(self.options['queue_name']))
        self.logger.info('\tqueue name: {}'.format(self.options['queue_name']))
        self.logger.info('\tpid file path: {}'.format(self.options['pid_file_path']))
        self.logger.info('\toutput log path: {}'.format(self.options['output_log_path']))
        self.logger.info('\terror log path: {}'.format(self.options['error_log_path']))
        self.logger.info('\tmessage type: {}'.format(self.options['message_type']))
        self.logger.info('\texception callback: {}'.format(self.options['exception_callback']))

        # Set signal handler up.
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.receive_loop()
        self.logger.info("{} is completely exited.".format(django_sqs.PROJECT))
