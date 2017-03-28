import django_sqs
import errno
import logging
import os

from django.core.management.base import BaseCommand
from django_sqs.registered_queue import RegisteredQueue
from django_sqs.daemonize import CustomDaemonRunner
from logging.handlers import WatchedFileHandler

logger = logging.getLogger(django_sqs.__name__)


def pid_exists(pid):
    """
    Check whether pid exists in the current process table.
    UNIX only.

    Args:
        pid:

    Returns:

    """

    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


class Command(BaseCommand):
    help = "Run Amazon SQS receiver for queues registered with django_sqs."
    args = '[queue_name;package.module:receiver_name [queue_name;package.module:receiver_name [...]]]'

    _queue = None

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument(dest='action', metavar='ACTION', action='store',
                            help='[start|restart|stop]')
        parser.add_argument('-qa', '--queue-alias',
                            dest='queue_alias', type=str, default=None,
                            help="Set the Queue Alias.")
        parser.add_argument('-ar', '--aws-region',
                            dest='aws_region', type=str, default=None,
                            help="Set the AWS Region.")
        parser.add_argument('-ak', '--aws-access-key-id',
                            dest='aws_access_key_id', type=str, default=None,
                            help="Set the AWS Access Key ID.")
        parser.add_argument('-as', '--aws-secret-access-key',
                            dest='aws_secret_access_key', type=str, default=None,
                            help="Set the AWS Secret Access Key.")
        parser.add_argument('-q', '--queue-name',
                            dest='queue_name', type=str, default=None,
                            help="Set a --queue-name option.")
        parser.add_argument('-sv', '--sqs-visibility-timeout',
                            dest='sqs_visibility_timeout', type=int, default=120,
                            help="AWS SQS visibility option.")
        parser.add_argument('-sp', '--sqs-polling-interval',
                            dest='sqs_polling_interval', type=int, default=5,
                            help="AWS SQS visibility option.")
        parser.add_argument('-l', '--output-log-path',
                            dest='output_log_path', type=str, default=None,
                            help="Standard output log file. "
                                 "Set a --output-log-path option.")
        parser.add_argument('-e', '--error-log-path',
                            dest='error_log_path', type=str, default=None,
                            help="Standard error log file."
                                 "Set a --error-log-path option.")
        parser.add_argument('-p', '--pid-file-path',
                            dest='pid_file_path', type=str, default=None,
                            help="Set a --pid-file-path option.")
        parser.add_argument('-s', '--suffix',
                            dest='suffix', default=None, metavar='SUFFIX',
                            help="Append SUFFIX to queue name.")
        parser.add_argument('-t', '--message-type',
                            dest='message_type', type=str, default=None,
                            help="A Type of message. str and json are supported only.")
        parser.add_argument('-m', '--message-limit',
                            dest='message_limit', type=int, default=None,
                            help="Exit after processing N messages")
        parser.add_argument('-ec', '--exception-callback',
                            dest='exception_callback', type=str, default=None,
                            help="a Function that executed when an exception occurred.")

    def handle(self, *args, **options):
        self.validate()

        _action = options.get('action')
        if _action not in ('start', 'restart', 'stop'):
            raise Exception('{} is not supported action.'.format(_action))

        if not options.get('queue_alias'):
            raise Exception("Please set --queue-alias options.")

        _queue = RegisteredQueue(**options)

        # Set logger up.
        if not logger.handlers:
            _formatter = logging.Formatter(
                fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                    django_sqs.PROJECT + '] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            _handler = WatchedFileHandler(_queue.options['output_log_path'])
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
            logger.setLevel(logging.DEBUG)
            logger.info('Set new logger up.')

        # Close the DB connection now and let Django reopen it when it
        # is needed again.  The goal is to make sure that every
        # process gets its own connection
        from django.db import connection
        connection.close()

        if 'start' == _action and os.path.isfile(_queue.options['pid_file_path']):
            with open(_queue.options['pid_file_path'], 'r') as _pid_file:
                for _pid in _pid_file:
                    try:
                        _pid = int(_pid.rstrip('\n'))
                    except (AttributeError, ValueError):
                        _pid = -1
                    logger.info('PID file exists already, so checking whether PID({}) is running.'.format(_pid))
                    if pid_exists(_pid):
                        logger.info('PID({}) is already running, so exit this process.'.format(_pid))
                        return

        _runner = CustomDaemonRunner(_queue, (__name__, _action))
        logger.info('Initiated daemon runner to {} {}.'.format(_action, _queue.options['queue_name']))
        _runner.do_action()
        logger.info('Exit process for {}.'.format(_queue))
