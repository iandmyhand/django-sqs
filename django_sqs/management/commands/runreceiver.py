import django_sqs
import logging
import os
import signal

from django.conf import settings
from django.core.management.base import BaseCommand
from django_sqs.registered_queue import RegisteredQueue
from django_sqs.daemonize import CustomDaemonRunner
from logging.handlers import WatchedFileHandler

logger = logging.getLogger(django_sqs.__name__)


# signal name dict for logging
_signals = {}
for name in dir(signal):
    if name.startswith('SIG'):
        _signals[getattr(signal, name)] = name


def _status_string(status):
    "Pretty status description for exited child."

    if os.WIFSIGNALED(status):
        return "Terminated by %s (%d)" % (
            _signals.get(os.WTERMSIG(status), "unknown signal"),
            os.WTERMSIG(status))

    if os.WIFEXITED(status):
        return "Exited with status %d" % os.WEXITSTATUS(status)

    if os.WIFSTOPPED(status):
        return "Stopped by %s (%d)" % (
            _signals.get(os.WSTOPSIG(status), "unknown signal"),
            os.WSTOPSIG(status))

    if os.WIFCONTINUED(status):
        return "Continued from stop"

    return "Unknown reason (%r)" % status


class Command(BaseCommand):
    help = "Run Amazon SQS receiver for queues registered with django_sqs."
    args = '[queue_name;package.module:receiver_name [queue_name;package.module:receiver_name [...]]]'

    _queues = None

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self._queues = list()

    def add_arguments(self, parser):
        parser.add_argument(dest='action', metavar='ACTION', action='store',
                            help='[start|restart|stop]')
        parser.add_argument('-q', '--queues', nargs='+',
                            dest='queues', type=str,
                            help="[queue_name "
                                 "[queue_name [...]]]")
        parser.add_argument('-d', '--daemonize',
                            dest='daemonize', type=bool, default=False,
                            help="Fork into background as a daemon. "
                                 "You can set this up at django\'s settings file: "
                                 "DJANGO_SQS_DAEMONIZE=[True|False].")
        parser.add_argument('-l', '--output-log-path',
                            dest='output_log_path', type=str, default=None,
                            help="Standard output log file. "
                                 "You can set this up at django\'s settings file: "
                                 "DJANGO_SQS_OUTPUT_LOG_PATH=[OUTPUT_LOG_FILE_PATH].")
        parser.add_argument('-e', '--error-log-path',
                            dest='error_log_path', type=str, default=None,
                            help="Standard error log file."
                                 "You can set this up at django\'s settings file: "
                                 "DJANGO_SQS_ERROR_LOG_PATH=[ERROR_LOG_FILE_PATH].")
        parser.add_argument('-p', '--pid-file-path',
                            dest='pid_file_path', type=str, default=None,
                            help="Store process ID in a file"
                                 "You can set this up at django\'s settings file: "
                                 "DJANGO_SQS_PID_FILE_PATH=[PID_FILE_PATH].")
        parser.add_argument('-s', '--suffix',
                            dest='suffix', default=None, metavar='SUFFIX',
                            help="Append SUFFIX to queue name.")
        parser.add_argument('-t', '--message-type',
                            dest='message_type', type=str, default=None,
                            help="A Type of message. str and json are supported only.")
        parser.add_argument('-m', '--message-limit',
                            dest='message_limit', type=int, default=None,
                            help="Exit after processing N messages")

    def handle(self, *args, **options):
        self.validate()

        _action = options['action']
        if _action not in ('start', 'restart', 'stop'):
            raise Exception('%s is not supported action.' % str(_action))

        if options.get('queues'):
            for _queue in options.get('queues'):
                self._queues.append(_queue)
        if not self._queues:
            self._queues = getattr(settings, 'DJANGO_SQS_QUEUES', None)
        if not self._queues:
            raise Exception('There are no queues to initialize.')

        _daemonize = options.get('daemonize')
        if not _daemonize:
            _daemonize = getattr(settings, 'DJANGO_SQS_DAEMONIZE', None)

        _pid_file_path = options.get('pid_file_path')
        if not _pid_file_path:
            _pid_file_path = getattr(settings, 'DJANGO_SQS_PID_FILE_PATH', 'django_sqs.pid')

        _output_log_path = options.get('output_log_path')
        if not _output_log_path:
            _output_log_path = getattr(settings, 'DJANGO_SQS_OUTPUT_LOG_PATH', 'django_sqs_out.log')

        _error_log_path = options.get('error_log_path')
        if not _error_log_path:
            _error_log_path = getattr(settings, 'DJANGO_SQS_ERROR_LOG_PATH', 'django_sqs_err.log')

        _message_type = options.get('message_type')
        if not _message_type:
            _message_type = getattr(settings, 'DJANGO_SQS_MESSAGE_TYPE', 'json')

        # Set logger up.
        if not logger.handlers:
            _formatter = logging.Formatter(
                fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                    django_sqs.PROJECT + '] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            _handler = WatchedFileHandler(_output_log_path)
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
            logger.setLevel(logging.DEBUG)
            logger.info('Set new logger up.')
        else:
            logger.info('Use logger already set up.')

        # Close the DB connection now and let Django reopen it when it
        # is needed again.  The goal is to make sure that every
        # process gets its own connection
        from django.db import connection
        connection.close()

        for _queue in self._queues:
            logger.info('Initiating queue[%s] with these options:' % str(_queue))
            logger.info('   action: ' + str(_action))
            logger.info('   daemonize: ' + str(_daemonize))
            logger.info('   pid file path: ' + str(_pid_file_path))
            logger.info('   output log path: ' + str(_output_log_path))
            logger.info('   error log path: ' + str(_error_log_path))
            logger.info('   message type: ' + str(_message_type))

            _registered_queue = RegisteredQueue(
                _queue,
                std_out_path=_output_log_path,
                std_err_path=_error_log_path,
                pid_file_path=_pid_file_path,
                message_type=_message_type)
            if _daemonize:
                logger.debug('Initiating daemon runner for %s...' % str(_registered_queue))
                _runner = CustomDaemonRunner(_registered_queue, (__name__, _action))
                logger.debug('Initiated daemon runner for %s...' % str(_registered_queue))
                logger.info('%s daemon for %s...' % (str(_action), str(_registered_queue)))
                _runner.do_action()
            else:
                logger.info('This is not a daemonized process. Use first queue.')
                _registered_queue.receive_loop(options.get('message_limit'), options.get('suffix'))
            logger.info('Exit process for %s' % str(_registered_queue))
