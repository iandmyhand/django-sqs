import django_sqs
import logging
import os
import signal
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand

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

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument(dest='action', metavar='ACTION', action='store',
                            help='[start|restart|stop]')
        parser.add_argument('-d', '--daemonize',
                            dest='daemonize', type=bool, default=False,
                            help="Fork into background as a daemon. "
                                 "You can set this up at django\'s settings file: ")

    def handle(self, *args, **options):
        self.validate()

        _action = options.get('action')
        if _action not in ('start', 'restart', 'stop'):
            raise Exception('%s is not supported action.' % str(_action))

        _queue_settings = getattr(settings, 'DJANGO_SQS_QUEUES', None)
        if not _queue_settings:
            raise Exception('There are no queues to initialize.')

        _daemonize = options.get('daemonize')
        if not _daemonize:
            _daemonize = getattr(settings, 'DJANGO_SQS_DAEMONIZE', False)

        if _daemonize:
            for _queue_alias in _queue_settings:

                _command = list()
                _command.append('python3')
                _command.append('manage.py')
                _command.append('runreceiver_daemon')
                _command.append(_action)
                _command.append('--queue-alias')
                _command.append(_queue_alias)
                _command.append('--aws-region')
                _command.append(_queue_settings[_queue_alias]['aws_region'])
                _command.append('--aws-access-key-id')
                _command.append(_queue_settings[_queue_alias]['aws_access_key_id'])
                _command.append('--aws-secret-access-key')
                _command.append(_queue_settings[_queue_alias]['aws_secret_access_key'])
                _command.append('--queue-name')
                _command.append(_queue_settings[_queue_alias]['queue_name'])
                _command.append('--sqs-visibility-timeout')
                _command.append(str(_queue_settings[_queue_alias]['sqs_visibility_timeout']))
                _command.append('--sqs-polling-interval')
                _command.append(str(_queue_settings[_queue_alias]['sqs_polling_interval']))
                _command.append('--output-log-path')
                _command.append(_queue_settings[_queue_alias]['output_log_path'])
                _command.append('--error-log-path')
                _command.append(_queue_settings[_queue_alias]['error_log_path'])
                _command.append('--pid-file-path')
                _command.append(_queue_settings[_queue_alias]['pid_file_path'])
                _command.append('--message-type')
                _command.append(_queue_settings[_queue_alias]['message_type'])
                _command.append('--exception-callback')
                _command.append(_queue_settings[_queue_alias]['exception_callback'])

                subprocess.Popen(' '.join(_command), shell=True).communicate()
