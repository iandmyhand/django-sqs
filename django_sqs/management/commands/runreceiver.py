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

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument('--daemonize',
                            dest='daemonize', type=str, default=None,
                            help='Fork into background as a daemon. Given value is used as log file.')
        parser.add_argument('--suffix',
                            dest='suffix', default=None, metavar='SUFFIX',
                            help="Append SUFFIX to queue name.")
        parser.add_argument('--pid-file-path',
                            dest='pid_file_path', type=str, default=None,
                            help="Store process ID in a file")
        parser.add_argument('--message-limit',
                            dest='message_limit', type=int, default=None,
                            help='Exit after processing N messages')

    def handle(self, *args, **options):
        self.validate()

        _daemonize = options.get('daemonize')
        if not _daemonize:
            _daemonize = getattr(settings, 'DAEMONIZE')

        _pid_file_path = options.get('pid_file_path')
        if not _pid_file_path:
            _pid_file_path = getattr(settings, 'PID_FILE_PATH', 'sqs.pid')

        if not logger.handlers:
            _formatter = logging.Formatter(
                fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                    django_sqs.PROJECT + '] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            _handler = WatchedFileHandler(_daemonize)
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
            logger.setLevel(logging.DEBUG)
            logger.info('Set new logger up.')
        else:
            logger.info('Use logger already set up.')

        _queues = list()
        # if queues:
        #     for _queue in queues:
        #         _queue_name, _receiver = _queue.split('=')
        #         logger.info('Initiating queue[%s] and receiver[%s]...' % (_queue_name, _receiver))
        #         django_sqs.register(_queue_name, _receiver)
        #         _queues.append({'queue_name': _queue_name, 'receiver': _receiver})
        if hasattr(settings, 'QUEUES') and settings.QUEUES:
            for _queue in settings.QUEUES:
                _queue_name = _queue.get('queue_name')
                _receiver = _queue.get('receiver')
                logger.info('Initiating queue[%s] and receiver[%s]...' % (_queue_name, _receiver))
                _registered_queue = RegisteredQueue(_queue_name, django_sqs._get_func(_receiver),
                                                    std_out_path=_daemonize,
                                                    std_err_path=_daemonize,
                                                    pid_file_path=_pid_file_path)
                _runner = CustomDaemonRunner(_registered_queue, (__name__, args[0]))
                for _handler in logger.handlers:
                    logger.debug('handler: ' + str(_handler))
                    logger.debug('handler: ' + str(_handler.__dict__))
                # _runner.daemon_context.files_preserve = [logger.handlers[1].stream]
                _runner.do_action()

                # django_sqs.register(_queue_name, _receiver)
                # _queues.append({'queue_name': _queue_name, 'receiver': _receiver})
        else:
            raise Exception('There are no queues to initialize.')

        # if len(_queues) == 1:
        #     self._receive(_queues[0]['queue_name'],
        #                   suffix=options.get('suffix'),
        #                   message_limit=options.get('message_limit', None))
        # else:
        #     # Close the DB connection now and let Django reopen it when it
        #     # is needed again.  The goal is to make sure that every
        #     # process gets its own connection
        #     from django.db import connection
        #     connection.close()
        #
        #     os.setpgrp()
        #     children = {}  # queue name -> pid
        #     for _queue in _queues:
        #         pid = self._fork_child(_queue['queue_name'],
        #                                options.get('message_limit', None))
        #         children[pid] = _queue['queue_name']
        #         logger.info("Forked %s for %s" % (pid, _queue['queue_name']))
        #
        #     while children:
        #         pid, status = os.wait()
        #         queue_name = children[pid]
        #         logger.error("Child %d (%s) exited: %s" % (
        #             pid, children[pid], _status_string(status)))
        #         del children[pid]
        #
        #         pid = self._fork_child(queue_name)
        #         children[pid] = queue_name
        #         logger.info("Respawned %s for %s" % (pid, queue_name))

    def _fork_child(self, queue_name, message_limit=None):
        pid = os.fork()
        if pid:
            # parent
            return pid
        # child
        logger.info("Start receiving.")
        self._receive(queue_name, message_limit=message_limit)
        logger.error("CAN'T HAPPEN: exiting.")
        raise SystemExit(0)

    def _receive(self, queue_name, message_limit=None, suffix=None):
        _rq = django_sqs.queues[queue_name]
        if _rq.receiver:
            if message_limit is None:
                message_limit_info = ''
            else:
                message_limit_info = ' %d messages' % message_limit

            logger.info('Ready to receiving%s from queue %s%s...\n' % (
                message_limit_info, queue_name,
                ('.%s' % suffix if suffix else ''),
                )
            )
            _rq.receive_loop(message_limit=message_limit,
                             suffix=suffix)
        else:
            logger.error('Queue %s has no receiver, aborting.' % queue_name)
