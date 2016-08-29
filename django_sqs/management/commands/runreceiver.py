import django_sqs
import logging
import os
import signal

from logging.handlers import WatchedFileHandler
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
        parser.add_argument('--daemonize',
                            dest='daemonize', type=str, default=None,
                            help='Fork into background as a daemon. Given value is used as log file.')
        parser.add_argument('--suffix',
                            dest='suffix', default=None, metavar='SUFFIX',
                            help="Append SUFFIX to queue name.")
        parser.add_argument('--pid-file',
                            dest='pid_file', type=str, default=None,
                            help="Store process ID in a file")
        parser.add_argument('--message-limit',
                            dest='message_limit', type=int, default=None,
                            help='Exit after processing N messages')

    def handle(self, *queues, **options):
        self.validate()

        _daemonize = options.get('daemonize')
        if not _daemonize:
            if hasattr(settings, 'DAEMONIZE') and settings.DAEMONIZE:
                _daemonize = settings.DAEMONIZE

        if _daemonize:
            from django.utils.daemonize import become_daemon
            become_daemon()

            _pid_file = options.get('pid_file')
            if not _pid_file:
                if hasattr(settings, 'PID_FILE') and settings.PID_FILE:
                    _pid_file = settings.PID_FILE
            if _pid_file:
                with open(_pid_file, 'w') as f:
                    f.write('%d\n' % os.getpid())

        if not logger.handlers:
            _formatter = logging.Formatter(
                fmt='[%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d ' +
                    django_sqs.PROJECT_NAME + '] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')
            _handler = WatchedFileHandler(_daemonize)
            _handler.setFormatter(_formatter)
            logger.addHandler(_handler)
            logger.setLevel(logging.DEBUG)
            logger.info('Set new logger up.')
        else:
            logger.info('Use logger already set up.')

        _queues = list()
        if queues:
            for _queue in queues:
                _queue_name, _receiver = _queue.split('=')
                logger.info('Initiating queue[%s] and receiver[%s]...' % (_queue_name, _receiver))
                django_sqs.register(_queue_name, _receiver)
                _queues.append({'queue_name': _queue_name, 'receiver': _receiver})
        elif hasattr(settings, 'QUEUES') and settings.QUEUES:
            for _queue in settings.QUEUES:
                _queue_name = _queue.get('queue_name')
                _receiver = _queue.get('receiver')
                logger.info('Initiating queue[%s] and receiver[%s]...' % (_queue_name, _receiver))
                django_sqs.register(_queue_name, _receiver)
                _queues.append({'queue_name': _queue_name, 'receiver': _receiver})
        else:
            raise Exception('There is no queues setting to initialize.')

        if len(_queues) == 1:
            self._receive(_queues[0]['queue_name'],
                          suffix=options.get('suffix'),
                          message_limit=options.get('message_limit', None))
        else:
            # Close the DB connection now and let Django reopen it when it
            # is needed again.  The goal is to make sure that every
            # process gets its own connection
            from django.db import connection
            connection.close()

            os.setpgrp()
            children = {}  # queue name -> pid
            for _queue in _queues:
                pid = self._fork_child(_queue['queue_name'],
                                       options.get('message_limit', None))
                children[pid] = _queue['queue_name']
                logger.info("Forked %s for %s" % (pid, _queue['queue_name']))

            while children:
                pid, status = os.wait()
                queue_name = children[pid]
                logger.error("Child %d (%s) exited: %s" % (
                    pid, children[pid], _status_string(status)))
                del children[pid]

                pid = self._fork_child(queue_name)
                children[pid] = queue_name
                logger.info("Respawned %s for %s" % (pid, queue_name))
        logger.info(django_sqs.PROJECT_NAME + ' is completely exited.')

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
