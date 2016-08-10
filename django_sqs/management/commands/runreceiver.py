import logging
import os
import signal

from django.conf import settings
from django.core.management.base import BaseCommand

import django_sqs


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
    args = '[queue_name:receiver_name [queue_name:receiver_name [...]]]'

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger('django_sqs')

    def add_arguments(self, parser):
        parser.add_argument('--daemonize',
                            dest='daemonize', type=str, default='/dev/null',
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
        if _daemonize:
            from django.utils.daemonize import become_daemon
            become_daemon(out_log=_daemonize,
                          err_log=_daemonize)

        if options.get('pid_file'):
            with open(options['pid_file'], 'w') as f:
                f.write('%d\n' % os.getpid())

        self._logger.setLevel(logging.DEBUG if settings.DEBUG else logging.INFO)
        _formatter = logging.Formatter('[%(levelname)s %(asctime)s %(name)s] %(message)s')
        _handler = logging.FileHandler(_daemonize)
        _handler.setFormatter(_formatter)
        self._logger.addHandler(_handler)

        _queues = list()
        for _queue in queues:
            _queue_name, _receiver = _queue.split(':')
            self._logger.info('Initiating queue[%s] and receiver[%s]...' % (_queue_name, _receiver))
            django_sqs.register(_queue_name, _receiver)
            _queues.append({'queue_name': _queue_name, 'receiver': _receiver})

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
                self._logger.info("Forked %s for %s" % (pid, _queue['queue_name']))

            while children:
                pid, status = os.wait()
                queue_name = children[pid]
                self._logger.error("Child %d (%s) exited: %s" % (
                    pid, children[pid], _status_string(status)))
                del children[pid]

                pid = self._fork_child(queue_name)
                children[pid] = queue_name
                self._logger.info("Respawned %s for %s" % (pid, queue_name))

    def _fork_child(self, queue_name, message_limit=None):
        pid = os.fork()
        if pid:
            # parent
            return pid
        # child
        self._logger.info("Start receiving.")
        self._receive(queue_name, message_limit=message_limit)
        self._logger.error("CAN'T HAPPEN: exiting.")
        raise SystemExit(0)

    def _receive(self, queue_name, message_limit=None, suffix=None):
        _rq = django_sqs.queues[queue_name]
        if _rq.receiver:
            if message_limit is None:
                message_limit_info = ''
            else:
                message_limit_info = ' %d messages' % message_limit

            self._logger.info('Receiving%s from queue %s%s...\n' % (
                message_limit_info, queue_name,
                ('.%s' % suffix if suffix else ''),
                )
            )
            _rq.receive_loop(message_limit=message_limit,
                             suffix=suffix)
        else:
            self._logger.error('Queue %s has no receiver, aborting.' % queue_name)
