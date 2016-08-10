import logging
import os
from optparse import make_option
import signal
import sys

from django.core.management.base import BaseCommand

import django_sqs


# null handler to avoid warnings
class _NullHandler(logging.Handler):
    def emit(self, record):
        pass

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

    def add_arguments(self, parser):
        parser.add_argument('--daemonize',
                            action='store_true', dest='daemonize', default=False,
                            help='Fork into background as a daemon.')
        parser.add_argument('--daemon-stdout-log',
                            dest='stdout_log', default='/dev/null',
                            help="Log daemon's standard output stream to a file")
        parser.add_argument('--daemon-stderr-log',
                            dest='stderr_log', default='/dev/null',
                            help="Log daemon's standard error stream to a file")
        parser.add_argument('--suffix',
                            dest='suffix', default=None, metavar='SUFFIX',
                            help="Append SUFFIX to queue name.")
        parser.add_argument('--pid-file',
                            dest='pid_file', default=None,
                            help="Store process ID in a file")
        parser.add_argument('--message-limit',
                            dest='message_limit', default=None, type=int,
                            help='Exit after processing N messages')

    def handle(self, *queues, **options):
        self.validate()

        _queues = list()
        for _queue in queues:
            _queue_name, _receiver = _queue.split(':')
            sys.stdout.write('Initiating queue[%s] and receiver[%s]...\n' % (_queue_name, _receiver))
            django_sqs.register(_queue_name, _receiver)
            _queues.append({'queue_name': _queue_name, 'receiver': _receiver})

        if options.get('daemonize', False):
            from django.utils.daemonize import become_daemon
            become_daemon(out_log=options.get('stdout_log', '/dev/null'),
                          err_log=options.get('stderr_log', '/dev/null'))

        if options.get('pid_file'):
            with open(options['pid_file'], 'w') as f:
                f.write('%d\n' % os.getpid())

        if len(_queues) == 1:
            self.receive(_queues[0]['queue_name'],
                         suffix=options.get('suffix'),
                         message_limit=options.get('message_limit', None))
        else:
            # fork a group of processes.  Quick hack, to be replaced
            # ASAP with something decent.
            _log = logging.getLogger('django_sqs.runreceiver.master')
            _log.addHandler(_NullHandler())

            # Close the DB connection now and let Django reopen it when it
            # is needed again.  The goal is to make sure that every
            # process gets its own connection
            from django.db import connection
            connection.close()

            os.setpgrp()
            children = {}               # queue name -> pid
            for _queue in _queues:
                pid = self.fork_child(_queue['queue_name'],
                                      options.get('message_limit', None))
                children[pid] = _queue['queue_name']
                _log.info("Forked %s for %s" % (pid, _queue['queue_name']))

            while children:
                pid, status = os.wait()
                queue_name = children[pid]
                _log.error("Child %d (%s) exited: %s" % (
                    pid, children[pid], _status_string(status)))
                del children[pid]

                pid = self.fork_child(queue_name)
                children[pid] = queue_name
                _log.info("Respawned %s for %s" % (pid, queue_name))

    def fork_child(self, queue_name, message_limit=None):
        pid = os.fork()
        if pid:
            # parent
            return pid
        # child
        _log = logging.getLogger('django_sqs.runreceiver.%s' % queue_name)
        _log.addHandler(_NullHandler())
        _log.info("Start receiving.")
        self.receive(queue_name, message_limit=message_limit)
        _log.error("CAN'T HAPPEN: exiting.")
        raise SystemExit(0)

    def receive(self, queue_name, message_limit=None, suffix=None):
        _log = logging.getLogger('django_sqs.runreceiver.%s' % queue_name)

        _rq = django_sqs.queues[queue_name]
        if _rq.receiver:
            if message_limit is None:
                message_limit_info = ''
            else:
                message_limit_info = ' %d messages' % message_limit

            _log.info('Receiving%s from queue %s%s...\n' % (
                message_limit_info, queue_name,
                ('.%s' % suffix if suffix else ''),
                )
            )
            _rq.receive_loop(message_limit=message_limit,
                             suffix=suffix)
        else:
            sys.stdout.write('Queue %s has no receiver, aborting.\n' % queue_name)
