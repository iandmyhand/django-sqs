import django_sqs
import errno
import logging
import os
import signal
import subprocess

from django.conf import settings
from django.core.management.base import BaseCommand
from django_sqs.registered_queue import RegisteredQueue
from django_sqs.daemonize import CustomDaemonRunner
from logging.handlers import WatchedFileHandler

logger = logging.getLogger(django_sqs.__name__)


class Command(BaseCommand):
    help = "Run a purge command to delete message from SQS."
    args = "--queue-alias [QUEUE_ALIAS] --receipt-handle [RECEIPT_HANDLE]"

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument('-q', '--queue-alias',
                            dest='queue_alias', type=str, default=None,
                            help="Put a Queue alias that set in the Django settings file.")
        parser.add_argument('-rh', '--receipt-handle',
                            dest='receipt_handle', type=str, default=None,
                            help="Put a SQS's ReceiptHandle value to remove from the Queue.")

    def handle(self, *args, **options):
        self.validate()

        _queue_alias = options.get('queue_alias')
        if not _queue_alias:
            raise Exception("Please put a Queue alias with --queue-alias option.")

        _receipt_handle = options.get('receipt_handle')
        if not _receipt_handle:
            raise Exception("Please put a ReceiptHandle value with --receipt-handle option.")

        django_sqs.purge(_queue_alias, _receipt_handle)
        logger.warning("A message in {} deleted with receipt handle[{}].".format(_queue_alias, _receipt_handle))
