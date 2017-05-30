import django_sqs
import logging

from django.core.management.base import BaseCommand

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
        parser.add_argument('-a', '--all',
                            dest='all', type=bool, default=False,
                            help="Whether purge all messages or not.")

    def handle(self, *args, **options):
        self.validate()

        _queue_alias = options.get('queue_alias')
        if not _queue_alias:
            raise Exception("Please put a Queue alias with --queue-alias option.")

        _receipt_handle = options.get('receipt_handle')
        if not _receipt_handle:
            if options.get('all'):
                django_sqs.purge_all(_queue_alias)
            else:
                raise Exception("Please put a ReceiptHandle value with --receipt-handle option.")
        else:
            django_sqs.purge(_queue_alias, _receipt_handle)
        logger.warning("A message in {} deleted with receipt handle[{}].".format(_queue_alias, _receipt_handle))
