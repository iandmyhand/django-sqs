import sys

from django.core.management.base import NoArgsCommand

import django_sqs


class Command(NoArgsCommand):
    help = "Provides information about used SQS queues and the number of items in each of them."

    def handle_noargs(self, **options):
        sys.stdout.write("\n\n")
        sys.stdout.write("Active SQS queues\n")
        sys.stdout.write("-----------------\n")
        sys.stdout.write("\n\n")
        for label, queue in django_sqs.queues.items():
            q = queue.get_queue()
            sys.stdout.write("%-30s: %4s\n" % (label, q.count()))
            if queue.suffixes:
                for suffix in queue.suffixes:
                    q = queue.get_queue(suffix)
                    sys.stdout.write(" .%-28s: %4s\n" % (suffix, q.count()))
