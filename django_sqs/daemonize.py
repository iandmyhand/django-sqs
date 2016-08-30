from daemon.runner import DaemonRunner
from daemon.runner import make_pidlockfile
from daemon.daemon import DaemonContext


class CustomDaemonRunner(DaemonRunner):

    def __init__(self, app, argv):
        """

        Args:
            app (object): Application name
            argv (list(str)):
                index 0: program name
                index 1: action to perform

        Returns:
            None

        """
        # Parse args with passed from python's argument parser.
        self.parse_args(argv)
        self.app = app
        self.daemon_context = DaemonContext()

        # Use logger.
        self.daemon_context.stdin = None
        self.daemon_context.stdout = None
        self.daemon_context.stderr = None

        self.pidfile = None
        if app.pidfile_path is not None:
            self.pidfile = make_pidlockfile(app.pidfile_path, app.pidfile_timeout)
        self.daemon_context.pidfile = self.pidfile

    def parse_args(self, argv=None):
        super(CustomDaemonRunner, self).parse_args(argv)
