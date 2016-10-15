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
        if not hasattr(app, 'stdin_path'):
            raise Exception('App must has stdin_path.')
        if not hasattr(app, 'stdout_path'):
            raise Exception('App must has stdout_path.')
        if not hasattr(app, 'stderr_path'):
            raise Exception('App must has stderr_path.')
        if not hasattr(app, 'pidfile_path'):
            raise Exception('App must has pidfile_path.')
        if not hasattr(app, 'pidfile_timeout'):
            raise Exception('App must has pidfile_timeout.')

        # Parse args with passed from python's argument parser.
        self.parse_args(argv)
        self.app = app
        self.daemon_context = DaemonContext()

        # Use logger.
        self.daemon_context.stdin = open(app.stdin_path, 'rt')
        self.daemon_context.stdout = open(app.stdout_path, 'a+')
        self.daemon_context.stderr = open(app.stderr_path, 'a+')

        if app.pidfile_path is not None:
            self.pidfile = make_pidlockfile(app.pidfile_path, app.pidfile_timeout)
        self.daemon_context.pidfile = self.pidfile

    def parse_args(self, argv=None):
        super(CustomDaemonRunner, self).parse_args(argv)
