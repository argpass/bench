from __future__ import unicode_literals
import six

from collections import defaultdict
from importlib import import_module
import os
import pkgutil
import sys

from pending.core.exceptions import ImproperlyConfigured
from .base import BaseCommand, CommandError, CommandParser
from .color import color_style
from pending.config import settings


def find_commands(management_dir):
    """
    Given a path to a management directory, returns a list of all the command
    names that are available.

    Returns an empty list if no commands are defined.
    """
    command_dir = os.path.join(management_dir, 'commands')
    # Workaround for a Python 3.2 bug with pkgutil.iter_modules
    sys.path_importer_cache.pop(command_dir, None)
    return [name for _, name, is_pkg in pkgutil.iter_modules([command_dir])
            if not is_pkg and not name.startswith('_')]


def load_command_class(app_name, name):
    """
    Given a command name and an application name, returns the Command
    class instance. All errors raised by the import process
    (ImportError, AttributeError) are allowed to propagate.
    """
    module = import_module('%s.management.commands.%s' % (app_name, name))
    return module.Command()


def get_commands():
    """
    Returns a dictionary mapping command names to their callback applications.
    """
    commands = {}
    for app in settings.INSTALLED_APPS:
        try:
            app_module = import_module(app)
            path = os.path.join(app_module.__path__[0], 'management')
            commands.update({
                name: app_module.__name__ for name in find_commands(path)
            })
        except ImportError:
            raise ImproperlyConfigured('Fail to import app:%s' % repr(app))

    return commands


class ManagementUtility(object):
    """
    Encapsulates the logic of the django-admin and manage.py utilities.

    A ManagementUtility has a number of commands, which can be manipulated
    by editing the self.commands dictionary.
    """
    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        self.settings_exception = None

    def main_help_text(self, commands_only=False):
        """
        Returns the script's main help text, as a string.
        """
        if commands_only:
            usage = sorted(get_commands().keys())
        else:
            usage = [
                "",
                "Type '%s help <subcommand>' "
                "for help on a specific subcommand." % self.prog_name,
                "",
                "Available subcommands:",
            ]
            commands_dict = defaultdict(lambda: [])
            for name, app in six.iteritems(get_commands()):
                if app == 'django.core':
                    app = 'django'
                else:
                    app = app.rpartition('.')[-1]
                commands_dict[app].append(name)
            style = color_style()
            for app in sorted(commands_dict.keys()):
                usage.append("")
                usage.append(style.NOTICE("[%s]" % app))
                for name in sorted(commands_dict[app]):
                    usage.append("    %s" % name)
            # Output an extra note if settings are not properly configured
            if self.settings_exception is not None:
                usage.append(style.NOTICE(
                    "Note that only Django core commands are listed "
                    "as settings are not properly configured (error: %s)."
                    % self.settings_exception))

        return '\n'.join(usage)

    def fetch_command(self, subcommand):
        """
        Tries to fetch the given subcommand, printing a message with the
        appropriate command called from the command line (usually
        "django-admin" or "manage.py") if it can't be found.
        """
        # Get commands outside of try block to prevent swallowing exceptions
        commands = get_commands()
        try:
            app_name = commands[subcommand]
        except KeyError:
            sys.stderr.write("Unknown command: %r\nType '%s help' for usage.\n" %
                             (subcommand, self.prog_name))
            sys.exit(1)
        klass = load_command_class(app_name, subcommand)
        return klass

    def execute(self):
        """
        Given the command-line arguments, this figures out which sub_command is
        being run, creates a parser appropriate to that command, and runs it.
        """
        try:
            sub_command = self.argv[1]
        except IndexError:
            sub_command = 'help'  # Display help if no arguments were given.

        # Preprocess options to extract --settings and --pythonpath.
        # These options could affect the commands that are available, so they
        # must be processed early.
        parser = CommandParser(
            None, usage="%(prog)s subcommand [options] [args]", add_help=False)
        parser.add_argument('--settings')
        parser.add_argument('--pythonpath')
        parser.add_argument('args', nargs='*')  # catch-all
        try:
            options, args = parser.parse_known_args(self.argv[2:])
        except CommandError:
            pass  # Ignore any option errors at this point.

        else:
            try:
                settings.INSTALLED_APPS
            except ImproperlyConfigured as exc:
                self.settings_exception = exc
                # A handful of built-in management
                # commands work without settings.
                # Load the default settings -- where INSTALLED_APPS is empty.

            if sub_command == 'help':
                if '--commands' in args:
                    sys.stdout.write(
                        self.main_help_text(commands_only=True) + '\n')
                elif len(options.args) < 1:
                    sys.stdout.write(self.main_help_text() + '\n')
                else:
                    self.fetch_command(options.args[0]).print_help(
                        self.prog_name, options.args[0])
            elif self.argv[1:] in (['--help'], ['-h']):
                sys.stdout.write(self.main_help_text() + '\n')
            else:
                self.fetch_command(sub_command).run_from_argv(self.argv)


def execute_from_command_line(argv=None):
    """
    A simple method that runs a ManagementUtility.
    """
    utility = ManagementUtility(argv)
    utility.execute()
