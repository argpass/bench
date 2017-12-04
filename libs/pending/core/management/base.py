# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import sys
from argparse import ArgumentParser
from .color import no_style, color_style


class CommandError(Exception):
    pass


class CommandParser(ArgumentParser):

    def __init__(self, cmd, **kwargs):
        self.cmd = cmd
        super(CommandParser, self).__init__(**kwargs)

    def parse_args(self, args=None, namespace=None):
        # Catch missing argument for a better error message
        if (hasattr(self.cmd, 'missing_args_message') and
                not (args or any(not arg.startswith('-') for arg in args))):
            self.error(self.cmd.missing_args_message)
        return super(CommandParser, self).parse_args(args, namespace)

    def error(self, message):
        if getattr(self.cmd, "_called_from_command_line", None):
            super(CommandParser, self).error(message)
        else:
            raise CommandError("Error: %s" % message)


class OutputWrapper(object):
    """
    Wrapper around stdout/stderr
    """
    @property
    def style_func(self):
        return self._style_func

    @style_func.setter
    def style_func(self, style_func):
        if style_func and hasattr(self._out, 'isatty') and self._out.isatty():
            self._style_func = style_func
        else:
            self._style_func = lambda x: x

    def __init__(self, out, style_func=None, ending='\n'):
        self._out = out
        self._style_func = None
        self.style_func = style_func
        self.ending = ending

    def __getattr__(self, name):
        return getattr(self._out, name)

    def write(self, msg, style_func=None, ending=None):
        ending = self.ending if ending is None else ending
        if ending and not msg.endswith(ending):
            msg += ending
        style_func = style_func or self.style_func
        self._out.write(style_func(msg))


class BaseCommand(object):
    help = ''
    args = ''
    _called_from_command_line = False

    def __init__(self, stdout=None, stderr=None, no_color=False):
        self.stdout = OutputWrapper(stdout or sys.stdout)
        self.stderr = OutputWrapper(stderr or sys.stderr)
        if no_color:
            self.style = no_style()
        else:
            self.style = color_style()
            self.stderr.style_func = self.style.ERROR

    def usage(self, subcommand):
        """
        Return a brief description of how to use this command, by
        default from the attribute ``self.help``.

        """
        usage = '%%prog %s [options] %s' % (subcommand, self.args)
        if self.help:
            return '%s\n\n%s' % (usage, self.help)
        else:
            return usage

    def create_parser(self, prog_name, subcommand):
        """
        Create and return the ``ArgumentParser`` which will be used to
        parse the arguments to this command.

        """
        parser = CommandParser(
            self, prog="%s %s" % (os.path.basename(prog_name), subcommand),
            description=self.help or None)
        if self.args:
            parser.add_argument('args', nargs='*')
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """
        Entry point for subclassed commands to add custom arguments.
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def print_help(self, prog_name, subcommand):
        """
        Print the help message for this command, derived from
        ``self.usage()``.

        """
        parser = self.create_parser(prog_name, subcommand)
        parser.print_help()

    def run_from_argv(self, argv):
        self._called_from_command_line = True
        parser = self.create_parser(argv[0], argv[1])

        options = parser.parse_args(argv[2:])
        cmd_options = vars(options)
        args = cmd_options.pop('args', ())
        self.execute(*args, **cmd_options)
        # try:
        #     self.execute(*args, **cmd_options)
        # except Exception as e:
        #     if not isinstance(e, CommandError):
        #         raise e
        #     self.stderr.write('%s: %s' % (e.__class__.__name__, e))
        #     sys.exit(1)

    def execute(self, *args, **options):
        output = self.handle(*args, **options)
        if output:
            self.stdout.write(output)

    def handle(self, *args, **options):
        """
        The actual logic of the command. Subclasses must implement
        this method.

        """
        raise NotImplementedError(u"Implement the method by subclass")


