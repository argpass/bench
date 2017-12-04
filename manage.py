#!coding: utf-8
import os
import sys
import site

TOP_DIR_ = os.path.dirname(os.path.abspath(__file__))
site.addsitedir(os.path.join(TOP_DIR_, "./libs"))


if __name__ == "__main__":
    from pending.framework import setup
    setup()
    from pending.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)
