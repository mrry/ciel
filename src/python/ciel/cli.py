# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
import ciel.runtime.master
import ciel.runtime.util.start_job
import ciel.runtime.util.run_script
import ciel.config
from ciel import CIEL_VERSION_STRING
import sys

def start_master(args):
    ciel.runtime.master.main(args)

def start_worker(args):
    ciel.runtime.worker.main(args)

def run_job(args):
    ciel.runtime.util.start_job.main(args)

def run_jar(args):
    ciel.runtime.util.start_job.jar(args)

def run_sw(args):
    ciel.runtime.util.run_script.main(args)

def config(args):
    ciel.config.main(args)

def show_help(args):
    print >>sys.stderr, "usage: ciel COMMAND [ARGS]"
    print >>sys.stderr
    print >>sys.stderr, "The main Ciel commands are:"
    for command, _, description in default_command_list:
        if description is not None:
            print >>sys.stderr, '   %s %s' % (command.ljust(10), description)

def version():
    print >>sys.stderr, CIEL_VERSION_STRING

default_command_list = [('master',    start_master, "Start running a CIEL master"),
                        ('worker',    start_worker, "Start running a CIEL worker"),
                        ('run',       run_job,      "Run a CIEL job"),
                        ('java',      run_jar,      "Run a Java-based CIEL job"),
                        ('sw',        run_sw,       "Run a Skywriting script"),
                        ('config',    config,       "Configure CIEL"),
                        ('help',      show_help,    "Display this message"),
                        ('--version', version,      None)]

default_command_map = dict([(x, (y, z)) for x, y, z in default_command_list])

def main():

    my_args = sys.argv[:]

    if len(my_args) < 2:
        func = show_help
        exit_code = -1
    else:

        command = my_args.pop(1)
        try:
            func, _ = default_command_map[command]
            exit_code = 0
        except KeyError:
            print >>sys.stderr, 'Unrecognised command: %s' % command
            func = show_help
            exit_code = -1

    func(my_args)
    sys.exit(exit_code)
    

if __name__ == '__main__':
    main()
