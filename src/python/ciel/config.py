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

import ConfigParser
import os
import sys
import optparse

# CIEL will consult a configuration file stored as '~/.ciel'
# Attempts to set the config will update such a file.
CIEL_USER_CONFIG = os.path.expanduser('~/.ciel')
CIEL_GLOBAL_CONFIG = '/etc/ciel.conf'

_cp = ConfigParser.SafeConfigParser()
_cp.read([CIEL_USER_CONFIG, CIEL_GLOBAL_CONFIG])

def get(section, key, default=None):
    try:
        return _cp.get(section, key)
    except:
        return default

def set(section, key, value):
    try:
        _cp.set(section, key, value)
    except ConfigParser.NoSectionError:
        _cp.add_section(section)
        _cp.set(section, key, value)
    return

def write():
    with open(os.path.expanduser('~/.ciel'), 'w') as f:
        _cp.write(f)

def main(my_args=sys.argv):

    def cli_get(option, opt_str, value, *args, **kwargs):
        section, key = value.split('.')
        print get(section, key)

    def cli_set(option, opt_str, value, *args, **kwargs):
        section_key, value = value
        section, key = section_key.split('.')
        set(section, key, value)
        write()

    parser = optparse.OptionParser(usage='Usage: ciel config [options]')
    parser.add_option("-g", "--get", action="callback", nargs=1, callback=cli_get, help="Display the value of a configuration option", metavar="SECTION.KEY", type="str")
    parser.add_option("-s", "--set", action="callback", nargs=2, callback=cli_set, help="Update the value of a configuration option (saved in %s)" % CIEL_USER_CONFIG, metavar="SECTION.KEY VALUE", type="str")

    if len(my_args) < 2:
        parser.print_help()
        sys.exit(-1)

    (options, args) = parser.parse_args(args=my_args)
