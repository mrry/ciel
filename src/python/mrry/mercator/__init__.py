# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
# Copyright (c) 2010 Anil Madhavapeddy <anil@recoil.org>
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

from __future__ import with_statement
from optparse import OptionParser
import socket
import cherrypy
import sys

def set_port(port):
    cherrypy.config.update({'server.socket_port': port})

def set_config(filename):
    cherrypy.config.update(filename)

def main(default_role=None):

    cherrypy.config.update({'server.socket_host': socket.getfqdn()})
    
    parser = OptionParser()
    parser.add_option("-r", "--role", action="store", dest="role", help="Server role", metavar="ROLE", default=default_role)
    parser.add_option("-p", "--port", action="callback", callback=lambda w, x, y, z: set_port(y), default=cherrypy.config.get('server.socket_port'), type="int", help="Server port", metavar="PORT")
    parser.add_option("-c", "--config", action="callback", callback=lambda w, x, y, z: set_config(y), help="Configuration file", metavar="FILE")
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="URI", default=None)
    parser.add_option("-w", "--workerlist", action="store", dest="workerlist", help="List of workers", metavar = "FILE", default=None)
    (options, _) = parser.parse_args()
   
    if options.role == 'master':
        from mrry.mercator.runtime.master import master_main
        master_main(options)
    elif options.role == 'worker':
        from mrry.mercator.runtime.worker import worker_main
        if not cherrypy.config.get('server.socket_port'):
            parser.print_help()
            print >> sys.stderr, "Must specify port for worker with --port\n"
            sys.exit(1)
        worker_main(options)
    elif options.role == 'interactive':
        from mrry.mercator.runtime.interactive import interactive_main
        interactive_main(options)
    else:
        raise
    
if __name__ == '__main__':
    main()
