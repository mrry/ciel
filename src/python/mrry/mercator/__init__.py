from __future__ import with_statement
from optparse import OptionParser
import socket
import cherrypy


def set_port(port):
    cherrypy.config.update({'server.socket_port': port})

def set_config(filename):
    cherrypy.config.update(filename)

def main(default_role=None):

    cherrypy.config.update({'server.socket_host': socket.getfqdn()})
    
    parser = OptionParser()
    parser.add_option("-r", "--role", action="store", dest="role", help="Server role", metavar="ROLE", default=default_role)
    parser.add_option("-p", "--port", action="callback", callback=lambda w, x, y, z: set_port(y), type="int", help="Server port", metavar="PORT")
    parser.add_option("-c", "--config", action="callback", callback=lambda w, x, y, z: set_config(y), help="Configuration file", metavar="FILE")
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="URI", default=None)
    parser.add_option("-w", "--workerlist", action="store", dest="workerlist", help="List of workers", metavar = "FILE", default=None)
    (options, _) = parser.parse_args()

    if options.role == 'master':
        from mrry.mercator.runtime.master import master_main
        master_main(options)
    elif options.role == 'worker':
        from mrry.mercator.runtime.worker import worker_main
        worker_main(options)
    else:
        raise
    
if __name__ == '__main__':
    main()
