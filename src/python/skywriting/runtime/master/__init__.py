# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from skywriting.runtime.block_store import BlockStore, get_own_netloc
from skywriting.runtime.lighttpd import LighttpdAdapter
from skywriting.runtime.master.deferred_work import DeferredWorkPlugin
from skywriting.runtime.master.hot_standby import BackupSender, \
    MasterRecoveryMonitor
from skywriting.runtime.master.job_pool import JobPool
from skywriting.runtime.master.master_view import MasterRoot
from skywriting.runtime.master.recovery import RecoveryManager, \
    TaskFailureInvestigator
from skywriting.runtime.master.worker_pool import WorkerPool
from skywriting.runtime.task_executor import TaskExecutorPlugin
from skywriting.runtime.pycurl_rpc import post_string
from skywriting.runtime.pycurl_thread import create_pycurl_thread
import cherrypy
import ciel
import logging
import os
import simplejson
import skywriting
import socket
import subprocess
import tempfile
import urllib
import urllib2
from pkg_resources import Requirement, resource_filename
import sys
from optparse import OptionParser

def master_main(options):

    create_pycurl_thread(ciel.engine)

    deferred_worker = DeferredWorkPlugin(ciel.engine)
    deferred_worker.subscribe()

    worker_pool = WorkerPool(ciel.engine, deferred_worker, None)
    worker_pool.subscribe()

    task_failure_investigator = TaskFailureInvestigator(worker_pool, deferred_worker)
    
    job_pool = JobPool(ciel.engine, options.journaldir, None, task_failure_investigator, deferred_worker, worker_pool, options.task_log_root)
    job_pool.subscribe()
    
    worker_pool.job_pool = job_pool

    backup_sender = BackupSender(cherrypy.engine)
    backup_sender.subscribe()

    if options.hostname is not None:
        local_hostname = options.hostname
    else:
        local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    master_netloc = '%s:%d' % (local_hostname, local_port)
    ciel.log('Local port is %d' % local_port, 'STARTUP', logging.INFO)
    
    if options.blockstore is None:
        static_content_root = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/ciel-files-'))
        ciel.log('Block store directory not specified: storing in %s' % static_content_root, 'STARTUP', logging.WARN)
    else:
        static_content_root = options.blockstore
    block_store_dir = os.path.join(static_content_root, "data")
    try:
        os.mkdir(block_store_dir)
    except:
        pass

    block_store = BlockStore(local_hostname, local_port, block_store_dir)
    block_store.build_pin_set()
    block_store.check_local_blocks()

    # TODO: Re-enable this and test thoroughly.
    #if options.master is not None:
    #    monitor = MasterRecoveryMonitor(cherrypy.engine, 'http://%s/' % master_netloc, options.master, job_pool)
    #    monitor.subscribe()
    #else:
    monitor = None

    recovery_manager = RecoveryManager(ciel.engine, job_pool, block_store, deferred_worker)
    recovery_manager.subscribe()
  
    root = MasterRoot(worker_pool, block_store, job_pool, backup_sender, monitor)

    cherrypy.config.update({"server.thread_pool" : 50})
    cherrypy.config.update({"checker.on" : False})

    cherrypy_conf = dict()
    
    app = cherrypy.tree.mount(root, "", cherrypy_conf)
    
    lighty = LighttpdAdapter(ciel.engine, static_content_root, local_port)
    lighty.subscribe()
    # Zap CherryPy's original flavour server
    cherrypy.server.unsubscribe()
    server = cherrypy.process.servers.FlupFCGIServer(application=app, bindAddress=lighty.socket_path)
    adapter = cherrypy.process.servers.ServerAdapter(cherrypy.engine, httpserver=server, bind_addr=lighty.socket_path)
    # Insert a FastCGI server in its place
    adapter.subscribe()

    if hasattr(ciel.engine, "signal_handler"):
        ciel.engine.signal_handler.subscribe()
    if hasattr(ciel.engine, "console_control_handler"):
        ciel.engine.console_control_handler.subscribe()

    ciel.engine.start()
    ciel.engine.block()

def set_port(port):
    cherrypy.config.update({'server.socket_port': port})

def set_config(filename):
    cherrypy.config.update(filename)

def main():

    cherrypy.config.update({'server.socket_host': '0.0.0.0'})
    
    cherrypy.config.update({'log.screen': False})
    
    if cherrypy.config.get('server.socket_port') is None:
        cherrypy.config.update({'server.socket_port': 8000})
    
    parser = OptionParser(usage="Usage: ciel master [options]")
    parser.add_option("-p", "--port", action="callback", callback=lambda w, x, y, z: set_port(y), default=cherrypy.config.get('server.socket_port'), type="int", help="Server port", metavar="PORT")
    parser.add_option("-b", "--blockstore", action="store", dest="blockstore", help="Path to the block store directory", metavar="PATH", default=None)
    parser.add_option("-j", "--journaldir", action="store", dest="journaldir", help="Path to the job journal directory", metavar="PATH", default=None)
    parser.add_option("-H", "--hostname", action="store", dest="hostname", help="Hostname the master and other workers should use to contact this host", default=None)
    parser.add_option("-D", "--daemonise", action="store_true", dest="daemonise", help="Run as a daemon", default=False)
    parser.add_option("-o", "--logfile", action="store", dest="logfile", help="If daemonised, log to FILE", default="/dev/null", metavar="FILE")
    parser.add_option("-v", "--verbose", action="callback", callback=lambda w, x, y, z: ciel.set_log_level(logging.DEBUG), help="Turns on debugging output")
    parser.add_option("-6", "--task-log-root", action="store", dest="task_log_root", help="Path to store task state log", metavar="PATH", default=None)
    parser.add_option("-i", "--pidfile", action="store", dest="pidfile", help="Record the PID of the process", default=None);
    (options, args) = parser.parse_args()

    if options.pidfile:
        cherrypy.process.plugins.PIDFile(cherrypy.engine, options.pidfile).subscribe()

    if options.daemonise:
        if options.logfile is None:
            cherrypy.config.update({'log.screen': False})
        daemon = cherrypy.process.plugins.Daemonizer(cherrypy.engine, stdout=options.logfile, stderr=options.logfile)
        cherrypy.engine.subscribe("start", daemon.start, 0)

    master_main(options)
    
if __name__ == '__main__':
    main()
