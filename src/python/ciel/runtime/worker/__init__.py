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
from ciel.runtime.worker.upload_manager import UploadManager
from ciel.runtime.master.deferred_work import DeferredWorkPlugin
import ciel
from ciel.runtime.worker.master_proxy import MasterProxy
from ciel.runtime.task_executor import TaskExecutorPlugin
from ciel.runtime.block_store import BlockStore
from ciel.runtime.worker.worker_view import WorkerRoot
from ciel.runtime.worker.pinger import Pinger
from ciel.runtime.file_watcher import create_watcher_thread
from cherrypy.process import plugins
import logging
import tempfile
import cherrypy
import os
import socket
import urlparse
import simplejson
import subprocess
import sys
from threading import Lock, Condition
from datetime import datetime
from ciel.runtime.lighttpd import LighttpdAdapter
from ciel.runtime.worker.process_pool import ProcessPool
from ciel.runtime.worker.multiworker import MultiWorker
from ciel.runtime.pycurl_thread import create_pycurl_thread
from ciel.runtime.tcp_server import create_tcp_server
from ciel.runtime.worker.execution_features import ExecutionFeatures
from pkg_resources import Requirement, resource_filename
from optparse import OptionParser
import ciel.config

class WorkerState:
    pass
WORKER_STARTED = WorkerState()
WORKER_UNASSOCIATED = WorkerState()
WORKER_ASSOCIATED = WorkerState()

class Worker(plugins.SimplePlugin):
    
    def __init__(self, bus, port, options):
        plugins.SimplePlugin.__init__(self, bus)

        create_pycurl_thread(bus)
        if options.aux_port is not None:
            create_tcp_server(options.aux_port)

        self.id = None
        self.port = port
        self.master_url = options.master
        self.master_proxy = MasterProxy(self, bus, self.master_url)
        self.master_proxy.subscribe()
        
        # This will now be set by the pinger when it attempts to contact the master.
        self.hostname = None
#        if options.hostname is None:
#            self.hostname = self.master_proxy.get_public_hostname()
#        else:
#            self.hostname = options.hostname

        if options.blockstore is None:
            self.static_content_root = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/ciel-files-'))
        else:
            self.static_content_root = options.blockstore
        block_store_dir = os.path.join(self.static_content_root, "data")
        try:
            os.mkdir(block_store_dir)
        except:
            pass
        
        # The hostname is None because we get this from the master.
        self.block_store = BlockStore(None, self.port, block_store_dir, ignore_blocks=False)
        self.block_store.build_pin_set()
        self.block_store.check_local_blocks()
        create_watcher_thread(bus, self.block_store)
        self.upload_deferred_work = DeferredWorkPlugin(bus, 'upload_work')
        self.upload_deferred_work.subscribe()
        self.upload_manager = UploadManager(self.block_store, self.upload_deferred_work)
        self.execution_features = ExecutionFeatures()
        #self.task_executor = TaskExecutorPlugin(bus, self, self.master_proxy, self.execution_features, 1)
        #self.task_executor.subscribe()
        
        self.scheduling_classes = parse_scheduling_class_option(None, options.num_threads)
        
        self.multiworker = MultiWorker(ciel.engine, self)
        self.multiworker.subscribe()
        self.process_pool = ProcessPool(bus, self, self.execution_features.process_cacheing_executors)
        self.process_pool.subscribe()
        self.runnable_executors = self.execution_features.runnable_executors.keys()
        self.server_root = WorkerRoot(self)
        self.pinger = Pinger(bus, self.master_proxy, None)
        self.pinger.subscribe()
        self.stopping = False
        self.event_log = []
        self.log_lock = Lock()
        self.log_condition = Condition(self.log_lock)

        self.cherrypy_conf = {}

        cherrypy.config.update({"server.thread_pool" : 20})
        cherrypy.config.update({"checker.on" : False})

        self.subscribe()

    def subscribe(self):
        self.bus.subscribe('stop', self.stop, priority=10)
        self.bus.subscribe("worker_event", self.add_log_entry)
        
    def unsubscribe(self):
        self.bus.unsubscribe('stop', self.stop)
        self.bus.unsubscribe("worker_event", self.add_log_entry)

    def netloc(self):
        return '%s:%d' % (self.hostname, self.port)

    def as_descriptor(self):
        return {'netloc': self.netloc(), 'features': self.runnable_executors, 'has_blocks': not self.block_store.is_empty(), 'scheduling_classes': self.scheduling_classes}

    def set_hostname(self, hostname):
        self.hostname = hostname
        self.block_store.set_hostname(hostname)

    def set_master(self, master_details):
        self.master_url = master_details['master']
        self.master_proxy.change_master(self.master_url)
        self.pinger.poke()

    def start_running(self):

        app = cherrypy.tree.mount(self.server_root, "", self.cherrypy_conf)

        lighty = LighttpdAdapter(ciel.engine, self.static_content_root, self.port)
        lighty.subscribe()
        # Zap CherryPy's original flavour server
        cherrypy.server.unsubscribe()
        server = cherrypy.process.servers.FlupFCGIServer(application=app, bindAddress=lighty.socket_path)
        adapter = cherrypy.process.servers.ServerAdapter(cherrypy.engine, httpserver=server, bind_addr=lighty.socket_path)
        # Insert a FastCGI server in its place
        adapter.subscribe()

        ciel.engine.start()
        if hasattr(ciel.engine, "signal_handler"):
            ciel.engine.signal_handler.subscribe()
        if hasattr(ciel.engine, "console_control_handler"):
            ciel.engine.console_control_handler.subscribe()
        ciel.engine.block()

    def stop(self):
        with self.log_lock:
            self.stopping = True
            self.log_condition.notify_all()
    
    def submit_task(self, task_descriptor):
        ciel.engine.publish("worker_event", "Start task " + repr(task_descriptor["task_id"]))
        ciel.engine.publish('execute_task', task_descriptor)
                
    def abort_task(self, task_id):
        ciel.engine.publish("worker_event", "Abort task " + repr(task_id))
        self.task_executor.abort_task(task_id)

    def add_log_entry(self, log_string):
        with self.log_lock:
            self.event_log.append((datetime.now(), log_string))
            self.log_condition.notify_all()

    def get_log_entries(self, start_index, end_index):
        with self.log_lock:
            return self.event_log[start_index:end_index]

    def await_log_entries_after(self, index):
        with self.log_lock:
            while len(self.event_log) <= int(index):
                if self.stopping == True:
                    break
                self.log_condition.wait()
            if self.stopping:
                raise Exception("Worker stopping")

def parse_scheduling_class_option(scheduling_classes, num_threads):
    """Parse the command-line option for scheduling classes, which are formatted as:
    CLASS1,N1;CLASS2,N2;..."""
    
    # By default, we use the number of threads (-n option (default=1)).
    if scheduling_classes is None:
        scheduling_classes = '*,%d' % num_threads
        return {'*' : num_threads}
    
    ret = {}
    class_strings = scheduling_classes.split(';')
    for class_string in class_strings:
        scheduling_class, capacity_string = class_string.split(',')
        capacity = int(capacity_string)
        ret[scheduling_class] = capacity
    return ret

def worker_main(options):
    local_port = cherrypy.config.get('server.socket_port')
    w = Worker(ciel.engine, local_port, options)
    w.start_running()

def set_port(port):
    cherrypy.config.update({'server.socket_port': port})

def set_config(filename):
    cherrypy.config.update(filename)

def main(args=sys.argv):
    cherrypy.config.update({'server.socket_host': '0.0.0.0'})
    cherrypy.config.update({'log.screen': False})
    if cherrypy.config.get('server.socket_port') is None:
        cherrypy.config.update({'server.socket_port': 8001})
    
    parser = OptionParser(usage="Usage: ciel worker [options]")
    parser.add_option("-p", "--port", action="callback", callback=lambda w, x, y, z: set_port(y), default=cherrypy.config.get('server.socket_port'), type="int", help="Server port", metavar="PORT")
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="URI", default=ciel.config.get('cluster', 'master', 'http://localhost:8000'))
    parser.add_option("-b", "--blockstore", action="store", dest="blockstore", help="Path to the block store directory", metavar="PATH", default=None)
    parser.add_option("-H", "--hostname", action="store", dest="hostname", help="Hostname the master and other workers should use to contact this host", default=None)
    parser.add_option("-n", "--num-threads", action="store", dest="num_threads", help="Number of worker threads to create (for worker/all-in-one)", type="int", default=1)
    parser.add_option("-D", "--daemonise", action="store_true", dest="daemonise", help="Run as a daemon", default=False)
    parser.add_option("-o", "--logfile", action="store", dest="logfile", help="If daemonised, log to FILE", default="/dev/null", metavar="FILE")
    #parser.add_option("-C", "--scheduling-classes", action="store", dest="scheduling_classes", help="List of semicolon-delimited scheduling classes", metavar="CLASS,N;CLASS,N;...", default=None)
    parser.add_option("-P", "--auxiliary-port", action="store", dest="aux_port", type="int", help="Listen port for auxiliary TCP connections (for workers)", metavar="PORT", default=None)
    parser.add_option("-v", "--verbose", action="callback", callback=lambda w, x, y, z: ciel.set_log_level(logging.DEBUG), help="Turns on debugging output")
    parser.add_option("-i", "--pidfile", action="store", dest="pidfile", help="Record the PID of the process", default=None);
    (options, args) = parser.parse_args(args=args)

    if options.pidfile:
        cherrypy.process.plugins.PIDFile(cherrypy.engine, options.pidfile).subscribe()

    if options.daemonise:
        if options.logfile is None:
            cherrypy.config.update({'log.screen': False})
        daemon = cherrypy.process.plugins.Daemonizer(cherrypy.engine, stdout=options.logfile, stderr=options.logfile)
        cherrypy.engine.subscribe("start", daemon.start, 0)

    worker_main(options)
    
if __name__ == '__main__':
    main()
