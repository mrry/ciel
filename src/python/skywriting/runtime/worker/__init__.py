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
from skywriting.runtime.worker.upload_manager import UploadManager
from skywriting.runtime.master.deferred_work import DeferredWorkPlugin
import ciel
from skywriting.runtime.worker.master_proxy import MasterProxy
from skywriting.runtime.task_executor import TaskExecutorPlugin
from skywriting.runtime.block_store import BlockStore
from skywriting.runtime.worker.worker_view import WorkerRoot
from skywriting.runtime.executors import ExecutionFeatures
from skywriting.runtime.worker.pinger import Pinger
from skywriting.runtime.file_watcher import create_watcher_thread
from cherrypy.process import plugins
import logging
import tempfile
import cherrypy
import skywriting
import httplib2
import os
import socket
import urlparse
import simplejson
import subprocess
from threading import Lock, Condition
from datetime import datetime
from skywriting.runtime.lighttpd import LighttpdAdapter
from skywriting.runtime.worker.process_pool import ProcessPool
from skywriting.runtime.worker.multiworker import MultiWorker

class WorkerState:
    pass
WORKER_STARTED = WorkerState()
WORKER_UNASSOCIATED = WorkerState()
WORKER_ASSOCIATED = WorkerState()

class Worker(plugins.SimplePlugin):
    
    def __init__(self, bus, port, options):
        plugins.SimplePlugin.__init__(self, bus)
        self.id = None
        self.port = port
        self.master_url = options.master
        self.master_proxy = MasterProxy(self, bus, self.master_url)
        self.master_proxy.subscribe()
        if options.hostname is None:
            self.hostname = self.master_proxy.get_public_hostname()
        else:
            self.hostname = options.hostname
        self.lighty_conf_template = options.lighty_conf
        if options.blockstore is None:
            self.static_content_root = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/sw-files-'))
        else:
            self.static_content_root = options.blockstore
        block_store_dir = os.path.join(self.static_content_root, "data")
        try:
            os.mkdir(block_store_dir)
        except:
            pass
        self.block_store = BlockStore(ciel.engine, self.hostname, self.port, block_store_dir, ignore_blocks=options.ignore_blocks)
        self.block_store.subscribe()
        self.block_store.build_pin_set()
        self.block_store.check_local_blocks()
        create_watcher_thread(bus, self.block_store)
        self.upload_deferred_work = DeferredWorkPlugin(bus, 'upload_work')
        self.upload_deferred_work.subscribe()
        self.upload_manager = UploadManager(self.block_store, self.upload_deferred_work)
        self.execution_features = ExecutionFeatures()
        #self.task_executor = TaskExecutorPlugin(bus, self, self.master_proxy, self.execution_features, 1)
        #self.task_executor.subscribe()
        self.multiworker = MultiWorker(ciel.engine, self, options.num_threads)
        self.multiworker.subscribe()
        self.process_pool = ProcessPool(bus, self)
        self.process_pool.subscribe()
        self.runnable_executors = self.execution_features.runnable_executors.keys()
        self.server_root = WorkerRoot(self)
        self.pinger = Pinger(bus, self.master_proxy, None, 30)
        self.pinger.subscribe()
        self.stopping = False
        self.event_log = []
        self.log_lock = Lock()
        self.log_condition = Condition(self.log_lock)

        self.cherrypy_conf = {}

        cherrypy.config.update({"server.thread_pool" : 20})


        
        if options.staticbase is not None:
            self.cherrypy_conf["/skyweb"] = { "tools.staticdir.on": True, "tools.staticdir.dir": options.staticbase }

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
        return {'netloc': self.netloc(), 'features': self.runnable_executors, 'has_blocks': not self.block_store.is_empty()}

    def set_master(self, master_details):
        self.master_url = master_details['master']
        self.master_proxy.change_master(self.master_url)
        self.pinger.poke()

    def start_running(self):

        app = cherrypy.tree.mount(self.server_root, "", self.cherrypy_conf)

        if self.lighty_conf_template is not None:

            lighty = LighttpdAdapter(ciel.engine, self.lighty_conf_template, self.static_content_root, self.port)
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

def worker_main(options):
    local_port = cherrypy.config.get('server.socket_port')
    assert(local_port)
    
    w = Worker(ciel.engine, local_port, options)
    w.start_running()

if __name__ == '__main__':
    skywriting.main("worker")
