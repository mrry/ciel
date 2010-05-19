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

'''
Created on 4 Feb 2010

@author: dgm36
'''
from skywriting.runtime.worker.master_proxy import MasterProxy
from skywriting.runtime.task_executor import TaskExecutorPlugin
from skywriting.runtime.block_store import BlockStore
from skywriting.runtime.worker.worker_view import WorkerRoot
from skywriting.runtime.executors import ExecutionFeatures
from skywriting.runtime.worker.pinger import Pinger
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

class Worker(plugins.SimplePlugin):
    
    def __init__(self, bus, hostname, port, master_url):
        plugins.SimplePlugin.__init__(self, bus)
        self.hostname = hostname
        self.port = port
        self.master_url = master_url
        self.master_proxy = MasterProxy(self, master_url)
        temp_dir = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/sw-files-'))
        self.block_store = BlockStore(self.hostname, self.port, temp_dir, self.master_proxy)
        self.execution_features = ExecutionFeatures()
        self.task_executor = TaskExecutorPlugin(bus, self.block_store, self.master_proxy, self.execution_features, 1)
        self.task_executor.subscribe()
        self.server_root = WorkerRoot(self)
        self.pinger = Pinger(cherrypy.engine, self.master_proxy, None, 30)
        self.pinger.subscribe()    
        
    def subscribe(self):
        self.bus.subscribe('stop', self.stop)
        
    def unsubscribe(self):
        self.bus.unsubscribe('stop', self.stop)
        
    def netloc(self):
        return '%s:%d' % (self.hostname, self.port)

    def as_descriptor(self):
        return {'netloc': self.netloc(), 'features': self.execution_features.all_features()}

    def set_master(self, master_details):
        self.master_url = master_details['master']
        self.master_proxy.change_master(self.master_url)
        self.master_proxy.register_as_worker()

    def start_running(self):
        cherrypy.engine.start()
        cherrypy.tree.mount(self.server_root, "", None)
        if hasattr(cherrypy.engine, "signal_handler"):
            cherrypy.engine.signal_handler.subscribe()
        if hasattr(cherrypy.engine, "console_control_handler"):
            cherrypy.engine.console_control_handler.subscribe()

        try:
            self.master_proxy.register_as_worker()
        except:
            cherrypy.log.error("Error registering with master: %s" % (self.master_url, ), 'WORKER', logging.WARNING, True)
            pass
        
        cherrypy.engine.block()

    def stop(self):
        pass

def worker_main(options):
    local_hostname = cherrypy.config.get('server.socket_host')
    local_port = cherrypy.config.get('server.socket_port')
    assert(local_port)
    
    w = Worker(cherrypy.engine, local_hostname, local_port, options.master)
    w.start_running()

if __name__ == '__main__':
    skywriting.main("worker")
