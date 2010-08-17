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
Created on 11 Feb 2010

@author: dgm36
'''
from __future__ import with_statement
from skywriting.runtime.master.deferred_work import DeferredWorkPlugin
from skywriting.runtime.master.master_view import MasterRoot
from skywriting.runtime.master.data_store import GlobalNameDirectory
from skywriting.runtime.master.worker_pool import WorkerPool
from skywriting.runtime.block_store import BlockStore
from skywriting.runtime.task_executor import TaskExecutorPlugin
from skywriting.runtime.master.local_master_proxy import LocalMasterProxy
from skywriting.runtime.master.task_pool import TaskPool
import skywriting
from skywriting.runtime.master.scheduler import Scheduler
import simplejson
import logging
import urllib2
import urllib
import httplib2
import tempfile
import socket
import cherrypy

def master_main(options):

    deferred_worker = DeferredWorkPlugin(cherrypy.engine)
    deferred_worker.subscribe()

    global_name_directory = GlobalNameDirectory(cherrypy.engine)
    global_name_directory.subscribe()

    worker_pool = WorkerPool(cherrypy.engine, deferred_worker)
    worker_pool.subscribe()

    task_pool = TaskPool(cherrypy.engine, global_name_directory, worker_pool, deferred_worker)
    task_pool.subscribe()
    

    local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    print 'Local port is', local_port
    master_proxy = LocalMasterProxy(task_pool, None, global_name_directory, worker_pool)
    block_store = BlockStore(local_hostname, local_port, tempfile.mkdtemp(), master_proxy)
    master_proxy.block_store = block_store

    scheduler = Scheduler(cherrypy.engine, task_pool, worker_pool, deferred_worker)
    scheduler.subscribe()
    
    root = MasterRoot(task_pool, worker_pool, block_store, global_name_directory)

    cherrypy.config.update({"server.thread_pool" : 50})

    cherrypy_conf = dict()
    
    if options.staticbase is not None:
        cherrypy_conf["/skyweb"] = { "tools.staticdir.on": True, "tools.staticdir.dir": options.staticbase }

    cherrypy.tree.mount(root, "", cherrypy_conf)
    
    if hasattr(cherrypy.engine, "signal_handler"):
        cherrypy.engine.signal_handler.subscribe()
    if hasattr(cherrypy.engine, "console_control_handler"):
        cherrypy.engine.console_control_handler.subscribe()

    cherrypy.engine.start()
    
    
    
    if options.workerlist is not None:
        master_details = {'netloc': '%s:%d' % (local_hostname, local_port)}
        master_details_as_json = simplejson.dumps(master_details)
        with (open(options.workerlist, "r")) as f:
            for worker_url in f.readlines():
                try:
                    http = httplib2.Http()
                    http.request(urllib2.urlparse.urljoin(worker_url, '/master/'), "POST", master_details_as_json)
                    # Worker will be created by a callback.
                except:
                    cherrypy.log.error("Error adding worker: %s" % (worker_url, ), "WORKER", logging.WARNING)
                    
    cherrypy.engine.block()

#    sch = SchedulerProxy(cherrypy.engine)
#    sch.subscribe()
#
#    reaper = WorkerReaper(cherrypy.engine)
#    reaper.subscribe()
#
#    wr = WorkflowRunner(cherrypy.engine)
#    wr.subscribe()
#
#    te = TaskExecutor(cherrypy.engine)
#    te.subscribe()
#
#    ph = PingHandler(cherrypy.engine)
#    ph.subscribe()

if __name__ == '__main__':
    skywriting.main("master")
