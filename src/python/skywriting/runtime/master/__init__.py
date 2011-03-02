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
from skywriting.runtime.master.lazy_task_pool import LazyTaskPool,\
    LazyTaskPoolAdapter
from skywriting.runtime.master.lazy_scheduler import LazyScheduler
from skywriting.runtime.master.deferred_work import DeferredWorkPlugin
from skywriting.runtime.master.master_view import MasterRoot
from skywriting.runtime.master.worker_pool import WorkerPool
from skywriting.runtime.block_store import BlockStore
from skywriting.runtime.task_executor import TaskExecutorPlugin
import skywriting
import simplejson
import logging
import urllib2
import urllib
import httplib2
import tempfile
import socket
import cherrypy
import subprocess
from skywriting.runtime.master.job_pool import JobPool
import os
import ciel
from skywriting.runtime.lighttpd import LighttpdAdapter
from skywriting.runtime.master.recovery import RecoveryManager,\
    TaskFailureInvestigator
from skywriting.runtime.master.hot_standby import BackupSender,\
    MasterRecoveryMonitor

def master_main(options):

    deferred_worker = DeferredWorkPlugin(ciel.engine)
    deferred_worker.subscribe()

    worker_pool = WorkerPool(ciel.engine, deferred_worker)
    worker_pool.subscribe()

    lazy_task_pool = LazyTaskPool(ciel.engine, worker_pool)
    task_pool_adapter = LazyTaskPoolAdapter(lazy_task_pool)
    lazy_task_pool.subscribe()
    
    job_pool = JobPool(ciel.engine, lazy_task_pool, options.journaldir)
    job_pool.subscribe()

    task_failure_investigator = TaskFailureInvestigator(lazy_task_pool, worker_pool, deferred_worker)
    
    backup_sender = BackupSender(cherrypy.engine)
    backup_sender.subscribe()

    local_hostname = socket.getfqdn()
    local_port = cherrypy.config.get('server.socket_port')
    master_netloc = '%s:%d' % (local_hostname, local_port)
    print 'Local port is', local_port
    
    if options.blockstore is None:
        static_content_root = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/sw-files-'))
    else:
        static_content_root = options.blockstore
    block_store_dir = os.path.join(static_content_root, "data")
    try:
        os.mkdir(block_store_dir)
    except:
        pass

    block_store = BlockStore(ciel.engine, local_hostname, local_port, block_store_dir)
    block_store.subscribe()
    block_store.build_pin_set()
    block_store.check_local_blocks()

    if options.master is not None:
        monitor = MasterRecoveryMonitor(cherrypy.engine, 'http://%s/' % master_netloc, options.master, job_pool)
        monitor.subscribe()
    else:
        monitor = None

    recovery_manager = RecoveryManager(ciel.engine, job_pool, lazy_task_pool, block_store, deferred_worker)
    recovery_manager.subscribe()

    scheduler = LazyScheduler(ciel.engine, lazy_task_pool, worker_pool)
    scheduler.subscribe()
    
    root = MasterRoot(task_pool_adapter, worker_pool, block_store, job_pool, backup_sender, monitor, task_failure_investigator)

    cherrypy.config.update({"server.thread_pool" : 50})

    cherrypy_conf = dict()
    
    if options.staticbase is not None:
        cherrypy_conf["/skyweb"] = { "tools.staticdir.on": True, "tools.staticdir.dir": options.staticbase }

    app = cherrypy.tree.mount(root, "", cherrypy_conf)
    lighty_conf_template = options.lighty_conf
    if lighty_conf_template is not None:
        lighty = LighttpdAdapter(ciel.engine, lighty_conf_template, static_content_root, local_port)
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
    
    if options.workerlist is not None:
        master_details = {'netloc': master_netloc}
        master_details_as_json = simplejson.dumps(master_details)
        with (open(options.workerlist, "r")) as f:
            for worker_url in f.readlines():
                try:
                    http = httplib2.Http()
                    http.request(urllib2.urlparse.urljoin(worker_url, 'control/master/'), "POST", master_details_as_json)
                    # Worker will be created by a callback.
                except:
                    ciel.log.error("Error adding worker: %s" % (worker_url, ), "WORKER", logging.WARNING)
                    
    ciel.engine.block()

#    sch = SchedulerProxy(ciel.engine)
#    sch.subscribe()
#
#    reaper = WorkerReaper(ciel.engine)
#    reaper.subscribe()
#
#    wr = WorkflowRunner(ciel.engine)
#    wr.subscribe()
#
#    te = TaskExecutor(ciel.engine)
#    te.subscribe()
#
#    ph = PingHandler(ciel.engine)
#    ph.subscribe()

if __name__ == '__main__':
    skywriting.main("master")
