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
from skywriting.runtime.block_store import BlockStore
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

def master_main(options):

    create_pycurl_thread(ciel.engine)

    deferred_worker = DeferredWorkPlugin(ciel.engine)
    deferred_worker.subscribe()

    worker_pool = WorkerPool(ciel.engine, deferred_worker, None)
    worker_pool.subscribe()

    task_failure_investigator = TaskFailureInvestigator(worker_pool, deferred_worker)
    
    job_pool = JobPool(ciel.engine, options.journaldir, None, task_failure_investigator, deferred_worker, worker_pool)
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
        static_content_root = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/sw-files-'))
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

    if options.master is not None:
        monitor = MasterRecoveryMonitor(cherrypy.engine, 'http://%s/' % master_netloc, options.master, job_pool)
        monitor.subscribe()
    else:
        monitor = None

    recovery_manager = RecoveryManager(ciel.engine, job_pool, block_store, deferred_worker)
    recovery_manager.subscribe()
  
    root = MasterRoot(worker_pool, block_store, job_pool, backup_sender, monitor)

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
                    post_string(urllib2.urlparse.urljoin(worker_url, 'control/master/'), master_details_as_json)
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
