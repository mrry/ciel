# Copyright (c) 2011 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from urlparse import urljoin
import httplib2
import logging
import simplejson

from cherrypy.process import plugins
from Queue import Queue, Empty
from skywriting.runtime.plugins import THREAD_TERMINATOR
import threading
import ciel
    
class PingerPoker:
    pass
PINGER_POKER = PingerPoker()
    
class MasterRecoveryMonitor(plugins.SimplePlugin):
    '''
    The pinger maintains the connection between a master and a backup master.
    It operates as follows:
    
    1. The backup master registers with the primary master to receive journal
       events.
       
    2. The backup periodically pings the master to find out if it has failed.
    
    3. When the primary is deemed to have failed, the backup instructs the
       workers to re-register with it.

    4. The backup takes over at this point.
    '''
    
    def __init__(self, bus, my_url, master_url, job_pool, ping_timeout=5):
        plugins.SimplePlugin.__init__(self, bus)
        self.queue = Queue()
        self.non_urgent_queue = Queue()
        
        self.is_primary = False
        self.is_connected = False
        self.is_running = False
        
        self.thread = None
        self.my_url = my_url
        self.master_url = master_url
        self.ping_timeout = ping_timeout
        
        self.job_pool = job_pool
        
        self._lock = threading.Lock()
        self.workers = set()
                
    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop)
        self.bus.subscribe('poke', self.poke)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        self.bus.unsubscribe('poke', self.poke)
                
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self.thread_main, args=())
            self.thread.start()
    
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.queue.put(THREAD_TERMINATOR)
            self.thread.join()
            self.thread = None
        
    def poke(self):
        self.queue.put(PINGER_POKER)
        
    def register_as_backup(self):
        h = httplib2.Http()
        h.request(urljoin(self.master_url, '/backup/'), 'POST', simplejson.dumps(self.my_url))
    
    def ping_master(self):
        h = httplib2.Http()
        try:
            response, _ = h.request(self.master_url, 'GET')
        except:
            ciel.log('Error contacting primary master', 'MONITOR', logging.WARN, True)
            return False
        if response['status'] != '200':
            ciel.log('Got unusual status from primary master: %s' % response['status'], 'MONITOR', logging.WARN)
            return False
        return True
    
    def add_worker(self, worker):
        with self._lock:
            self.workers.add(worker)
    
    def notify_all_workers(self):
        master_details = simplejson.dumps({'master' : self.my_url})
        with self._lock:
            for netloc in self.workers:
                h = httplib2.Http()
                ciel.log('Notifying worker: %s' % netloc, 'MONITOR', logging.INFO)
                try:
                    response, _ = h.request('http://%s/master/' % netloc, 'POST', master_details)
                    if response['status'] != '200':
                        ciel.log('Error %s when notifying worker of new master: %s' % (response['status'], netloc), 'MONITOR', logging.WARN, True)
                except:
                    ciel.log('Error notifying worker of new master: %s' % netloc, 'MONITOR', logging.WARN, True)
    
    def thread_main(self):
        
        # While not connected, attempt to register as a backup master.
        while not self.is_connected:
        
            try:    
                self.register_as_backup()
                ciel.log('Registered as backup master for %s' % self.master_url, 'MONITOR', logging.INFO)
                self.is_connected = True
                break
            except:
                ciel.log('Unable to register with master', 'MONITOR', logging.WARN, True)
                pass
        
            try:
                maybe_terminator = self.queue.get(block=True, timeout=self.ping_timeout)
                if not self.is_running or maybe_terminator is THREAD_TERMINATOR:
                    return
            except Empty:
                pass
        
            
        # While connected, periodically ping the master.
        while self.is_connected:
            
            try:
                new_thing = self.queue.get(block=True, timeout=self.ping_timeout)
                if not self.is_connected or not self.is_running or new_thing is THREAD_TERMINATOR:
                    return
            except Empty:
                pass
            
            try:
                if not self.ping_master():
                    self.is_connected = False
            except:
                self.is_connected = False
            
        if not self.is_running:
            return
        
        # This master is now the primary.
        self.is_primary = True
        
        self.job_pool.start_all_jobs()
        self.notify_all_workers()
            
class BackupSender:
    
    def __init__(self, bus):
        self.bus = bus
        self.standby_urls = set()
        self.queue = Queue()
        
        self.is_running = False
        self.is_logging = False
    
    def register_standby_url(self, url):
        self.is_logging = True
        self.queue.put(('U', url))

    def publish_refs(self, task_id, ref):
        if not self.is_logging:
            return
        self.queue.put(('P', task_id, ref))
        
    def add_worker(self, worker_netloc):
        if not self.is_logging:
            return
        self.queue.put(('W', worker_netloc))
        
    def add_job(self, id, root_task_descriptor):
        if not self.is_logging:
            return
        self.queue.put(('J', id, root_task_descriptor))
        
    def add_data(self, id, data):
        if not self.is_logging:
            return
        self.queue.put(('D', id, data))

    def do_publish_refs(self, task_id, ref):
        for url in self.standby_urls:
            spawn_url = urljoin(url, '/task/%s/publish' % task_id)
            h = httplib2.Http()
            h.request(spawn_url, 'POST', ref)        

    def do_add_job(self, id, root_task_descriptor):
        for url in self.standby_urls:
            spawn_url = urljoin(url, '/job/%s' % id)
            h = httplib2.Http()
            h.request(spawn_url, 'POST', root_task_descriptor)
            
    def do_add_data(self, id, data):
        for url in self.standby_urls:
            spawn_url = urljoin(url, '/data/%s' % id)
            h = httplib2.Http()
            h.request(spawn_url, 'POST', data)        
        
    def do_add_worker(self, worker_descriptor):
        for url in self.standby_urls:
            spawn_url = urljoin(url, '/worker/')
            h = httplib2.Http()
            h.request(spawn_url, 'POST', worker_descriptor)
        
    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        
    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self.thread_main, args=())
            self.thread.start()
    
    def stop(self):
        if self.is_running:
            self.is_running = False
            self.queue.put(THREAD_TERMINATOR)
            self.thread.join()
            self.thread = None
            
    def thread_main(self):
        
        while True:
            
            # While not connected, attempt to register as a backup master.
            while self.is_running:

                try:
                    maybe_terminator = self.queue.get(block=True)
                    if not self.is_running or maybe_terminator is THREAD_TERMINATOR:
                        return
                except Empty:
                    pass
                
                log_entry = maybe_terminator
                
                try:
                    if log_entry[0] == 'U':
                        self.standby_urls.add(log_entry[1])
                    elif log_entry[0] == 'P':
                        self.do_publish_refs(log_entry[1], log_entry[2])
                    elif log_entry[0] == 'W':
                        self.do_add_worker(log_entry[1])
                    elif log_entry[0] == 'J':
                        self.do_add_job(log_entry[1], log_entry[2])
                    elif log_entry[0] == 'D':
                        self.do_add_data(log_entry[1], log_entry[2])
                    else:
                        raise
                except:
                    ciel.log('Error passing log to backup master.', 'BACKUP_SENDER', logging.WARN, True)
