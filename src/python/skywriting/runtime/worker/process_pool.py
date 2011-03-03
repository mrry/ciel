# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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
import tempfile
import os
import uuid
import ciel
import logging

class ProcessRecord:
    """Represents a long-running process that is attached to this worker."""
    
    def __init__(self, id, pid, protocol):
        self.id = id
        self.pid = pid
        self.protocol = protocol
        
        # The worker communicates with the process using a pair of named pipes.
        self.fifos_dir = tempfile.mkdtemp(prefix="ciel-ipc-fifos-")
                
        self.from_process_fifo_name = os.path.join(self.fifos_dir, 'from_process')
        os.mkfifo(self.from_process_fifo_name)
        self.from_process_fifo = os.open(self.from_process_fifo_name, os.O_RDONLY | os.O_NONBLOCK)
        
        self.to_process_fifo_name = os.path.join(self.fifos_dir, 'to_process')
        os.mkfifo(self.to_process_fifo_name)
        # Linux hack: this prevents ENXIO when we open the pipe in non-blocking mode.
        self.to_process_fifo = os.open(self.to_process_fifo_name, os.O_RDWR | os.O_NONBLOCK)
        
    def cleanup(self):
        try:
            os.close(self.from_process_fifo)
            os.close(self.to_process_fifo)
        except:
            ciel.log('Error cleaning up process %s, ignoring' % self.id, 'PROCESS', logging.WARN)
        
    def as_descriptor(self):
        return {'id' : self.id,
                'protocol' : self.protocol,
                'to_worker_fifo' : self.from_process_fifo_name,
                'from_worker_fifo' : self.to_process_fifo_name}
        
    def __repr__(self):
        return 'ProcessRecord(%s, %s)' % (repr(self.id), repr(self.pid))

class ProcessPool:
    """Represents a collection of long-running processes attached to this worker."""

    def __init__(self, bus):
        self.processes = {}
        self.bus = bus
        
    def subscribe(self):
        self.bus.subscribe('start', self.start)
        self.bus.subscribe('stop', self.stop)
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start)
        self.bus.unsubscribe('stop', self.stop)
        
    def start(self):
        pass
    
    def stop(self):
        for record in self.processes.values():
            record.cleanup()
        
    def create_process_record(self, pid, protocol, id=None):
        if id is None:
            id = str(uuid.uuid4())
        
        record = ProcessRecord(id, pid, protocol)
        self.processes[id] = record
        return record
        
    def get_process_record(self, id):
        try:
            return self.processes[id]
        except KeyError:
            return None
        
    def delete_process_record(self, record):
        del self.processes[record.id]
        record.cleanup()
        
    def get_process_ids(self):
        return self.processes.keys()