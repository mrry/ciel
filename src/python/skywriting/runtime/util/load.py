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
from optparse import OptionParser
import os
import httplib2
import urlparse
import simplejson
import math
import random
import uuid
from skywriting.runtime.references import SW2_ConcreteReference, SWNoProvenance
from skywriting.runtime.block_store import SWReferenceJSONEncoder

def get_worker_netlocs(master_uri):
    http = httplib2.Http()
    response, content = http.request(urlparse.urljoin(master_uri, '/worker/', 'GET'))
    if response.status != 200:
        raise Exception("Error: Could not contact master")
    workers = simplejson.loads(content)
    netlocs = []
    for worker in workers:#
        if not worker['failed']:
            netlocs.append(worker['netloc'])
    return netlocs
    
def build_extent_list(filename, options):

    extents = []
    
    try:
        file_size = os.path.getsize(filename)
    except:
        raise Exception("Could not get the size of the input file.")
    
    if options.size is not None:
        # Divide the input file into a variable number of fixed sized chunks.
        if options.delimiter is not None:
            # Use the delimiter, and the size is an upper bound.
            with open(filename, 'rb') as f:
                start = 0
                while start < file_size:
                    finish = min(start + options.size, file_size)
                    curr = None
                    if finish == file_size:
                        # This is the last block, so take everything up to the
                        # end of the file.
                        extents.append((start, file_size))
                    else:
                        # First try seeking from the approximate finish point backwards
                        # until we see a delimiter.
                        while finish > start: 
                            f.seek(finish)
                            curr = f.read(1)
                            if curr == options.delimiter:
                                finish += 1
                                break
                            finish -= 1
                            
                        if curr != options.delimiter:
                            # Need to seek forward.
                            finish = min(file_size, start + options.size + 1)
                            f.seek(finish)
                            while finish < file_size:
                                curr = f.read(1)
                                finish += 1              
                                if curr == options.delimiter:
                                    break
              
                            
                        extents.append((start, finish))
                        start = finish 
        else:
            # Chunks are a fixed number of bytes.    
            for start in range(0, file_size, options.size):
                extents.append((start, min(file_size, start + options.size)))
        
    elif options.count is not None:
        # Divide the input file into a fixed number of equal-sized chunks.
        if options.delimiter is not None:
            # Use the delimiter to divide chunks.
            chunk_size = int(math.ceil(file_size / float(options.count)))
            with open(filename, 'rb') as f:
                start = 0
                for i in range(0, options.count - 1):
                    finish = min(start + chunk_size, file_size)
                    curr = None
                    if finish == file_size:
                        # This is the last block, so take everything up to the
                        # end of the file.
                        extents.append((start, file_size))
                    else:
                        # First try seeking from the approximate finish point backwards
                        # until we see a delimiter.
                        while finish > start: 
                            f.seek(finish)
                            curr = f.read(1)
                            if curr == options.delimiter:
                                finish += 1
                                break
                            finish -= 1
                            
                        if curr != options.delimiter:
                            # Need to seek forward.
                            finish = min(file_size, start + chunk_size + 1)
                            f.seek(finish)
                            while finish < file_size:
                                curr = f.read(1)
                                finish += 1                            
                                if curr == options.delimiter:
                                    break
                                
                        extents.append((start, finish))
                        start = finish
                extents.append((start, file_size))

        else:
            # Chunks are an equal number of bytes.
            chunk_size = int(math.ceil(file_size / float(options.count)))
            for start in range(0, file_size, chunk_size):
                extents.append((start, min(file_size, start + chunk_size)))
    
    return extents
    
def select_targets(netlocs, num_replicas):
    assert num_replicas <= len(netlocs)
    return random.sample(netlocs, num_replicas)
    
def create_name_prefix(specified_name):
    if specified_name is None:
        specified_name = 'upload:%s' % str(uuid.uuid4())
    return specified_name
    
def make_block_id(name_prefix, block_index):
    return '%s:%s' % (name_prefix, block_index)
    
def upload_extent_to_targets(input_file, block_id, start, finish, targets, packet_size):

    input_file.seek(start)
    
    https = [httplib2.Http() for _ in targets]
        
    for h, target in zip(https, targets):
        h.request('http://%s/upload/%s' % (target, block_id), 'POST', 'start')
        
    for packet_start in range(start, finish, packet_size):
        packet = input_file.read(min(packet_size, finish - packet_start))
        for h, target in zip(https, targets):
            h.request('http://%s/upload/%s/%d' % (target, block_id, packet_start - start), 'POST', packet)
        
    for h, target in zip(https, targets):
        h.request('http://%s/upload/%s/commit' % (target, block_id), 'POST', 'end')
        
def upload_string_to_targets(input, block_id, targets, packet_size):

    https = [httplib2.Http() for _ in targets]
        
    for h, target in zip(https, targets):
        h.request('http://%s/upload/%s' % (target, block_id), 'POST', 'start')
        
    for packet_start in range(0, len(input), packet_size):
        slice = input[packet_start:min(packet_size, len(input) - packet_start)]
        for h, target in zip(https, targets):
            h.request('http://%s/upload/%s/%d' % (target, block_id, packet_start), 'POST', slice)
        
    for h, target in zip(https, targets):
        h.request('http://%s/upload/%s/commit' % (target, block_id), 'POST', 'end')
        
        
def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-s", "--size", action="store", dest="size", help="Block size in bytes", metavar="N", type="int", default=None)
    parser.add_option("-n", "--num-blocks", action="store", dest="count", help="Number of blocks", metavar="N", type="int", default=1)
    parser.add_option("-r", "--replication", action="store", dest="replication", help="Copies of each block", type="int", metavar="N", default=1)
    parser.add_option("-d", "--delimiter", action="store", dest="delimiter", help="Block delimiter character", metavar="CHAR", default=None)
    parser.add_option("-l", "--lines", action="store_const", dest="delimiter", const="\n", help="Use newline as block delimiter")
    parser.add_option("-p", "--packet-size", action="store", dest="packet_size", help="Upload packet size in bytes", metavar="N", type="int",default=16384)
    parser.add_option("-i", "--id", action="store", dest="name", help="Block name prefix", metavar="NAME", default=None)
    (options, args) = parser.parse_args()
    
    workers = get_worker_netlocs(options.master)
    
    input_filename = args[0]    
    
    extent_list = build_extent_list(input_filename, options)
    
    name_prefix = create_name_prefix(options.name)
    
    output_references = []
    
    # Upload the data in extents.
    with open(input_filename, 'rb') as input_file:
        for i, (start, finish) in enumerate(extent_list):
            targets = select_targets(workers, options.replication)
            block_name = make_block_id(name_prefix, i)
            upload_extent_to_targets(input_file, block_name, start, finish, targets, options.packet_size)
            conc_ref = SW2_ConcreteReference(block_name, SWNoProvenance(), finish - start, targets)
            output_references.append(conc_ref)
            
    # Upload the index object.
    index = simplejson.dumps(output_references, cls=SWReferenceJSONEncoder)
    index_targets = select_targets(workers, options.replication)
    block_name = '%s:index' % name_prefix
    upload_string_to_targets(index, block_name, index_targets, options.packet_size)
    index_ref = SW2_ConcreteReference(block_name, SWNoProvenance(), len(index), index_targets)
    
    print index_ref
    print
    for target in index_targets:
        print 'swbs://%s/%s' % (target, block_name)
    
if __name__ == '__main__':
    main()