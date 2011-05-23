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
from shared.references import SW2_ConcreteReference, SW2_FetchReference, SWReferenceJSONEncoder
import sys
import time
import itertools

def get_worker_netlocs(master_uri):
    http = httplib2.Http()
    response, content = http.request(urlparse.urljoin(master_uri, 'control/worker/', 'GET'))
    if response.status != 200:
        raise Exception("Error: Could not contact master")
    workers = simplejson.loads(content)
    netlocs = []
    for worker in workers:#
        if not worker['failed']:
            netlocs.append(worker['netloc'])
    return netlocs
    
def build_extent_list(filename, size, count, delimiter):

    extents = []
    
    try:
        file_size = os.path.getsize(filename)
    except:
        raise Exception("Could not get the size of the input file.")
    
    if size is not None:
        # Divide the input file into a variable number of fixed sized chunks.
        if delimiter is not None:
            # Use the delimiter, and the size is an upper bound.
            with open(filename, 'rb') as f:
                start = 0
                while start < file_size:
                    finish = min(start + size, file_size)
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
                            if curr == delimiter:
                                finish += 1
                                break
                            finish -= 1
                            
                        if curr != delimiter:
                            # Need to seek forward.
                            finish = min(file_size, start + size + 1)
                            f.seek(finish)
                            while finish < file_size:
                                curr = f.read(1)
                                finish += 1              
                                if curr == delimiter:
                                    break
              
                            
                        extents.append((start, finish))
                        start = finish 
        else:
            # Chunks are a fixed number of bytes.    
            for start in range(0, file_size, size):
                extents.append((start, min(file_size, start + size)))
        
    elif count is not None:
        # Divide the input file into a fixed number of equal-sized chunks.
        if delimiter is not None:
            # Use the delimiter to divide chunks.
            chunk_size = int(math.ceil(file_size / float(count)))
            with open(filename, 'rb') as f:
                start = 0
                for i in range(0, count - 1):
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
                            if curr == delimiter:
                                finish += 1
                                break
                            finish -= 1
                            
                        if curr != delimiter:
                            # Need to seek forward.
                            finish = min(file_size, start + chunk_size + 1)
                            f.seek(finish)
                            while finish < file_size:
                                curr = f.read(1)
                                finish += 1                            
                                if curr == delimiter:
                                    break
                                
                        extents.append((start, finish))
                        start = finish
                extents.append((start, file_size))

        else:
            # Chunks are an equal number of bytes.
            chunk_size = int(math.ceil(file_size / float(count)))
            for start in range(0, file_size, chunk_size):
                extents.append((start, min(file_size, start + chunk_size)))
    
    return extents
    
def select_targets(netlocs, num_replicas):
    if num_replicas > len(netlocs):
        print 'Not enough replicas remain... exiting.'
        sys.exit(-1)
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
        h.request('http://%s/control/upload/%s' % (target, block_id), 'POST', 'start')
        
    for packet_start in range(start, finish, packet_size):
        packet = input_file.read(min(packet_size, finish - packet_start))
        for h, target in zip(https, targets):
            h.request('http://%s/control/upload/%s/%d' % (target, block_id, packet_start - start), 'POST', packet)
        
    for h, target in zip(https, targets):
        h.request('http://%s/control/upload/%s/commit' % (target, block_id), 'POST', simplejson.dumps(finish - start))
        h.request('http://%s/control/admin/pin/%s' % (target, block_id), 'POST', 'pin')
        
def upload_string_to_targets(input, block_id, targets):

    https = [httplib2.Http() for _ in targets]
        
    for h, target in zip(https, targets):
        h.request('http://%s/control/upload/%s' % (target, block_id), 'POST', 'start')
        
    for h, target in zip(https, targets):
        h.request('http://%s/control/upload/%s/%d' % (target, block_id, 0), 'POST', input)
        
    for h, target in zip(https, targets):
        h.request('http://%s/control/upload/%s/commit' % (target, block_id), 'POST', simplejson.dumps(len(input)))
        h.request('http://%s/control/admin/pin/%s' % (target, block_id), 'POST', 'pin')

def do_uploads(master, args, size=None, count=1, replication=1, delimiter=None, packet_size=1048576, name=None, do_urls=False, urllist=None, repeat=1):
    
    workers = get_worker_netlocs(master)
    
    name_prefix = create_name_prefix(name)
    
    output_references = []
    
    # Upload the data in extents.
    if not do_urls:
        
        if len(args) == 1:
            input_filename = args[0] 
            extent_list = build_extent_list(input_filename, size, count, delimiter)
        
            with open(input_filename, 'rb') as input_file:
                for i, (start, finish) in enumerate(extent_list):
                    targets = select_targets(workers, replication)
                    block_name = make_block_id(name_prefix, i)
                    print >>sys.stderr, 'Uploading %s to (%s)' % (block_name, ",".join(targets))
                    upload_extent_to_targets(input_file, block_name, start, finish, targets, packet_size)
                    conc_ref = SW2_ConcreteReference(block_name, finish - start, targets)
                    output_references.append(conc_ref)
                    
        else:
            
            for i, input_filename in enumerate(args):
                with open(input_filename, 'rb') as input_file:
                    targets = select_targets(workers, replication)
                    block_name = make_block_id(name_prefix, i)
                    block_size = os.path.getsize(input_filename)
                    print >>sys.stderr, 'Uploading %s to (%s)' % (input_filename, ",".join(targets))
                    upload_extent_to_targets(input_file, block_name, 0, block_size, targets, packet_size)
                    conc_ref = SW2_ConcreteReference(block_name, block_size, targets)
                    output_references.append(conc_ref)

    else:
        
        if urllist is None:
            urls = []
            for filename in args:
                with open(filename, 'r') as f:
                    for line in f:
                        urls.append(line.strip())
        else:
            urls = urllist
            
        urls = itertools.chain.from_iterable(itertools.repeat(urls, repeat))
            
        #target_fetch_lists = {}
        
        upload_sessions = []
                    
        output_ref_dict = {}
                    
        for i, url in enumerate(urls):
            targets = select_targets(workers, replication)
            block_name = make_block_id(name_prefix, i)
            ref = SW2_FetchReference(block_name, url, i)
            for j, target in enumerate(targets):
                upload_sessions.append((target, ref, j))
            h = httplib2.Http()
            print >>sys.stderr, 'Getting size of %s' % url
            try:
                response, _ = h.request(url, 'HEAD')
                size = int(response['content-length'])
            except:
                print >>sys.stderr, 'Error while getting size of %s; assuming default size (1048576 bytes)' % url
                size = 1048576 
                
            # The refs will be updated as uploads succeed.
            conc_ref = SW2_ConcreteReference(block_name, size)
            output_ref_dict[block_name] = conc_ref
            output_references.append(conc_ref)
        
        while True:

            pending_uploads = {}
            
            failed_this_session = []
            
            for target, ref, index in upload_sessions:
                h2 = httplib2.Http()
                print >>sys.stderr, 'Uploading to %s' % target
                id = uuid.uuid4()
                response, _ = h2.request('http://%s/control/fetch/%s' % (target, id), 'POST', simplejson.dumps([ref], cls=SWReferenceJSONEncoder))
                if response.status != 202:
                    print >>sys.stderr, 'Failed... %s' % target
                    #failed_targets.add(target)
                    failed_this_session.append((ref, index))
                else:
                    pending_uploads[ref, index] = target, id
            
            # Wait until we get a non-try-again response from all of the targets.
            while len(pending_uploads) > 0:
                time.sleep(3)
                for ref, index in list(pending_uploads.keys()):
                    target, id = pending_uploads[ref, index]
                    try:
                        response, _ = h2.request('http://%s/control/fetch/%s' % (target, id), 'GET')
                        if response.status == 408:
                            print >>sys.stderr, 'Continuing to wait for %s:%d on %s' % (ref.id, index, target)
                            continue
                        elif response.status == 200:
                            print >>sys.stderr, 'Succeded! %s:%d on %s' % (ref.id, index, target)
                            output_ref_dict[ref.id].location_hints.add(target)
                            del pending_uploads[ref, index]
                        else:
                            print >>sys.stderr, 'Failed... %s' % target
                            del pending_uploads[ref, index]
                            failed_this_session.append((ref, index))

                    except:
                        print >>sys.stderr, 'Failed... %s' % target
                        del pending_uploads[target]
                        
            if len(pending_uploads) == 0 and len(failed_this_session) == 0:
                break
                        
            # All transfers have finished or failed, so check for failures.
            if len(failed_this_session) > 0:
                
                # We refetch the worker list, in case any have failed in the mean time.
                workers = get_worker_netlocs(master)
                
                upload_sessions = []
                
                for ref, index in failed_this_session:
                    target, = select_targets(workers, 1)
                    upload_sessions.append(target, ref, index)
                    
    # Upload the index object.
    index = simplejson.dumps(output_references, cls=SWReferenceJSONEncoder)
    block_name = '%s:index' % name_prefix
    
    index_targets = select_targets(workers, replication)
    upload_string_to_targets(index, block_name, index_targets)
    
    index_ref = SW2_ConcreteReference(block_name, len(index), index_targets)
        
    return index_ref

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-s", "--size", action="store", dest="size", help="Block size in bytes", metavar="N", type="int", default=None)
    parser.add_option("-n", "--num-blocks", action="store", dest="count", help="Number of blocks", metavar="N", type="int", default=1)
    parser.add_option("-r", "--replication", action="store", dest="replication", help="Copies of each block", type="int", metavar="N", default=1)
    parser.add_option("-d", "--delimiter", action="store", dest="delimiter", help="Block delimiter character", metavar="CHAR", default=None)
    parser.add_option("-l", "--lines", action="store_const", dest="delimiter", const="\n", help="Use newline as block delimiter")
    parser.add_option("-p", "--packet-size", action="store", dest="packet_size", help="Upload packet size in bytes", metavar="N", type="int",default=1048576)
    parser.add_option("-i", "--id", action="store", dest="name", help="Block name prefix", metavar="NAME", default=None)
    parser.add_option("-u", "--urls", action="store_true", dest="urls", help="Treat files as containing lists of URLs", default=False)
    (options, args) = parser.parse_args()
    
    index_ref = do_uploads(options.master, args, options.size, options.count, options.replication, options.delimiter, options.packet_size, options.name, options.urls)

    block_name = index_ref.id
    index_targets = index_ref.location_hints

    for target in index_targets:
        print 'swbs://%s/%s' % (target, block_name)
    
if __name__ == '__main__':
    main()
