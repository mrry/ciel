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
import optparse
import sys
import httplib2
import simplejson
import urlparse
import os

PROTOCOLS = {'json' : True, 'protobuf' : True, 'pickle' : True, 'line' : True}
FORMATS = {'json' : True, 'env' : True, 'protobuf' : True, 'pickle' : True}

def attach(worker_uri, pid, protocol='json'):
    
    h = httplib2.Http()
    process_request = simplejson.dumps((pid, protocol))
    try:
        response, content = h.request(urlparse.urljoin(worker_uri, '/control/process/'), 'POST', process_request)
    except:
        print >>sys.stderr, "Error: couldn't contact worker"
        return None

    if response['status'] != '200':
        print >>sys.stderr, "Error: non-OK status from worker (%s)" % response['status']
        return None

    else:
        return simplejson.loads(content)

def render(descriptor, format='json'):

    if format == 'json':
        print simplejson.dumps(descriptor)
    elif format == 'env':
        print 'export CIEL_PROCESS_ID=%s' % descriptor['id']
        print 'export CIEL_PROCESS_PROTOCOL=%s' % descriptor['protocol']
        print 'export CIEL_PIPE_TO_WORKER=%s' % descriptor['to_worker_fifo']
        print 'export CIEL_PIPE_FROM_WORKER=%s' % descriptor['from_worker_fifo']
        
    else:
        print >>sys.stderr, 'Format not yet supported: %s' % format
        raise


def main():
    parser = optparse.OptionParser(usage='sw-attach [options] -P PID')
    parser.add_option("-w", "--worker", action="store", dest="worker", help="Worker URI", metavar="WORKER", default='http://localhost:8001/')
    parser.add_option("-P", "--pid", action="store", dest="pid", help="Process ID", metavar="PID", type="int", default=os.getppid())
    parser.add_option("-p", "--protocol", action="store", dest="protocol", help="IPC protocol to use. Valid protocols are json (default), protobuf or pickle", default='json')
    parser.add_option("-F", "--format", action="store", dest="format", help="Format to write out umbilical details. Valid formats are json (default), env, protobuf or pickle", default='json')
    (options, _) = parser.parse_args()

    should_exit = False
    if options.protocol not in PROTOCOLS.keys():
        print >>sys.stderr, "Error: must specify a valid protocol"
        should_exit = True
        
    if options.format not in FORMATS.keys():
        print >>sys.stderr, "Error: must specify a valid format"
        should_exit = True

    if options.pid is None:
        print >>sys.stderr, "Error: must specify a process ID"
        should_exit = True
        
    if should_exit:
        parser.print_help()
        sys.exit(-1)
    else:
        descriptor = attach(options.worker, options.pid, options.protocol)
        if descriptor is None:
            parser.print_help()
            sys.exit(-1)
        else:
            render(descriptor, options.format)
            sys.exit(0)
        
if __name__ == '__main__':
    main()