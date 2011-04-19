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

import optparse
import sys
import traceback

from skywriting.lang.task import SkywritingTask
from shared.rpc_helper import RpcHelper, ShutdownException
    
parser = optparse.OptionParser()
parser.add_option("-v", "--version", action="store_true", dest="version", default=False, help="Display version info")
parser.add_option("-w", "--write-fifo", action="store", dest="write_fifo", default=None, help="FIFO for communication towards Ciel")
parser.add_option("-r", "--read-fifo", action="store", dest="read_fifo", default=None, help="FIFO for communication from Ciel")
parser.add_option("-l", "--stdlib-base", action="store", dest="stdlib_base", default=None, help="Path to Skywriting's standard library, for include statements")

(options, args) = parser.parse_args()

if options.version:
    print "Ciel Skywriting v0.2. Python:"
    print sys.version
    sys.exit(0)
    
if options.write_fifo is None or options.read_fifo is None:
    print >>sys.stderr, "Skywriting: Must specify a read-fifo and a write-fifo."
    sys.exit(1)

write_fp = open(options.write_fifo, "w")
read_fp = open(options.read_fifo, "r")

message_helper = RpcHelper(read_fp, write_fp)

while True:

    try:
        task = SkywritingTask(message_helper, options.stdlib_base)
        task.run()
        message_helper.send_message("exit", {"keep_process": "may_keep"})
        
    except ShutdownException as e:
        print >>sys.stderr, "Skywriting: dying at Ciel's request. Given reason:", e.reason
        sys.exit(0) 
        
    except Exception as e:
        print >>sys.stderr, "Skywriting: dying due to exception at top level"
        message_helper.send_message("error", {"report": traceback.format_exc()})
        sys.exit(1)

