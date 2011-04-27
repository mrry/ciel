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
Created on 15 Apr 2010

@author: dgm36
'''
from skywriting.runtime.object_cache import retrieve_object_for_ref
import skywriting.runtime.util.start_job
import time
import datetime
import sys
import os
from optparse import OptionParser

def now_as_timestamp():
    return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

def main():
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    parser.add_option("-i", "--id", action="store", dest="id", help="Job ID", metavar="ID", default="default")
    parser.add_option("-e", "--env", action="store_true", dest="send_env", help="Set this flag to send the current environment with the script as _env", default=False)
    (options, args) = parser.parse_args()
   
    if not options.master:
        parser.print_help()
        print >> sys.stderr, "Must specify master URI with --master"
        sys.exit(1)

    if len(args) != 1:
        parser.print_help()
        print >> sys.stderr, "Must specify one script file to execute, as argument"
        sys.exit(1)

    script_name = args[0]
    master_uri = options.master
    id = options.id
    
    print id, "STARTED", now_as_timestamp()

    swi_package = {"swimain": {"filename": script_name}}
    swi_args = {"sw_file_ref": {"__package__": "swimain"}, "start_args": args}
    if options.send_env:
        swi_args["start_env"] = dict(os.environ)

    new_job = skywriting.runtime.util.start_job.submit_job_with_package(swi_package, "swi", swi_args, os.getcwd(), master_uri)
    
    result = skywriting.runtime.util.start_job.await_job(new_job["job_id"], master_uri)
    
    reflist = retrieve_object_for_ref(result, "json")
    sw_return = retrieve_object_for_ref(reflist[0], "json")
    return sw_return
    
if __name__ == '__main__':
    main()
