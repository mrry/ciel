# Copyright (c) 2010 Christopher Smowton <chris.smowton@cl.cam.ac.uk>
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
import skywriting.runtime.util.start_job
from skywriting.runtime.object_cache import retrieve_object_for_ref
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
    parser.add_option("-s", "--skypy-stub", action="store", dest="skypy_stub", help="Path to Skypy stub.py", metavar="PATH", default=None)
    (options, args) = parser.parse_args()
   
    if not options.master:
        parser.print_help()
        print >> sys.stderr, "Must specify master URI with --master"
        sys.exit(1)

    master_uri = options.master
    
    script_name = args[0]
    script_args = args[1:]

    sp_package = {"skypymain": {"filename": script_name}}
    sp_args = {"pyfile_ref": {"__package__": "skypymain"}, "entry_point": "skypy_main", "entry_args": script_args}

    new_job = skywriting.runtime.util.start_job.submit_job_with_package(sp_package, "skypy", sp_args, os.getcwd(), master_uri, args)
    
    result = skywriting.runtime.util.start_job.await_job(new_job["job_id"], master_uri)

    reflist = retrieve_object_for_ref(result, "json", None)

    return reflist[0]

if __name__ == '__main__':
    main()
