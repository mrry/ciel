
import simplejson
import sys
import load
import urlparse
import httplib2
import pickle
import skywriting.runtime.executors
import time
import datetime
import os
import os.path
from skywriting.runtime.util.sw_pprint import sw_pprint

from skywriting.runtime.block_store import SWReferenceJSONEncoder,json_decode_object_hook,BlockStore
from shared.references import SW2_ConcreteReference
from optparse import OptionParser
import ciel

http = httplib2.Http()

def now_as_timestamp():
    return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

def resolve_vars(value, callback_map):
    
    def resolve_vars_val(value):
        if isinstance(value, list):
            return [resolve_vars_val(v) for v in value]
        elif isinstance(value, dict):
            for callback_key in callback_map:
                if callback_key in value:
                    return callback_map[callback_key](value)
            return dict([(resolve_vars_val(k), resolve_vars_val(v)) for (k, v) in value.items()])
        else:
            return value

    return resolve_vars_val(value)

def ref_of_string(val, master_uri):
    master_data_uri = urlparse.urljoin(master_uri, "control/data/")
    master_netloc = urlparse.urlparse(master_uri).netloc
    (_, content) = http.request(master_data_uri, "POST", val)
    return simplejson.loads(content, object_hook=json_decode_object_hook)
    
def ref_of_object(val, package_path, master_uri):
    if "filename" not in val:
        raise Exception("start_job can't handle resources that aren't files yet; package entries must have a 'filename' member")
    if not os.path.isabs(val["filename"]):
        # Construct absolute path by taking it as relative to package descriptor
        val["filename"] = os.path.join(package_path, val["filename"])
    if "index" in val and val["index"]:
        return load.do_uploads(master_uri, [val["filename"]])
    else:
        with open(val["filename"], "r") as infile:
            file_data = infile.read()
        return ref_of_string(file_data, master_uri)

def task_descriptor_for_package_and_initial_task(package_dict, start_handler, start_args, package_path, master_uri, args):

    def resolve_arg(value):
        try:
            return args[value["__args__"]]
        except IndexError:
            if "default" in value:
                print "Positional argument", value["__args__"], "not specified; using default", value["default"]
                return value["default"]
            else:
                raise Exception("Package requires at least %d args" % (value["__args__"] + 1))

    def resolve_env(value):
        try:
            return os.environ[value["__env__"]]
        except KeyError:
            if "default" in value:
                print "Environment variable", value["__env__"], "not specified; using default", value["default"]
                return value["default"]
            else:
                raise Exception("Package requires environment variable '%s'" % value["__env__"])

    env_and_args_callbacks = {"__args__": resolve_arg,
                              "__env__": resolve_env}
    package_dict = resolve_vars(package_dict, env_and_args_callbacks)
    start_args = resolve_vars(start_args, env_and_args_callbacks)

    submit_package_dict = dict([(k, ref_of_object(v, package_path, master_uri)) for (k, v) in package_dict.items()])
    package_ref = ref_of_string(pickle.dumps(submit_package_dict), master_uri)

    resolved_args = resolve_vars(start_args, {"__package__": lambda x: submit_package_dict[x["__package__"]]})

    return skywriting.runtime.executors.build_init_descriptor(start_handler, resolved_args, package_ref, master_uri, ref_of_string)

def submit_job_with_package(package_dict, start_handler, start_args, package_path, master_uri, args):
    
    task_descriptor = task_descriptor_for_package_and_initial_task(package_dict, start_handler, start_args, package_path, master_uri, args)

    print "Submitting descriptor:"
    sw_pprint(task_descriptor, indent=2)

    master_task_submit_uri = urlparse.urljoin(master_uri, "control/job/")
    (_, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))
    return simplejson.loads(content)

def await_job(jobid, master_uri):
    notify_url = urlparse.urljoin(master_uri, "control/job/%s/completion" % jobid)
    (_, content) = http.request(notify_url)
    completion_result = simplejson.loads(content, object_hook=json_decode_object_hook)
    if "error" in completion_result:
        raise Exception("Job failure: %s" % completion_result["error"])
    else:
        return completion_result["result_ref"]

def main():

    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    
    (options, args) = parser.parse_args()
    master_uri = options.master

    if master_uri is None or master_uri == "":
        raise Exception("Must specify a master with -m or SW_MASTER")
    
    with open(args[0], "r") as package_file:
        job_dict = simplejson.load(package_file)

    package_dict = job_dict["package"]
    start_dict = job_dict["start"]
    start_handler = start_dict["handler"]
    start_args = start_dict["args"]

    (package_path, _) = os.path.split(args[0])

    print "BEFORE_SUBMIT", now_as_timestamp()

    new_job = submit_job_with_package(package_dict, start_handler, start_args, package_path, master_uri, args[1:])

    print "SUBMITTED", now_as_timestamp()
    
    job_url = urlparse.urljoin(master_uri, "control/browse/job/%s" % new_job['job_id'])
    print "JOB_URL", job_url

    result = await_job(new_job['job_id'], master_uri)

    fakeBlockStore = BlockStore(ciel.engine, None, None, "/tmp")
    reflist = fakeBlockStore.retrieve_object_for_ref(result, "json")

    print "GOT_RESULT", now_as_timestamp()

    return reflist
