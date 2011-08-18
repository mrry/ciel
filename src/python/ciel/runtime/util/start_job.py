import sys
import simplejson
import load
import urlparse
import httplib2
import pickle
import time
import datetime
import os.path
from ciel.runtime.util.sw_pprint import sw_pprint

from ciel.public.references import SWReferenceJSONEncoder,json_decode_object_hook,\
    SW2_FutureReference, SWDataValue, SWErrorReference,\
    SW2_SocketStreamReference, SW2_StreamReference, SW2_ConcreteReference,\
    build_reference_from_tuple
from ciel.runtime.object_cache import retrieve_object_for_ref, decoders
from optparse import OptionParser
from ciel.runtime.block_store import get_fetch_urls_for_ref
from StringIO import StringIO
import sys
from ciel.runtime.executors.init import build_init_descriptor

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
    
def ref_of_object(key, val, package_path, master_uri):
    if "__ref__" in val:
        return build_reference_from_tuple(val['__ref__'])
    if "filenamelist" in val:
        
        with open(val["filenamelist"]) as listfile:
            filenamelist = [x.strip() for x in listfile.readlines()]
        
        try:
            replication = val["replication"]
        except KeyError:
            replication = 1
        return load.do_uploads(master_uri, filenamelist, replication=replication)
    if "filename" not in val and "urls" not in val:
        raise Exception("start_job can't handle resources that aren't files yet; package entries must have a 'filename' member")
    if "filename" in val and not os.path.isabs(val["filename"]):
        # Construct absolute path by taking it as relative to package descriptor
        if "cwd" in val and val["cwd"]:
            val["filename"] = os.path.join(os.getcwd(), val["filename"])
        else:
            val["filename"] = os.path.join(package_path, val["filename"])
    if "urlindex" in val and val["urlindex"]:
        try:
            replication = val["replication"]
        except KeyError:
            replication = 3
        try:
            repeat = val["repeat"]
        except KeyError:
            repeat = 1
        if 'urls' in val:
            return load.do_uploads(master_uri, [], urllist=val["urls"], do_urls=True, replication=replication, repeat=repeat)
        else:
            return load.do_uploads(master_uri, [val["filename"]], do_urls=True, replication=replication)
    elif "index" in val and val["index"]:
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
                print >>sys.stderr, "Positional argument", value["__args__"], "not specified; using default", value["default"]
                return value["default"]
            else:
                print >>sys.stderr, "Mandatory argument", value["__args__"], "not specified."
                sys.exit(-1)

    def resolve_env(value):
        try:
            return os.environ[value["__env__"]]
        except KeyError:
            if "default" in value:
                print >>sys.stderr, "Environment variable", value["__env__"], "not specified; using default", value["default"]
                return value["default"]
            else:
                print >>sys.stderr, "Mandatory environment variable", value["__env__"], "not specified."
                sys.exit(-1)

    env_and_args_callbacks = {"__args__": resolve_arg,
                              "__env__": resolve_env}
    package_dict = resolve_vars(package_dict, env_and_args_callbacks)
    start_args = resolve_vars(start_args, env_and_args_callbacks)

    submit_package_dict = dict([(k, ref_of_object(k, v, package_path, master_uri)) for (k, v) in package_dict.items()])
    for key, ref in submit_package_dict.items():
        print >>sys.stderr, key, '-->', simplejson.dumps(ref, cls=SWReferenceJSONEncoder)
    package_ref = ref_of_string(pickle.dumps(submit_package_dict), master_uri)

    resolved_args = resolve_vars(start_args, {"__package__": lambda x: submit_package_dict[x["__package__"]]})

    return build_init_descriptor(start_handler, resolved_args, package_ref, master_uri, ref_of_string)

def submit_job_with_package(package_dict, start_handler, start_args, job_options, package_path, master_uri, args):
    
    task_descriptor = task_descriptor_for_package_and_initial_task(package_dict, start_handler, start_args, package_path, master_uri, args)

    #print "Submitting descriptor:"
    #sw_pprint(task_descriptor, indent=2)

    payload = {"root_task" : task_descriptor, "job_options" : job_options}

    master_task_submit_uri = urlparse.urljoin(master_uri, "control/job/")
    (_, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(payload, cls=SWReferenceJSONEncoder))
    try:
        return simplejson.loads(content)
    except ValueError:
        print >>sys.stderr, 'Error submitting job'
        print >>sys.stderr, content
        sys.exit(-1)

def await_job(jobid, master_uri, timeout=None):
    notify_url = urlparse.urljoin(master_uri, "control/job/%s/completion" % jobid)
    if timeout is not None:
        payload = simplejson.dumps({'timeout' : timeout})
    else:
        payload = None
    completion_result = None
    for i in range(5):
        try:
            (_, content) = http.request(notify_url, "GET" if timeout is None else "POST", body=payload)
            completion_result = simplejson.loads(content, object_hook=json_decode_object_hook)
            break
        except:
            print >>sys.stderr, "Decode failed; retrying fetch..."
            pass

    if timeout is not None:
        return completion_result

    if completion_result is not None and "error" in completion_result:
        print >>sys.stderr, "Job failed: %s" % completion_result["error"]
        sys.exit(-1)
    elif completion_result is not None:
        return completion_result["result_ref"]
    else:
        print >>sys.stderr, 'Error receiving result from master'
    
def external_get_real_ref(ref, jobid, master_uri):
    fetch_url = urlparse.urljoin(master_uri, "control/ref/%s/%s" % (jobid, ref.id))
    _, content = httplib2.Http().request(fetch_url)
    real_ref = simplejson.loads(content, object_hook=json_decode_object_hook)
    print >>sys.stderr, "Resolved", ref, "-->", real_ref
    return real_ref 
    
def simple_retrieve_object_for_ref(ref, decoder, jobid, master_uri):
    if isinstance(ref, SWErrorReference):
        raise Exception("Can't decode %s" % ref)
    if isinstance(ref, SW2_FutureReference) or isinstance(ref, SW2_StreamReference) or isinstance(ref, SW2_SocketStreamReference):
        ref = external_get_real_ref(ref, jobid, master_uri)
    if isinstance(ref, SWDataValue):
        return retrieve_object_for_ref(ref, decoder, None)
    elif isinstance(ref, SW2_ConcreteReference):
        urls = get_fetch_urls_for_ref(ref)
        _, content = httplib2.Http().request(urls[0])
        return decoders[decoder](StringIO(content))
    else:
        raise Exception("Don't know how to retrieve a %s" % ref)
    
def recursive_decode(to_decode, template, jobid, master_uri):
    if isinstance(template, dict):
        
        try:
            decode_method = template["__decode_ref__"]
            decoded_value = simple_retrieve_object_for_ref(to_decode, decode_method, jobid, master_uri)
            recurse_template = template.get("value", None)
            if recurse_template is None:
                return decoded_value
            else:
                return recursive_decode(decoded_value, recurse_template, jobid, master_uri)
        except KeyError:
            pass
    
        try:
            concat_list = template["__concat__"]
            try:
                decode_method = template["encoding"]
            except KeyError:
                decode_method = "json"
            print to_decode
            decoded_value = [simple_retrieve_object_for_ref(x, decode_method, jobid, master_uri) for x in to_decode]
            assert isinstance(concat_list, list)
            assert isinstance(decoded_value, list)
            return "".join(recursive_decode(decoded_value, concat_list, jobid, master_uri))
        except KeyError:
            pass
        
        if not isinstance(to_decode, dict):
            raise Exception("%s and %s: Type mismatch" % to_decode, template)
        ret_dict = {}
        for (k, v) in to_decode:
            value_template = template.get(k, None)
            if value_template is None:
                ret_dict[k] = v
            else:
                ret_dict[k] = recursive_decode(v, value_template, jobid, master_uri)
        return ret_dict
    elif isinstance(template, list):
        if len(to_decode) != len(template):
            raise Exception("%s and %s: length mismatch" % to_decode, template)
        result_list = []
        for (elem_decode, elem_template) in zip(to_decode, template):
            if elem_template is None:
                result_list.append(elem_decode)
            else:
                result_list.append(recursive_decode(elem_decode, elem_template, jobid, master_uri))
        return result_list

def main(my_args=sys.argv):

    parser = OptionParser(usage='Usage: ciel run [options] PACKAGE_FILE')
    parser.add_option("-m", "--master", action="store", dest="master", help="URI of the cluster master", metavar="MASTER", default=os.getenv("CIEL_MASTER", "http://localhost:8000"))
    
    (options, args) = parser.parse_args(args=my_args)
    master_uri = options.master

    if master_uri is None or master_uri == "":
        print >>sys.stderr, "Must specify a master with -m or CIEL_MASTER"
        parser.print_help()
        sys.exit(-1)
    elif len(args) < 1:
        print >>sys.stderr, "Must specify a package file to run"
        parser.print_help()
        sys.exit(-1)
    
    with open(args[-1], "r") as package_file:
        job_dict = simplejson.load(package_file)

    package_dict = job_dict.get("package",{})
    start_dict = job_dict["start"]
    start_handler = start_dict["handler"]
    start_args = start_dict["args"]
    try:
        job_options = job_dict["options"]
    except KeyError:
        job_options = {}
    
    (package_path, _) = os.path.split(args[-1])

    print "BEFORE_SUBMIT", now_as_timestamp()

    new_job = submit_job_with_package(package_dict, start_handler, start_args, job_options, package_path, master_uri, args[2:])

    print "SUBMITTED", now_as_timestamp()
    
    job_url = urlparse.urljoin(master_uri, "control/browse/job/%s" % new_job['job_id'])
    print "JOB_URL", job_url

    result = await_job(new_job['job_id'], master_uri)

    print "GOT_RESULT", now_as_timestamp()
    
    reflist = simple_retrieve_object_for_ref(result, "json", new_job['job_id'], master_uri)
    
    decode_template = job_dict.get("result", None)
    if decode_template is None:
        return reflist
    else:
        try:
            decoded = recursive_decode(reflist, decode_template, new_job['job_id'], master_uri)
            return decoded
        except Exception as e:
            print "Failed to decode due to exception", repr(e)
            return reflist
        
def submit():
    """Toned-down version of the above for automation purposes."""        
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("CIEL_MASTER"))
    
    (options, args) = parser.parse_args()
    master_uri = options.master

    if master_uri is None or master_uri == "":
        raise Exception("Must specify a master with -m or CIEL_MASTER")
    
    with open(args[0], "r") as package_file:
        job_dict = simplejson.load(package_file)

    package_dict = job_dict.get("package",{})
    start_dict = job_dict["start"]
    start_handler = start_dict["handler"]
    start_args = start_dict["args"]
    try:
        job_options = job_dict["options"]
    except KeyError:
        job_options = {}
    

    (package_path, _) = os.path.split(args[0])

    new_job = submit_job_with_package(package_dict, start_handler, start_args, job_options, package_path, master_uri, args[1:])
    
    job_browse_url = urlparse.urljoin(master_uri, "control/browse/job/%s" % new_job['job_id'])
    print >>sys.stderr, "Information available at ", job_browse_url

    print new_job['job_id']

def wait():
    """Toned-down version of the above for automation purposes."""        
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("CIEL_MASTER"))
    parser.add_option("-t", "--timeout", action="store", dest="timeout", help="Timeout", metavar="DURATION", default=10)
    
    (options, args) = parser.parse_args()

    master_uri = options.master

    if master_uri is None or master_uri == "":
        raise Exception("Must specify a master with -m or CIEL_MASTER")

    result = await_job(args[0], master_uri, options.timeout)

    print >>sys.stderr, "Job done?", result

    return result

def result():
    """Toned-down version of the above for automation purposes."""        
    
    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("CIEL_MASTER"))
    parser.add_option("-t", "--timeout", action="store", dest="timeout", help="Timeout", metavar="DURATION", default=10)
    parser.add_option("-p", "--package", action="store", dest="package", help="Package file (for parsing format)", metavar="FILE", default=None)

    (options, args) = parser.parse_args()

    if len(args) < 1:
        print >>sys.stderr, "Must specify the job ID on the command line"
        sys.exit(-1)

    master_uri = options.master

    if master_uri is None or master_uri == "":
        print >>sys.stderr, ("Must specify a master with -m or CIEL_MASTER")
        sys.exit(-1)

    result = await_job(args[0], master_uri)

    print result

    if not result:
        print >>sys.stderr, "Timed out"
        sys.exit(-2)

    reflist = simple_retrieve_object_for_ref(result, "json", args[0], master_uri)

    if options.package is not None:
        with open(options.package) as f:
            job_dict = simplejson.load(f)
            decode_template = job_dict.get("result", None)
    else:
        return reflist
        
    if decode_template is not None:
        try:
            decoded = recursive_decode(reflist, decode_template, args[0], master_uri)
            return decoded
        except Exception as e:
            print "Failed to decode due to exception", repr(e)
            return reflist
    else:
        return reflist
