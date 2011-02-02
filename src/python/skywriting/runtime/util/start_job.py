
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
from optparse import OptionParser

def main():

    parser = OptionParser()
    parser.add_option("-m", "--master", action="store", dest="master", help="Master URI", metavar="MASTER", default=os.getenv("SW_MASTER"))
    (options, args) = parser.parse_args()
    master_uri = options.master
    master_netloc = urlparse.urlparse(master_uri).netloc
    
    with open(args[0], "r") as package_file:
        job_dict = simplejson.load(package_file)

    package_dict = job_dict["package"]

    def now_as_timestamp():
        return (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())

    def ref_of_string(val, id=None):
        http = httplib2.Http()
        if id is None:
            master_data_uri = urlparse.urljoin(master_uri, "/data/")
        else:
            master_data_uri = urlparse.urljoin(master_uri, "/data/" + id)
        (_, content) = http.request(master_data_uri, "POST", val)
        newid = simplejson.loads(content)
        if id is None:
            return SW2_ConcreteReference(newid, len(val), [master_netloc])
        else:
            return SW2_ConcreteReference(id, len(val), [master_netloc])
    
    def ref_of_object(val):
        if "filename" not in val:
            raise Exception("start_job can't handle resources that aren't files yet; package entries must have a 'filename' member")
        if not os.path.isabs(val["filename"]):
            # Construct absolute path by taking it as relative to package descriptor
            (filehead, filetail) = os.path.split(args[0])
            val["filename"] = os.path.join(filehead, val["filename"])
        if "index" in val and val["index"]:
            return load.do_uploads(master_uri, [val["filename"]])
        else:
            with open(val["filename"], "r") as infile:
                file_data = infile.read()
            return ref_of_string(file_data)

    submit_package_dict = dict([(k, ref_of_object(v)) for (k, v) in package_dict.items()])

    package_ref = ref_of_string(pickle.dumps(submit_package_dict))

    start_dict = job_dict["start"]
    start_handler = start_dict["handler"]
    start_args = start_dict["args"]

    def resolve_package_refs(value):
        if isinstance(value, list):
            return [resolve_package_refs(v) for v in value]
        elif isinstance(value, dict):
            if "__ref__" in value:
                return package_dict[value["__package__"]]
            else:
                return dict([(resolve_package_refs(k), resolve_package_refs(v)) for (k, v) in value])
        else:
            return value

    resolved_args = resolve_package_refs(start_args)

    class FakeTaskExecutor:
        def __init__(self, package_ref):
            self.package_ref = package_ref

    task_descriptor = {"handler": start_handler, "dependencies": set()}
       
    fake_te = FakeTaskExecutor(package_ref)
    build_executor = skywriting.runtime.executors.ExecutionFeatures().get_executor(start_handler, fake_te)
    build_executor.build_task_descriptor(**start_args)

    master_task_submit_uri = urlparse.urljoin(master_uri, "/job/")
    (_, content) = http.request(master_task_submit_uri, "POST", simplejson.dumps(task_descriptor, cls=SWReferenceJSONEncoder))

    out = simplejson.loads(content)
    
    notify_url = urlparse.urljoin(master_uri, "/job/%s/completion" % out['job_id'])
    job_url = urlparse.urljoin(master_uri, "/browse/job/%s" % out['job_id'])

    print id, "JOB_URL", job_url
    
    (_, content) = http.request(notify_url)
    completion_result = simplejson.loads(content, object_hook=json_decode_object_hook)
    if "error" in completion_result.keys():
        print id, "ERROR", completion_result["error"]
    else:
        print id, "GOT_RESULT", now_as_timestamp()
        print completion_result["result_ref"]
