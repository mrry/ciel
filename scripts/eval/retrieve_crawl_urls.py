import sys
import httplib2
import simplejson

for clusterfile in sys.argv[1:]:
    with open(clusterfile) as f:
        master_hostname = f.readline().strip()
    
    #print master_hostname

    h = httplib2.Http()
    all_jobs_url = "http://%s:8000/control/job/" % master_hostname

    _, content = h.request(all_jobs_url)

    job_list = simplejson.loads(content)

    crawl_url = "http://%s:8000/control/job/%s/" % (master_hostname, job_list[0])

    print crawl_url
