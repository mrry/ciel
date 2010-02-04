'''
Created on 28 Jan 2010

@author: dgm36
'''
from multiprocessing.process import Process
import threading

def single_job_main(job, task_result_queue):
    handler_thread = threading.Thread(target=job.handleRequests, args=(job,))
    worker_thread = threading.Thread(target=job.main, args=(job,))
    
    handler_thread.start()
    
    worker_thread.join()
    handler_thread.join()
    
    print "I'm running a single job: %s" % (job.name, )
    result = job.execute()
    task_result_queue.put((job, result))

def job_starter_main(job_starter_queue, pinger_queue):
    while True:
        job = job_starter_queue.get()
        Process(target=single_job_main, name="Job(%s)" % (job.name), args=(job,)).start()
