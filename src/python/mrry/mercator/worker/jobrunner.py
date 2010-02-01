'''
Created on 28 Jan 2010

@author: dgm36
'''
from multiprocessing.process import Process

def single_job_main(job, task_result_queue):
    print "I'm running a single job: %s" % (job.name, )
    result = job.execute()
    task_result_queue.put((job, result))

def job_runner_main(task_queue, task_result_queue):
    print "I'm a job runner"
    while True:
        job = task_queue.get()
        print "JobRunner got job %s" % (job.name, )
        Process(target=single_job_main, name="JobRunner(%s" % (job.name), args=(job, task_result_queue)).start()
