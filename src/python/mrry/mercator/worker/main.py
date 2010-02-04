'''
Created on 29 Jan 2010

@author: dgm36
'''

from multiprocessing import Process
from jobstarter import job_starter_main
from server import server_main
from pinger import dummy_pinger_main
from multiprocessing import Queue

def start_worker_processes():
    
    # Server uses this to send job-start messages to the job starter.
    job_starter_queue = Queue()
    
    # All processes use this to send ping messages to the relevant master (if necessary).
    pinger_queue = Queue()
    
    jobstarter = Process(target=job_starter_main, args=(job_starter_queue, pinger_queue), name="JobStarter")
    pinger = Process(target=dummy_pinger_main, args=(pinger_queue, ), name="Pinger")
    
    jobstarter.start()
    pinger.start()
    
    server_main(job_starter_queue, pinger_queue)

if __name__ == "__main__":
    start_worker_processes()