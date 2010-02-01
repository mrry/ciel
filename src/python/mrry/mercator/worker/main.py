'''
Created on 29 Jan 2010

@author: dgm36
'''

from multiprocessing import Process
from jobrunner import job_runner_main
from server import server_main
from pinger import pinger_main
from multiprocessing import Queue

def start_worker_processes():
    
    task_queue = Queue()
    task_result_queue = Queue()
    
    master_address = "127.0.0.1:7999"
    
    jobrunner = Process(target=job_runner_main, args=(task_queue, task_result_queue), name="JobRunner")
    pinger = Process(target=pinger_main, args=(master_address, task_result_queue), name="Pinger")
    
    jobrunner.start()
    pinger.start()
    
    server_main(task_queue)

if __name__ == "__main__":
    start_worker_processes()