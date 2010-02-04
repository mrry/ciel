'''
Created on 29 Jan 2010

@author: dgm36
'''
from Queue import Empty

def pinger_main(master_address, task_result_queue):
    while True:
        try:
            result = task_result_queue.get(True, 30)
            print "Pinger got a result:", result
            # Send result message to master.
        except Empty:
            # Send heartbeat ping to master.
            pass
        
def dummy_pinger_main(master_address, pinger_queue):
    while True:
        try:
            result = pinger_queue.get(True, 30)
            print "PINGER: %s" % (str(result), )
        except Empty:
            print "PINGER: Periodic heartbeat..."