'''
Created on 10 Feb 2010

@author: dgm36
'''
from mrry.mercator.tasks.util import MercatorIPC
import sys
import time

def callback(ipc, message):
    print >> sys.stderr, "Received message: %s" % (message, )

if __name__ == '__main__':
    ipc = MercatorIPC(lambda x: callback(ipc, x))
    
    time.sleep(100)
    
    ipc.finalize()