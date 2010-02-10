'''
Created on 10 Feb 2010

@author: dgm36
'''
from mrry.mercator.tasks.util import MercatorIPC
import time



if __name__ == '__main__':
    ipc = MercatorIPC(lambda x: ipc.send_message(x))
    
    time.sleep(100)
    
    ipc.finalize()