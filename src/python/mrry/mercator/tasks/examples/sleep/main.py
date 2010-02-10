'''
Created on 10 Feb 2010

@author: dgm36
'''
from mrry.mercator.tasks.util import MercatorIPC
import time

if __name__ == '__main__':
    ipc = MercatorIPC()
  
    time.sleep(60)
    ipc.send_message("60")
    time.sleep(60)
    ipc.send_message("120")
    time.sleep(60)
    ipc.send_message("180")
    
    ipc.finalize()