# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

'''
Created on 22 Apr 2010

@author: dgm36
'''

class WorkerFailedException(Exception):
    def __init__(self, worker):
        self.worker = worker

class AbortedException(Exception):
    def __init__(self):
        pass

class RuntimeSkywritingError(Exception):
    def __init__(self):
        pass

class AbortedExecutionException(RuntimeSkywritingError):
    def __init__(self):
        pass
    
class BlameUserException(RuntimeSkywritingError):
    def __init__(self, description):
        self.description = description
        
    def __repr__(self):
        return self.description
    
    def __str__(self):
        return self.description

class MasterNotRespondingException(RuntimeSkywritingError):
    def __init__(self):
        pass

class WorkerShutdownException(RuntimeSkywritingError):
    def __init__(self):
        pass

class ExecutionInterruption(Exception):
    def __init__(self):
        pass
    
class FeatureUnavailableException(ExecutionInterruption):
    def __init__(self, feature_name):
        self.feature_name = feature_name

    def __repr__(self):
        return 'FeatureUnavailableException(feature_name="%s")' % (self.feature_name, )
        
class DataTooBigException(ExecutionInterruption):
    def __init__(self, size):
        self.size = size
        
class ReferenceUnavailableException(ExecutionInterruption):
    def __init__(self, ref):
        self.ref = ref
        
    def __repr__(self):
        return 'ReferenceUnavailableException(ref=%s)' % (repr(self.ref), )
    
class SelectException(ExecutionInterruption):
    def __init__(self, select_group, timeout):
        self.select_group = select_group
        self.timeout = timeout
        
class MissingInputException(RuntimeSkywritingError):
    def __init__(self, bindings):
        self.bindings = bindings
        
    def __repr__(self):
        return 'MissingInputException(refs=%s)' % (repr(self.bindings.values()), )
