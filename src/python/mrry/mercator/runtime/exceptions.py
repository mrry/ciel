'''
Created on 22 Apr 2010

@author: dgm36
'''

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
    def __init__(self, ref, continuation):
        self.ref = ref
        self.continuation = continuation
        
    def __repr__(self):
        return 'ReferenceUnavailableException(ref=%s)' % (repr(self.ref), )
    
class SelectException(ExecutionInterruption):
    def __init__(self, select_group, timeout):
        self.select_group = select_group
        self.timeout = timeout
        
        