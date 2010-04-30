'''
Created on 20 Apr 2010

@author: dgm36
'''

class SWRealReference:
    
    def as_tuple(self):
        pass

class SWFutureReference(SWRealReference):
    pass

class SWLocalFutureReference(SWFutureReference):
    """
    Used as a placeholder reference for the results of spawned tasks. Refers to the
    output of a particular task in the spawn list. If that task has multiple outputs,
    refers to a particular result.
    
    This must be rewritten before the continuation is spawned. However, it may be used
    as the argument to another spawned task.
    """
    
    def __init__(self, spawn_list_index, result_index=0):
        self.spawn_list_index = spawn_list_index
        self.result_index = result_index
        
    def as_tuple(self):
        if self.result_index is None:
            return ('lfut', self.spawn_list_index)
        else:
            return ('lfut', self.spawn_list_index, self.result_index)
        
    def __repr__(self):
        return 'SWLocalFutureReference(%d, %d)' % (self.spawn_list_index, self.result_index)

class SWURLReference(SWRealReference):
    """
    A reference to one or more URLs representing the same data.
    """
    
    def __init__(self, urls, size_hint=None):
        self.urls = urls
        self.size_hint = size_hint
        
    def as_tuple(self):
        return ('urls', self.urls, self.size_hint)
    
    def __repr__(self):
        return 'SWURLReference(%s, %s)' % (repr(self.urls), repr(self.size_hint))

class SWGlobalFutureReference(SWFutureReference):
    """
    Used as a reference to a task that hasn't completed yet. The identifier is in a
    system-global namespace, and may be passed to other tasks or returned from
    tasks.
    
    SWLocalFutureReferences must be rewritten to be SWGlobalFutureReference objects.
    """

    def __init__(self, id):
        self.id = id
        
    def as_tuple(self):
        return ('gfut', self.id)

    def __repr__(self):
        return 'SWGlobalFutureReference(%d)' % (self.id, )

class SWLocalDataFile(SWRealReference):
    """
    Used when a reference is used as a file input (and hence should
    not be brought into the environment.
    """
    
    def __init__(self, filename):
        self.filename = filename
        
    def as_tuple(self):
        return ('lfile', self.filename)

    def __repr__(self):
        return 'SWLocalDataFile(%s)' % (repr(self.filename), )

class SWDataValue(SWRealReference):
    """
    Used to store data that has been dereferenced and loaded into the environment.
    """
    
    def __init__(self, value):
        self.value = value
        
    def as_tuple(self):
        return ('val', self.value)
    
    def __repr__(self):
        return 'SWDataValue(%s)' % (repr(self.value), )

def build_reference_from_tuple(reference_tuple):
    ref_type = reference_tuple[0]
    if ref_type == 'urls':
        return SWURLReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'lfut':
        if len(reference_tuple) == 3:
            result_index = reference_tuple[2]
        else:
            result_index = None
        return SWLocalFutureReference(reference_tuple[1], result_index)
    elif ref_type == 'gfut':
        return SWGlobalFutureReference(reference_tuple[1])
    elif ref_type == 'lfile':
        return SWLocalDataFile(reference_tuple[1])
    elif ref_type == 'val':
        return SWDataValue(reference_tuple[1])
