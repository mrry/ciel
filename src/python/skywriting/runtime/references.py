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
import uuid

'''
Created on 20 Apr 2010

@author: dgm36
'''

class SWRealReference:
    
    def as_tuple(self):
        pass

class SWErrorReference(SWRealReference):
    
    def __init__(self, reason, details):
        self.reason = reason
        self.details = details

    def as_tuple(self):
        return ('err', self.reason, self.details)

class SWNullReference(SWRealReference):
    
    def __init__(self):
        pass
    
    def as_tuple(self):
        return ('null',)
    
class SWFutureReference(SWRealReference):
    pass

class SWProvenance:
    
    def as_tuple(self):
        pass
    
class SWNoProvenance(SWProvenance):
    
    def as_tuple(self):
        return ('na', )
    
class SWTaskOutputProvenance(SWProvenance):
    
    def __init__(self, task_id, index):
        self.task_id = task_id
        self.index = index
    
    def as_tuple(self):
        return ('out', str(self.task_id), self.index)

class SWSpawnedTaskProvenance(SWProvenance):
    
    def __init__(self, task_id, spawn_list_index):
        self.task_id = task_id
        self.index = spawn_list_index

    def as_tuple(self):
        return ('spawn', str(self.task_id), self.index)

class SWTaskContinuationProvenance(SWProvenance):
    
    def __init__(self, task_id):
        self.task_id = task_id
        
    def as_tuple(self):
        return ('cont', str(self.task_id))

class SW2_FutureReference(SWFutureReference):
    
    def __init__(self, id, provenance=SWNoProvenance()):
        self.id = id
        self.provenance = provenance
    
    def as_tuple(self):
        return ('f2', self.id, self.provenance.as_tuple())
        
class SW2_ConcreteReference(SWRealReference):
        
    def __init__(self, id, provenance, location_hints):
        self.id = id
        self.provenance = provenance
        self.location_hints = location_hints
        
    def as_tuple(self):
        return('c2', self.id, self.provenance.as_tuple(), self.location_hints)
        
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

def build_provenance_from_tuple(provenance_tuple):
    p_type = provenance_tuple[0]
    if p_type == 'na':
        return SWNoProvenance()
    elif p_type == 'out':
        return SWTaskOutputProvenance(uuid.UUID(hex=provenance_tuple[1]), provenance_tuple[2])
    elif p_type == 'spawn':
        return SWSpawnedTaskProvenance(uuid.UUID(hex=provenance_tuple[1]), provenance_tuple[2])
    elif p_type == 'cont':
        return SWTaskContinuationProvenance(uuid.UUID(hex=provenance_tuple[1]))
    else:
        raise KeyError(p_type)

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
    elif ref_type == 'err':
        return SWErrorReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'null':
        return SWNullReference()
    elif ref_type == 'f2':
        return SW2_FutureReference(uuid.UUID(hex=reference_tuple[1]), build_provenance_from_tuple(reference_tuple[2]))
    elif ref_type == 'c2':
        return SW2_ConcreteReference(uuid.UUID(hex=reference_tuple[1]), build_provenance_from_tuple(reference_tuple[2]), reference_tuple[3])
    else:
        raise KeyError(ref_type)