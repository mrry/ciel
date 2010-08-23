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

class SWExecResultProvenance(SWProvenance):
    
    def __init__(self, task_id, exec_result_index):
        self.task_id = task_id
        self.exec_result_index = exec_result_index
        
    def as_tuple(self):
        return ('exec', str(self.task_id), self.exec_result_index)

class SWSpawnExecArgsProvenance(SWProvenance):
    
    def __init__(self, task_id, spawn_exec_index):
        self.task_id = task_id
        self.spawn_exec_index = spawn_exec_index
        
    def as_tuple(self):
        return ('se_args', str(self.task_id), self.spawn_exec_index)

class SW2_FutureReference(SWFutureReference):
    """
    Used as a reference to a task that hasn't completed yet. The identifier is in a
    system-global namespace, and may be passed to other tasks or returned from
    tasks.
    """
        
    def __init__(self, id, provenance=SWNoProvenance()):
        self.id = id
        self.provenance = provenance
    
    def as_tuple(self):
        return ('f2', str(self.id), self.provenance.as_tuple())

    def __repr__(self):
        return 'SW2_FutureReference(%s, %s)' % (repr(self.id), repr(self.provenance))
        
ACCESS_SWBS = 'swbs'

class SW2_ConcreteReference(SWRealReference):
        
    def __init__(self, id, provenance, size_hint=None, location_hints=None):
        self.id = id
        self.provenance = provenance
        self.size_hint = size_hint
        if location_hints is not None:
            self.location_hints = location_hints
        else:
            self.location_hints = {}
        
    def add_location_hint(self, netloc, access_method):
        try:
            hints_for_netloc = self.location_hints[netloc]
        except KeyError:
            hints_for_netloc = []
            self.location_hints[netloc] = hints_for_netloc
        hints_for_netloc.append(access_method)
        
    def combine_with(self, ref):
        """Add the location hints from ref to this object."""
        if isinstance(ref, SW2_ConcreteReference):
            assert ref.id == self.id

            # We attempt to upgrade the provenance if more information is 
            # available from the merging reference. 
            if isinstance(self.provenance, SWNoProvenance):
                self.provenance = ref.provenance
            
            # We attempt to upgrade the size hint if more information is
            # available from the merging reference.
            if self.size_hint is None:
                self.size_hint = ref.size_hint
            
            # We calculate the union of the two sets of location hints.
            for (netloc, access_methods) in ref.location_hints.items():
                try:
                    existing_access_methods = set(self.location_hints[netloc])
                except KeyError:
                    existing_access_methods = set()
                self.location_hints[netloc] = list(set(access_methods) | existing_access_methods)
        
    def as_future(self):
        return SW2_FutureReference(self.id, self.provenance)
        
    def as_tuple(self):
        return('c2', str(self.id), self.provenance.as_tuple(), self.size_hint, self.location_hints)
        
    def __repr__(self):
        return 'SW2_ConcreteReference(%s, %s, %s, %s)' % (repr(self.id), repr(self.provenance), repr(self.size_hint), repr(self.location_hints))
        
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
        return SWTaskOutputProvenance(provenance_tuple[1], provenance_tuple[2])
    elif p_type == 'spawn':
        return SWSpawnedTaskProvenance(provenance_tuple[1], provenance_tuple[2])
    elif p_type == 'cont':
        return SWTaskContinuationProvenance(provenance_tuple[1])
    elif p_type == 'se_args':
        return SWSpawnExecArgsProvenance(provenance_tuple[1], provenance_tuple[2])
    elif p_type == 'exec':
        return SWExecResultProvenance(provenance_tuple[1], provenance_tuple[2])
    else:
        raise KeyError(p_type)

def build_reference_from_tuple(reference_tuple):
    ref_type = reference_tuple[0]
    if ref_type == 'urls':
        return SWURLReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'val':
        return SWDataValue(reference_tuple[1])
    elif ref_type == 'err':
        return SWErrorReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'null':
        return SWNullReference()
    elif ref_type == 'f2':
        return SW2_FutureReference(reference_tuple[1], build_provenance_from_tuple(reference_tuple[2]))
    elif ref_type == 'c2':
        return SW2_ConcreteReference(reference_tuple[1], build_provenance_from_tuple(reference_tuple[2]), reference_tuple[3], reference_tuple[4])
    else:
        raise KeyError(ref_type)