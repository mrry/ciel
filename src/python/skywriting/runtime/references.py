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
import struct
import simplejson

class SWReferenceJSONEncoder(simplejson.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, SWRealReference):
            return {'__ref__': obj.as_tuple()}
        else:
            return simplejson.JSONEncoder.default(self, obj)

# Binary representation helper functions.
REF_TYPE_STRUCT = struct.Struct("2s")
INT_STRUCT = struct.Struct("!I")
PORT_STRUCT = struct.Struct("!H")
SIZE_STRUCT = struct.Struct("!Q")
def reftype_to_packed(reftype):
    return REF_TYPE_STRUCT.pack(reftype)
def string_to_packed(_str):
    return INT_STRUCT.pack(len(_str)) + _str
def id_to_packed(id):
    return string_to_packed(id)
def netloc_to_packed(netloc):
    colon_index = netloc.index(':')
    hostname = netloc[:colon_index]
    port = int(netloc[colon_index + 1:])
    return INT_STRUCT.pack(len(hostname)) + PORT_STRUCT.pack(port)
def netloc_set_to_packed(netlocs):
    return INT_STRUCT.pack(len(netlocs)) + "".join([netloc_to_packed(x) for x in netlocs])
def size_to_packed(size):
    return SIZE_STRUCT.pack(size)

class SWRealReference:
    
    def as_tuple(self):
        pass
    
    def as_binrepr(self):
        pass

    def is_consumable(self):
        return True

class SWErrorReference(SWRealReference):
    
    def __init__(self, id, reason, details):
        self.id = id
        self.reason = reason
        self.details = details

    def as_binrepr(self):
        return reftype_to_packed('xx') + id_to_packed(self.id) + string_to_packed(self.reason) + string_to_packed(self.details)

    def as_tuple(self):
        return ('err', self.id, self.reason, self.details)

class SW2_FutureReference(SWRealReference):
    """
    Used as a reference to a task that hasn't completed yet. The identifier is in a
    system-global namespace, and may be passed to other tasks or returned from
    tasks.
    """
        
    def __init__(self, id):
        self.id = id
    
    def is_consumable(self):
        return False
    
    def as_future(self):
        return self
    
    def as_binrepr(self):
        return reftype_to_packed('f2') + id_to_packed(self.id)
    
    def as_tuple(self):
        return ('f2', str(self.id))

    def __repr__(self):
        return 'SW2_FutureReference(%s)' % (repr(self.id), )
        
class SW2_ConcreteReference(SWRealReference):
        
    def __init__(self, id, size_hint=None, location_hints=None):
        self.id = id
        self.size_hint = size_hint
        if location_hints is not None:
            self.location_hints = set(location_hints)
        else:
            self.location_hints = set()
        
    def add_location_hint(self, netloc):
        self.location_hints.add(netloc)
        
    def combine_with(self, ref):
        """Add the location hints from ref to this object."""
        if isinstance(ref, SW2_ConcreteReference):
            assert ref.id == self.id
            
            # We attempt to upgrade the size hint if more information is
            # available from the merging reference.
            if self.size_hint is None:
                self.size_hint = ref.size_hint
            
            # We calculate the union of the two sets of location hints.
            self.location_hints.update(ref.location_hints)
            
    def as_future(self):
        return SW2_FutureReference(self.id)
        
    def as_binrepr(self):
        return reftype_to_packed('c2') + id_to_packed(self.id) + size_to_packed(self.size_hint) + netloc_set_to_packed(self.location_hints)
        
    def as_tuple(self):
        return('c2', str(self.id), self.size_hint, list(self.location_hints))
        
    def __repr__(self):
        return 'SW2_ConcreteReference(%s, %s, %s)' % (repr(self.id), repr(self.size_hint), repr(self.location_hints))
        
class SW2_StreamReference(SWRealReference):
    
    def __init__(self, id, location_hints=None):
        self.id = id
        if location_hints is not None:
            self.location_hints = set(location_hints)
        else:
            self.location_hints = set()
        
    def add_location_hint(self, netloc):
        self.location_hints.add(netloc)

    def combine_with(self, ref):
        """Add the location hints from ref to this object."""
        if isinstance(ref, SW2_StreamReference):
            assert ref.id == self.id
            
            # We attempt to upgrade the size hint if more information is
            # available from the merging reference.
            
            # We calculate the union of the two sets of location hints.
            self.location_hints.update(ref.location_hints)
            
    def as_future(self):
        return SW2_FutureReference(self.id)
        
    def as_binrepr(self):
        return reftype_to_packed('s2') + id_to_packed(self.id) + netloc_set_to_packed(self.location_hints)
        
    def as_tuple(self):
        return('s2', str(self.id), list(self.location_hints))
        
    def __repr__(self):
        return 'SW2_StreamReference(%s, %s)' % (repr(self.id), repr(self.location_hints))
                
class SW2_TombstoneReference(SWRealReference):
    
    def __init__(self, id, netlocs=None):
        self.id = id
        if netlocs is not None:
            self.netlocs = set(netlocs)
        else:
            self.netlocs = set()
            
    def is_consumable(self):
        return False        
    
    def add_netloc(self, netloc):
        self.netlocs.add(netloc)
        
    def as_binrepr(self):
        return reftype_to_packed('t2') + id_to_packed(self.id) + netloc_set_to_packed(self.netlocs)
        
    def as_tuple(self):
        return ('t2', str(self.id), list(self.netlocs))
    
    def __repr__(self):
        return 'SW2_TombstoneReference(%s, %s)' % (repr(self.id), repr(self.netlocs))

class SW2_FetchReference(SWRealReference):
    
    def __init__(self, id, url, index=None):
        self.id = id
        self.url = url
        self.index = index

    def is_consumable(self):
        return False
    
    def as_binrepr(self):
        return reftype_to_packed('fx') + id_to_packed(self.id) + string_to_packed(self.url)
    
    def as_tuple(self):
        return ('fetch2', str(self.id), str(self.url))
    
    def __repr__(self):
        return 'SW2_FetchReference(%s, %s)' % (repr(self.id), repr(self.url))

class SW2_SweetheartReference(SW2_ConcreteReference):

    def __init__(self, id, sweetheart_netloc, size_hint=None, location_hints=None):
        SW2_ConcreteReference.__init__(self, id, size_hint, location_hints)
        self.sweetheart_netloc = sweetheart_netloc
        
    def combine_with(self, ref):
        """Add the location hints from ref to this object."""
        SW2_ConcreteReference.combine_with(self, ref)
        if isinstance(ref, SW2_SweetheartReference):
            self.sweetheart_netloc = ref.sweetheart_netloc
            
    #def as_binrepr(self):
    #    return reftype_to_packed('<3') + id_to_packed(self.id) + size_to_packed(self.size_hint) + netloc_set_to_packed(self.location_hints)
        
    def as_tuple(self):
        return('<3', str(self.id), self.sweetheart_netloc, self.size_hint, list(self.location_hints))
        
    def __repr__(self):
        return 'SW2_SweetheartReference(%s, %s, %s, %s)' % (repr(self.id), repr(self.sweetheart_netloc), repr(self.size_hint), repr(self.location_hints))

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
    
    def __init__(self, id, value):
        self.id = id
        self.value = value
        
    def as_tuple(self):
        return ('val', self.id, self.value)
    
    def as_binrepr(self):
        return reftype_to_packed('dv') + id_to_packed(self.id) + string_to_packed(simplejson.dumps(self.value, cls=SWReferenceJSONEncoder))
    
    def __repr__(self):
        return 'SWDataValue(%s, %s)' % (repr(self.id), repr(self.value))

def build_reference_from_tuple(reference_tuple):
    ref_type = reference_tuple[0]
    if ref_type == 'urls':
        return SWURLReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'val':
        return SWDataValue(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'err':
        return SWErrorReference(reference_tuple[1], reference_tuple[2], reference_tuple[3])
    elif ref_type == 'f2':
        return SW2_FutureReference(reference_tuple[1])
    elif ref_type == 'c2':
        return SW2_ConcreteReference(reference_tuple[1], reference_tuple[2], reference_tuple[3])
    elif ref_type == '<3':
        return SW2_SweetheartReference(reference_tuple[1], reference_tuple[2], reference_tuple[3], reference_tuple[4])
    elif ref_type == 's2':
        return SW2_StreamReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 't2':
        return SW2_TombstoneReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'fetch2':
        return SW2_FetchReference(reference_tuple[1], reference_tuple[2])
    else:
        raise KeyError(ref_type)
    
def combine_references(original, update):

    # Sweetheart reference over all other types; combine location hints if any available.
    if (isinstance(update, SW2_SweetheartReference)):
        if (isinstance(original, SW2_ConcreteReference)):
            update.location_hints.update(original.location_hints)
        return update
        
    # Concrete reference > streaming reference > future reference.
    if (isinstance(original, SW2_FutureReference) or isinstance(original, SW2_StreamReference)) and isinstance(update, SW2_ConcreteReference):
        return update
    if isinstance(original, SW2_FutureReference) and isinstance(update, SW2_StreamReference):
        return update
    
    # For references of the same type, merge the location hints for the two references.
    if isinstance(original, SW2_StreamReference) and isinstance(update, SW2_StreamReference):
        original.combine_with(update)
        return original
    if isinstance(original, SW2_ConcreteReference) and isinstance(update, SW2_ConcreteReference):
        original.combine_with(update)
        return original
    
    if (isinstance(original, SW2_ConcreteReference) or isinstance(original, SW2_StreamReference)) and isinstance(update, SW2_TombstoneReference):
        original.location_hints.difference_update(update.netlocs)
        if len(original.location_hints) == 0:
            return original.as_future()
        else:
            return original
    
    # If we return false, this means we should ignore the update.
    return False
    
