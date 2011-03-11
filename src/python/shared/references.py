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

class SWRealReference:
    
    def as_tuple(self):
        pass

    def as_protobuf(self):
        pass
    
    def is_consumable(self):
        return True

    def as_future(self):
        # XXX: Should really make id a field of RealReference.
        return SW2_FutureReference(self.id)
    
def netloc_to_protobuf(netloc):
    hostname, port = netloc.split(':')
    loc = NetworkLocation()
    loc.hostname = hostname
    loc.port = int(port)
    return loc

def protobuf_to_netloc(netloc):
    return '%s:%d' % (netloc.hostname, netloc.port)

class SWErrorReference(SWRealReference):
    
    def __init__(self, id, reason, details):
        self.id = id
        self.reason = reason
        self.details = details

    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.ERROR
        ref.id = self.id
        ref.reason = self.reason
        ref.details = self.details
        return ref

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
    
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.FUTURE
        ref.id = self.id
        return ref
    
    def as_tuple(self):
        return ('f2', str(self.id))

    def __str__(self):
        return "<FutureRef: %s...>" % self.id[:10]

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
            
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.CONCRETE
        ref.size_hint = self.size_hint
        for netloc in self.location_hints:
            ref.location_hints.add(netloc_to_protobuf(netloc))
        return ref
        
    def as_tuple(self):
        return('c2', str(self.id), self.size_hint, list(self.location_hints))

    def __str__(self):
        return "<ConcreteRef: %s..., length %d, held in %d locations>" % (self.id[:10], self.size_hint, len(self.location_hints))
        
    def __repr__(self):
        return 'SW2_ConcreteReference(%s, %s, %s)' % (repr(self.id), repr(self.size_hint), repr(self.location_hints))

class SW2_SweetheartReference(SW2_ConcreteReference):

    def __init__(self, id, sweetheart_netloc, size_hint=None, location_hints=None):
        SW2_ConcreteReference.__init__(self, id, size_hint, location_hints)
        self.sweetheart_netloc = sweetheart_netloc
        
    def combine_with(self, ref):
        """Add the location hints from ref to this object."""
        SW2_ConcreteReference.combine_with(self, ref)
        if isinstance(ref, SW2_SweetheartReference):
            self.sweetheart_netloc = ref.sweetheart_netloc
            
    def as_protobuf(self):
        ref = SW2_ConcreteReference.as_protobuf(self)
        ref.type = Reference.SWEETHEART
        ref.sweetheart = netloc_to_protobuf(self.sweetheart_netloc)
        return ref
        
    def as_tuple(self):
        return('<3', str(self.id), self.sweetheart_netloc, self.size_hint, list(self.location_hints))
        
    def __repr__(self):
        return 'SW2_SweetheartReference(%s, %s, %s, %s)' % (repr(self.id), repr(self.sweetheart_netloc), repr(self.size_hint), repr(self.location_hints))
        
class SW2_FixedReference(SWRealReference):
    
    def __init__(self, id, fixed_netloc):
        self.id = id
        self.fixed_netloc = fixed_netloc
    
    def combine_with(self, ref):
        pass
    
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.FIXED
        ref.id = self.id
        ref.location_hints.add(netloc_to_protobuf(self.fixed_netloc))
        return ref
    
    def as_tuple(self):
        return ('fx', str(self.id), self.fixed_netloc)
        
    def __str__(self):
        return "<FixedRef: %s, stored at %s>" % (self.id[:10], self.fixed_netloc)
        
    def __repr__(self):
        return 'SW2_FixedReference(%s, %s)' % (repr(self.id), repr(self.fixed_netloc))
        
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
        
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.STREAM
        ref.id = self.id
        for netloc in self.location_hints:
            ref.location_hints.add(netloc_to_protobuf(netloc))
        return ref
        
    def as_tuple(self):
        return('s2', str(self.id), list(self.location_hints))

    def __str__(self):
        return "<StreamRef: %s..., held in %d locations>" % (self.id[:10], len(self.location_hints))
        
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
        
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.TOMBSTONE
        ref.id = self.id
        for netloc in self.location_hints:
            ref.location_hints.add(netloc_to_protobuf(netloc))
        return ref
        
    def as_tuple(self):
        return ('t2', str(self.id), list(self.netlocs))

    def __str__(self):
        return "<Tombstone: %s...>" % self.id[:10]

    def __repr__(self):
        return 'SW2_TombstoneReference(%s, %s)' % (repr(self.id), repr(self.netlocs))

class SW2_FetchReference(SWRealReference):
    
    def __init__(self, id, url, index=None):
        self.id = id
        self.url = url
        self.index = index

    def is_consumable(self):
        return False
    
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.FETCH
        ref.id = self.id
        ref.url = self.url
        return ref
    
    def as_tuple(self):
        return ('fetch2', str(self.id), str(self.url))

    def __str__(self):
        return "<FetchRef: %s..., for %s...>" % (self.id[:10], self.url[:20])

    def __repr__(self):
        return 'SW2_FetchReference(%s, %s)' % (repr(self.id), repr(self.url))

class SWDataValue(SWRealReference):
    """
    This is akin to a SW2_ConcreteReference which encapsulates its own data.
    The data is always a string, and must be decoded using block_store functions much like Concrete refs.
    """
    
    def __init__(self, id, value):
        self.id = id
        self.value = value
        
    def as_tuple(self):
        return ('val', self.id, self.value)
    
    def as_protobuf(self):
        ref = Reference()
        ref.type = Reference.VALUE
        ref.id = self.id
        ref.value = self.value
        return ref
    
    def __str__(self):
        string_repr = ""
        if len(self.value) < 20:
            string_repr = '"' + self.value + '"'
        else:
            string_repr = "%d bytes inline, starting with '%s'" % (len(self.value), self.value[:20])
        return "<DataValue: %s...: %s>" % (self.id[:10], string_repr)

    def __repr__(self):
        return 'SWDataValue(%s, %s)' % (repr(self.id), repr(self.value))

def build_reference_from_tuple(reference_tuple):
    ref_type = reference_tuple[0]
    if ref_type == 'val':
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
    elif ref_type == 'fx':
        return SW2_FixedReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 't2':
        return SW2_TombstoneReference(reference_tuple[1], reference_tuple[2])
    elif ref_type == 'fetch2':
        return SW2_FetchReference(reference_tuple[1], reference_tuple[2])
    else:
        raise KeyError(ref_type)

try:
    from shared.generated.ciel.protoc_pb2 import Reference, NetworkLocation

    def build_reference_from_protobuf(ref):
        if ref.type == Reference.VALUE:
            return SWDataValue(ref.id, ref.value)
        elif ref.type == Reference.ERROR:
            return SWErrorReference(ref.id, ref.reason, ref.details)
        elif ref.type == Reference.FUTURE:
            return SW2_FutureReference(ref.id)
        elif ref.type == Reference.CONCRETE:
            return SW2_ConcreteReference(ref.id, ref.size_hint, map(protobuf_to_netloc, ref.location_hints))
        elif ref.type == Reference.SWEETHEART:
            return SW2_SweetheartReference(ref.id, ref.sweetheart, ref.size_hint, map(protobuf_to_netloc, ref.location_hints))
        elif ref.type == Reference.STREAM:
            return SW2_StreamReference(ref.id, map(protobuf_to_netloc, ref.location_hints))
        elif ref.type == Reference.FIXED:
            return SW2_FixedReference(ref.id, protobuf_to_netloc(ref.location_hints[0]))
        elif ref.type == Reference.TOMBSTONE:
            return SW2_TombstoneReference(ref.id, map(protobuf_to_netloc, ref.location_hints))
        elif ref.type == Reference.FETCH:
            return SW2_FetchReference(ref.id, ref.url)
        else:
            raise KeyError(ref.type)
except ImportError:
    import sys
    print >>sys.stderr, 'Could not import protobufs.'
    
def combine_references(original, update):

    # DataValues are better than all others: they *are* the data
    if isinstance(original, SWDataValue):
        return original
    if isinstance(update, SWDataValue):
        return update

    # Sweetheart reference over other non-vals; combine location hints if any available.
    if (isinstance(update, SW2_SweetheartReference)):
        if (isinstance(original, SW2_ConcreteReference)):
            update.location_hints.update(original.location_hints)
        return update

    # Concrete reference > streaming reference > future reference.
    if (isinstance(original, SW2_FutureReference) or isinstance(original, SW2_StreamReference)) and isinstance(update, SW2_ConcreteReference):
        return update
    if isinstance(original, SW2_FutureReference) and isinstance(update, SW2_StreamReference):
        return update
    
    # Error reference > future reference.
    if isinstance(original, SW2_FutureReference) and isinstance(update, SWErrorReference):
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
    
    # Propagate failure if a fixed reference goes away.
    if (isinstance(original, SW2_FixedReference) and isinstance(update, SW2_TombstoneReference)):
        return SWErrorReference(original.id, 'LOST_FIXED_OBJECT', original.fixed_netloc)
    
    # If we reach this point, we should ignore the update.
    return original

