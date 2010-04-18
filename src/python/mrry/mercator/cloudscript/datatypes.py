'''
Created on Apr 18, 2010

@author: derek
'''

def all_leaf_values(value):
    """
    Recurses over a Skywriting data structure (containing lists, dicts and 
    primitive leaves), and yields all of the leaf objects.
    """
    if isinstance(value, list):
        for list_elem in value:
            for leaf in all_leaf_values(list_elem):
                yield leaf
    elif isinstance(value, dict):
        for (dict_key, dict_value) in value.items():
            for leaf in all_leaf_values(dict_key):
                yield leaf
            for leaf in all_leaf_values(dict_value):
                yield leaf
    else:
        # TODO: should we consider objects with fields?
        yield value
        
def map_leaf_values(f, value):
    """
    Recurses over a Skywriting data structure (containing lists, dicts and 
    primitive leaves), and returns a new structure with the leaves mapped as specified.
    """
    if isinstance(value, list):
        return map(lambda x: map_leaf_values(f, x), value)
    elif isinstance(value, dict):
        ret = {}
        for (dict_key, dict_value) in value.items():
            key = map_leaf_values(f, dict_key)
            value = map_leaf_values(f, dict_value)
            ret[key] = value
        return ret
    else:
        return f(value)