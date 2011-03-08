
import sys
import skypy

def skypy_callback(str):
    print >>sys.stderr, "Skypy got callback: ", str
    return (str + " and Skypy too")

def skypy_main():
    sw_ret = skypy.sync_exec("swi", sw_file_ref=skypy.package_lookup("sw_main"))
    sw_str = skypy.deref_json(sw_ret)
    return "SW returned: %s" % str(sw_str)
