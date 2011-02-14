#!/usr/bin/python

import sys

def try_package(name):
    try:
        x = __import__(name)
    except Exception as e:
        print e
        return False
    else:
        print "Package", name, "found"
        return True

success = True

for package in ["ply", "httplib2", "simplejson", "cherrypy", "pycurl"]:
    success = success and try_package(package)

if success:
    sys.exit(0)
else:
    sys.exit(1)
