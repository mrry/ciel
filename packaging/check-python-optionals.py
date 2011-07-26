#!/usr/bin/python

try:
    import flup
    print "Package flup found"
except:
    print "Can't find Python package 'flup'. Won't be able to run with lighttpd"
