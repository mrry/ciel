
from skywriting.runtime.pycurl_rpc import post_string_noreturn
from skywriting.runtime.block_store import get_own_netloc

import simplejson
import threading

import ciel
import logging

module_lock = threading.RLock()

# Maps reference ID -> entity interested in advertisments
remote_stat_subscriptions = dict()

def subscribe_remote_output_nopost(refid, subscriber):
    with module_lock:
        try:
            if remote_stat_subscriptions[refid] != subscriber:
                raise Exception("Subscribing %s: Remote-stat currently only supports one subscriber per remote output!" % refid)
        except KeyError:
            # Nobody is currently subscribed
            pass
        remote_stat_subscriptions[refid] = subscriber

def subscribe_remote_output(refid, remote_netloc, chunk_size, subscriber):
    subscribe_remote_output_nopost(refid, subscriber)
    post_data = simplejson.dumps({"netloc": get_own_netloc(), "chunk_size": chunk_size})
    post_string_noreturn("http://%s/control/streamstat/%s/subscribe" % (remote_netloc, refid), post_data, result_callback=(lambda success, url: subscribe_result(refid, success, url)))

def unsubscribe_remote_output_nopost(refid):
    with module_lock:
        del remote_stat_subscriptions[refid]

def unsubscribe_remote_output(refid):
    unsubscribe_remote_output_nopost(refid)
    netloc = get_own_netloc()
    post_data = simplejson.dumps({"netloc": netloc})
    post_string_noreturn("http://%s/control/streamstat/%s/unsubscribe" 
                          % (netloc, refid), post_data)

def subscribe_result(refid, success, url):
    try:
        with module_lock:
            remote_stat_subscriptions[refid].subscribe_result(success, url)
    except KeyError:
        ciel.log("Subscribe-result for %s ignored as no longer subscribed" % url, "REMOTE_STAT", logging.WARNING)

def receive_stream_advertisment(id, **args):
    try:
        with module_lock:
            remote_stat_subscriptions[id].advertisment(**args)
    except KeyError:
        ciel.log("Got advertisment for %s which is not an ongoing stream" % id, "REMOTE_STAT", logging.WARNING)

