
from skywriting.runtime.pycurl_rpc import post_string_noreturn
from skywriting.runtime.block_store import get_own_netloc

import simplejson
import threading

module_lock = threading.RLock()

# Maps reference ID -> entity interested in advertisments
remote_stat_subscriptions = dict()

def subscribe_remote_output_nopost(refid, subscriber):
    with module_lock:
        remote_stat_subscriptions[refid] = subscriber

def subscribe_remote_output(refid, remote_netloc, chunk_size, subscriber):
    subscribe_remote_output_nopost(refid, subscriber)
    post_data = simplejson.dumps({"netloc": get_own_netloc(), "chunk_size": chunk_size})
    _post_string_noreturn("http://%s/control/streamstat/%s/subscribe" % (remote_netloc, refid), post_data, result_callback=subscribe_result)

def unsubscribe_remote_output(refid):
    with module_lock:
        del remote_stat_subscriptions[refid]
    post_data = simplejson.dumps({"netloc": get_own_netloc()})
    _post_string_noreturn("http://%s/control/streamstat/%s/unsubscribe" 
                          % (self.worker_netloc, self.ref.id), post_data)

def subscribe_result(success, url):
    try:
        with module_lock:
            remote_stat_subscription[id].subscribe_result(success, url)
    except KeyError:
        ciel.log("Subscribe-result for %s ignored as no longer subscribed" % url, "REMOTE_STAT", logging.WARNING)

def receive_stream_advertisment(self, id, **args):
    try:
        with module_lock:
            remote_stat_subscriptions[id].advertisment(**args)
    except KeyError:
        ciel.log("Got advertisment for %s which is not an ongoing stream" % id, "REMOTE_STAT", logging.WARNING)

